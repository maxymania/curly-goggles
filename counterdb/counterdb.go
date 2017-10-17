/*
Copyright (c) 2017 Simon Schmidt

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
*/

// A simplistic Counter Database.
// Caution! This system has no clue of Consistency.
package counterdb

import "github.com/boltdb/bolt"
import "github.com/maxymania/curly-goggles/cluster"
import "github.com/maxymania/curly-goggles/hashcrown"
import "github.com/vmihailenco/msgpack"
import "encoding/gob"

var bucket = []byte("counter")

func init() {
	gob.RegisterName("counterdb.NQ",&RequestNode{})
	gob.RegisterName("counterdb.MQ",&RequestMaster{})
	gob.RegisterName("counterdb.SQ",&RequestSlave{})
	gob.RegisterName("counterdb.MR",&ResponseMaster{})
	gob.RegisterName("counterdb.X",&Nothing{})
}

type Nothing struct{}

type RequestNode struct{
	ToMaster RequestMaster
}

type RequestMaster struct{
	Key     []byte
	Command string
	Args    []byte
}
type ResponseMaster struct{
	// 0: OK
	// 1: No Hashring.
	// 2: IO-Error
	// 3: Update failed
	// 4: Recipient of this Request is Not master
	// 5: Network Error
	Errno  uint8
	Result []byte
}
type RequestSlave struct{
	Key   []byte
	Value []byte
}
type ResponseSlave struct{
	// 0: OK
	// 1: IO-Error
	Errno uint8
}

type CounterEntry struct{
	_msgpack struct{} `msgpack:",asArray"`
	Low    int64
	High   int64
	Number int64
}
func (ce *CounterEntry) Increment() int64 {
	ce.High ++
	if ce.Low==0 { ce.Low++ }
	ce.Number++
	return ce.High
}
func (ce *CounterEntry) Rollback(i int64) {
	ce.Number--
	if ce.High==i {
		ce.High--
	} else if ce.Low==i {
		ce.Low++
	}
	if ce.High<=ce.Low { ce.High,ce.Low = 0,0 }
}
func (ce *CounterEntry) Remold(o *CounterEntry) {
	ce.Low = o.Low
	ce.Number -= o.Number
	if ce.High<=ce.Low { ce.High,ce.Low,ce.Number = 0,0,0 }
}
func (ce *CounterEntry) Replace(o *CounterEntry) { *ce = *o }

type KeyValueEngine struct{
	Cluster *cluster.ClusterElement
	Store   *bolt.DB
}

func SetEngine(ce *cluster.ClusterElement, db *bolt.DB) {
	kve := &KeyValueEngine{ce,db}
	ce.Handler = kve.Handle
}

func (kv *KeyValueEngine) node(req *RequestNode) interface{} {
	hc := kv.Cluster.Ring.GetTarget(hashcrown.NewBinary(req.ToMaster.Key),make([]*hashcrown.Node,1))
	if len(hc)==0 { return &ResponseMaster{Errno:1} }
	if hc[0].Remote {
		_,_,rpc := cluster.Extract(hc[0])
		resp,err := rpc.Call(&(req.ToMaster))
		if err!=nil { return &ResponseMaster{Errno:5} }
		return resp
	}
	return kv.master(&(req.ToMaster))
}

func (kv *KeyValueEngine) master(req *RequestMaster) interface{} {
	hc := kv.Cluster.Ring.GetTarget(hashcrown.NewBinary(req.Key),make([]*hashcrown.Node,1))
	if len(hc)==0 { return &ResponseMaster{Errno:1} }
	if hc[0].Remote { return &ResponseMaster{Errno:4} }
	rv := []byte(nil)
	nv := []byte(nil)
	ok := false
	err := kv.Store.Batch(func (tx *bolt.Tx) error{
		ok = false
		rv = nil
		nv = nil
		bkt,e := tx.CreateBucketIfNotExists(bucket)
		if e!=nil { return e }
		ce := new(CounterEntry)
		if msgpack.Unmarshal(bkt.Get(req.Key),ce)!=nil { *ce = CounterEntry{} }
		
		switch req.Command {
		case "Increment":
			rv,_ = msgpack.Marshal(ce.Increment())
		case "Rollback":
			{
				i := int64(0)
				msgpack.Unmarshal(req.Args,&i)
				ce.Rollback(i)
			}
		case "Remold","Replace":
			{
				o := new(CounterEntry)
				if msgpack.Unmarshal(bkt.Get(req.Key),o)!=nil { *o = CounterEntry{} }
				switch req.Command {
				case "Remold":  ce.Remold(o)
				case "Replace": ce.Replace(o)
				}
			}
		}
		nv,_ = msgpack.Marshal(ce)
		e = bkt.Put(req.Key,nv)
		if e!=nil { return e }
		ok = true
		return nil
	})
	if err!=nil { return &ResponseMaster{Errno:2} } // IO-Error
	if !ok { return &ResponseMaster{Errno:3} } // Update failed
	
	// Replace Pair to all Remote Nodes
	for _,node := range kv.Cluster.NodeList {
		_,_,rpc := cluster.ExtractRaw(node)
		rpc.Send(&RequestSlave{req.Key,nv})
	}
	
	return &ResponseMaster{Result:rv}
}
func (kv *KeyValueEngine) slave(req *RequestSlave) interface{} {
	err := kv.Store.Batch(func (tx *bolt.Tx) error{
		bkt,e := tx.CreateBucketIfNotExists(bucket)
		if e!=nil { return e }
		return bkt.Put(req.Key,req.Value)
	})
	
	if err!=nil { return &ResponseSlave{Errno:1} }
	return &ResponseSlave{Errno:0}
}

func (kv *KeyValueEngine) Handle(clientAddr string, request interface{}) (response interface{}) {
	switch v := request.(type) {
	case *RequestNode:   return kv.node(v)
	case *RequestMaster: return kv.master(v)
	case *RequestSlave:  return kv.slave(v)
	}
	return &Nothing{}
}

