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

package cluster

import "github.com/maxymania/curly-goggles/hashcrown"
import "github.com/hashicorp/memberlist"
import "github.com/valyala/gorpc"
import "github.com/vmihailenco/msgpack"
import "sync"
import "fmt"
import "bytes"
import "net"

type ClusterNode struct{
	_msgpack struct{} `msgpack:",asArray"`
	Marker  string
	RpcPort int
	NodeId  []byte
}

type remoteNode struct{
	*ClusterNode
	Name string
	Rpc  *gorpc.Client
}
type NodeMap map[string]*remoteNode

// Return a simple answer!
func emptyHandler(clientAddr string, request interface{}) (response interface{}) {
	return false
}

type ClusterElement struct{
	Marker  string
	Ring    *hashcrown.NodeRing
	Local   *ClusterNode
	
	Handler gorpc.HandlerFunc
	Server  *gorpc.Server
	
	mutex sync.Mutex
	NodeMap NodeMap
}
func (c *ClusterElement) Initialize(nodeID []byte, RpcPort int) {
	c.NodeMap = make(NodeMap)
	if c.Handler==nil { c.Handler = emptyHandler }
	c.Local = &ClusterNode{
		Marker  : c.Marker,
		RpcPort : RpcPort,
		NodeId  : nodeID,
	}
	c.Ring = hashcrown.NewNodeRing()
	
	c.Server = gorpc.NewTCPServer(fmt.Sprintf(":%d",c.Local.RpcPort), c.Handler)
	c.Server.Start()
}


func (c *ClusterElement) NodeMeta(limit int) []byte {
	data,_ := msgpack.Marshal(c.Local)
	if limit<len(data) { return nil }
	return data
}
func (c *ClusterElement) NotifyMsg([]byte) {}
func (c *ClusterElement) GetBroadcasts(overhead, limit int) [][]byte { return nil }
func (c *ClusterElement) LocalState(join bool) []byte { return nil }
func (c *ClusterElement) MergeRemoteState(buf []byte, join bool) { }
func (c *ClusterElement) decode(n *memberlist.Node) *remoteNode {
	cn := new(ClusterNode)
	if msgpack.Unmarshal(n.Meta,cn)!=nil { return nil }
	if cn.Marker!=c.Marker { return nil }
	rn := new(remoteNode)
	rn.ClusterNode = cn
	rn.Name = n.Name
	
	// Node is a remote node!!!
	if cn.RpcPort>0 && !bytes.Equal(rn.NodeId,c.Local.NodeId) {
		ta := &net.TCPAddr{IP:n.Addr,Port:cn.RpcPort}
		rn.Rpc = gorpc.NewTCPClient(ta.String())
	}
	
	return rn
}
func (c *ClusterElement) NotifyJoin(n *memberlist.Node) {
	rn := c.decode(n)
	if rn==nil { return }
	
	c.mutex.Lock()
	c.NodeMap[rn.Name] = rn
	c.mutex.Unlock()
	
	if bytes.Equal(rn.NodeId,c.Local.NodeId) {
		c.Ring.SetLocalNode(hashcrown.NewBinary(rn.NodeId),rn)
	} else {
		c.Ring.AddNode(hashcrown.NewBinary(rn.NodeId),rn)
	}
}
func (c *ClusterElement) NotifyLeave(n *memberlist.Node) {
	c.mutex.Lock()
	rn,ok := c.NodeMap[n.Name]
	if !ok {
		c.mutex.Unlock()
		return
	}
	delete(c.NodeMap,n.Name)
	c.mutex.Unlock()
	c.Ring.RemoveNode(hashcrown.NewBinary(rn.NodeId))
}
func (c *ClusterElement) NotifyUpdate(n *memberlist.Node) {}
func (c *ClusterElement) NotifyAlive(peer *memberlist.Node) error {
	if c.decode(peer)==nil {
		return fmt.Errorf("Invalid Nodes")
	}
	return nil
}
func (c *ClusterElement) NotifyMerge(peers []*memberlist.Node) error {
	for _,peer := range peers {
		if c.decode(peer)==nil {
			return fmt.Errorf("Invalid Nodes")
		}
	}
	return nil
}
