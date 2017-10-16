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

package hashcrown

import "github.com/tidwall/btree"
import "crypto/md5"
import "bytes"
import "sync"
import "fmt"

type Node struct{
	Key     [md5.Size]byte
	Remote  bool
	Further interface{} // Further informations about the node.
}
func (n* Node) Less(than btree.Item, ctx interface{}) bool {
	return bytes.Compare(n.Key[:],than.(*Node).Key[:])<0
}
func (n *Node) String() string {
	if n.Remote { return fmt.Sprintf("R:%v",n.Further) }
	return fmt.Sprintf("L:%v",n.Further)
}

type NodeRing struct{
	mutex sync.RWMutex
	tree  *btree.BTree
	local *Node
}
func NewNodeRing() *NodeRing {
	nr := new(NodeRing)
	nr.tree = btree.New(2, nr)
	return nr
}
func (nr *NodeRing) SetLocalNode(key Binary,value interface{}) {
	nr.mutex.Lock() ; defer nr.mutex.Unlock()
	if nr.local!=nil {
		nr.tree.Delete(nr.local)
		nr.local = nil
	}
	nr.local = &Node{key.Md5(),false,value}
	nr.tree.ReplaceOrInsert(nr.local)
}
func (nr *NodeRing) AddNode(key Binary,value interface{}) {
	nr.mutex.Lock() ; defer nr.mutex.Unlock()
	nr.tree.ReplaceOrInsert(&Node{key.Md5(),true,value})
}
func (nr *NodeRing) RemoveNode(key Binary) *Node{
	return nr.RemoveNodeObj(&Node{key.Md5(),true,nil})
}
func (nr *NodeRing) RemoveNodeObj(obj *Node) *Node{
	nr.mutex.Lock() ; defer nr.mutex.Unlock()
	item,_ := nr.tree.Delete(obj).(*Node)
	return item
}
func (nr *NodeRing) GetNode(key Binary) *Node{
	return nr.GetNodeObj(&Node{key.Md5(),true,nil})
}
func (nr *NodeRing) GetNodeObj(obj *Node) *Node{
	nr.mutex.RLock() ; defer nr.mutex.RUnlock()
	item,_ := nr.tree.Get(obj).(*Node)
	return item
}
func (nr *NodeRing) GetTargetObj(keyObj *Node, dst []*Node) []*Node {
	nr.mutex.RLock() ; defer nr.mutex.RUnlock()
	q := &Query{keyObj,make([]btree.Item,0,len(dst)),len(dst)}
	q.ApplyTo(nr.tree)
	dst = dst[:len(q.Values)]
	for i,v := range q.Values { dst[i] = v.(*Node) }
	return dst
}
func (nr *NodeRing) GetTarget(key Binary, dst []*Node) []*Node {
	return nr.GetTargetObj(&Node{key.Md5(),true,nil},dst)
}

