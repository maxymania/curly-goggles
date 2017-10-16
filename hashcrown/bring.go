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

/*
Performs the following scan on a BTree (sorted Set/Map):

[key...last] || [first...key-1]

This scan is limited by a maximum number of entries.
*/
type Query struct{
	Key btree.Item
	Values []btree.Item
	Number int // Number of items
}
func (q *Query) want() bool {
	return q.Number > len(q.Values)
}
func (q *Query) consume(elem btree.Item) bool {
	if !q.want() { return false }
	q.Values = append(q.Values,elem)
	return true
}

func (q *Query) ApplyTo(tree *btree.BTree) {
	tree.AscendGreaterOrEqual(q.Key,q.consume)
	tree.AscendLessThan(q.Key,q.consume)
}


