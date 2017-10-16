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

import "hash/crc64"
import "crypto/md5"

var crctable = crc64.MakeTable(crc64.ECMA)

func CrcStep(crc uint64,b byte) uint64 {
	crc = ^crc
	crc = crctable[byte(crc)^b] ^ (crc >> 8)
	return ^crc
}

func CrcMd5(crc uint64,b [md5.Size]byte) uint64 {
	crc = ^crc
	for _,v := range b {
		crc = crctable[byte(crc)^v] ^ (crc >> 8)
	}
	return ^crc
}
func CrcInt(crc uint64,i int) uint64 {
	crc = ^crc
	crc = crctable[byte(crc)^byte(i    )] ^ (crc >> 8)
	crc = crctable[byte(crc)^byte(i>> 8)] ^ (crc >> 8)
	crc = crctable[byte(crc)^byte(i>>16)] ^ (crc >> 8)
	crc = crctable[byte(crc)^byte(i>>24)] ^ (crc >> 8)
	return ^crc
}


type Binary struct {
	data []byte
}
func NewBinary(b []byte) Binary { return Binary{b} }
func StrBinary(b string) Binary { return Binary{[]byte(b)} }
func (b Binary) Md5() [md5.Size]byte { return md5.Sum(b.data) }
func (b Binary) Md5Crc() uint64 { return CrcMd5(0,md5.Sum(b.data)) }
func (b Binary) String() string { return string(b.data) }
func (b Binary) ToBytes(prefix []byte) []byte { return append(prefix,b.data...) }

// returns the following byte sequence: MD5(b) || b
func (b Binary) ToHashRangeKey(buffer []byte) []byte {
	md := b.Md5()
	return b.ToBytes(append(buffer[:0],md[:]...))
}

