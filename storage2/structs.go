/*
Copyright (c) 2019 Simon Schmidt

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



package storage2

import (
	"sync"
	//"github.com/byte-mug/zrab2k/kvtp"
	"github.com/byte-mug/zrab2k/rpcmux"
)

type LayerPoint struct{
	Die    <-chan struct{}
	Source <- chan *rpcmux.Request
	Next   chan <- *rpcmux.Request
	Resps  *sync.Pool
}

type EndPoint struct{
	Die    <-chan struct{}
	Source <- chan *rpcmux.Request
	Resps  *sync.Pool
}

type DiskSpace interface{
	HasEnoughDiskSpace(key, value []byte) bool
	AccountForDiskSpace(key, value []byte)
}
type infiniteDiskSpace struct{}
func (i infiniteDiskSpace) HasEnoughDiskSpace(key, value []byte) bool { return true }
func (i infiniteDiskSpace) AccountForDiskSpace(key, value []byte) {}
func InfiniteDiskSpace() DiskSpace { return infiniteDiskSpace{} }

type RedirectReader interface{
	RedirectRead(other string,req *rpcmux.Request) bool
}
type RedirectWriter interface{
	RedirectWrite(req *rpcmux.Request) (string,bool)
}

