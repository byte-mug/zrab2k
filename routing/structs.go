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



package routing

import (
	"github.com/byte-mug/zrab2k/rpcmux"
)

type RedirectReader interface{
	// Note: This method can also be used for Write requests.
	// The only difference is, that it is explicitely targeted towards
	// a particular node, backend, etc... you name it.
	RedirectRead(other string,req *rpcmux.Request) bool
}

type RedirectWriter interface{
	RedirectWrite(req *rpcmux.Request) (string,bool)
}

type NodeGoodness interface{
	RequestGoodness(other string) uint64
}

func ForwardResponse(resp *rpcmux.Response, req *rpcmux.Request) {
	defer req.Release()
	msg,_ := resp.Get()
	if msg!=nil {
		req.Reply(msg)
	} else {
		req.ReplyDefault()
	}
}

func Forward(req *rpcmux.Request, cli rpcmux.Client) error {
	resp,err := cli.Request(req.Msg,req.Context())
	if err!=nil { return err }
	if resp==nil {
		req.ReplyDefault()
		req.Release()
	}
	go ForwardResponse(resp,req)
	return nil
}

