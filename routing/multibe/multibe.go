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



package multibe

import (
	"context"
	"sync"
	"github.com/byte-mug/zrab2k/rpcmux"
	"github.com/byte-mug/zrab2k/routing"
)

var died <-chan struct{}
func init(){
	dead := make(chan struct{})
	close(dead)
	died = dead
}

type Dialer func(str string) (*rpcmux.Stream,error)

type Client struct {
	parent *Forwarder
	node string
	nlck sync.Mutex
	died <-chan struct{}
	
	stream *rpcmux.Stream
	client rpcmux.Client
}
func (c *Client) reinstantiate() error {
	// Check!
	select {
	case <- c.died:
	default: return nil
	}
	
	c.nlck.Lock()
	defer c.nlck.Unlock()
	
	// Recheck!
	select {
	case <- c.died:
	default: return nil
	}
	
	stream,err := c.parent.Dial(c.node)
	if err!=nil { return err }
	
	c.stream = stream
	c.died = stream.Die
	c.client = stream.Client()
	return nil
}
func (c *Client) Request(msg rpcmux.Message, ctx context.Context) (resp *rpcmux.Response, err error) {
	err = c.reinstantiate()
	if err==nil {
		resp,err = c.client.Request(msg,ctx)
	}
	return
}

type Forwarder struct {
	Dial     Dialer
	ReadOnly bool
	
	ndmap map[string] *Client
	ndmpl sync.Mutex
}
func (s *Forwarder) create(node string) *Client {
	s.ndmpl.Lock()
	defer s.ndmpl.Unlock()
	cl,ok := s.ndmap[node]
	if ok { return cl }
	cl = &Client{parent:s,node:node,died:died}
	nnmap := make(map[string] *Client,len(s.ndmap)+1)
	for k,v := range s.ndmap { nnmap[k] = v }
	nnmap[node] = cl
	
	// TODO: Compiler-Barrier.
	s.ndmap = nnmap
	return cl
}

func (s *Forwarder) Node(node string) (cl *Client,ok bool) {
	cl,ok = s.ndmap[node]
	if !(ok || s.ReadOnly) {
		cl = s.create(node)
		ok = true
	}
	return
}
func (s *Forwarder) RedirectRead(other string,req *rpcmux.Request) bool {
	cli,ok := s.Node(other)
	if !ok { return false }
	return routing.Forward(req,cli)!=nil
}

type Selector struct{
	routing.RedirectReader
	routing.NodeGoodness
	Nodes []string
	MinGoodness uint64
}
func (s *Selector) RedirectWrite(req *rpcmux.Request) (string,bool) {
	cur := ""
	goodness := uint64(0)
	for _,n := range s.Nodes {
		cgn := s.RequestGoodness(n)
		if goodness<=s.MinGoodness || len(cur)==0 || goodness<cgn {
			cur = n
			goodness = cgn
		}
	}
	if goodness<=s.MinGoodness || len(cur)==0 { return "",false }
	if s.RedirectRead(cur,req) { return cur,true }
	return "",false
}

