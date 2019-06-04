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



package lsm

import (
	"sync"
	"github.com/byte-mug/zrab2k/kvtp"
	"github.com/byte-mug/zrab2k/rpcmux"
	"github.com/dgraph-io/badger"
	"github.com/byte-mug/zrab2k/storage2"
)

const (
	state_none uint8 = iota
	state_read
	state_write
)

func respErr(err string,pool *sync.Pool) *kvtp.Response {
	resp := pool.Get().(*kvtp.Response)
	resp.Code = kvtp.RESP_Error
	resp.ExpiresAt = 0
	resp.Val = append(resp.Val[:0],err...)
	return resp
}

func respOk(pool *sync.Pool) *kvtp.Response {
	resp := pool.Get().(*kvtp.Response)
	resp.Code = kvtp.RESP_None
	resp.ExpiresAt = 0
	resp.Val = resp.Val[:0]
	return resp
}

type pendings []*rpcmux.Request
func (p *pendings) add(r *rpcmux.Request) bool {
	if len(*p)>cap(*p) {
		return false
	}
	*p = append(*p,r)
	return true
}
/* Remove the Value last added to the list. */
func (p *pendings) pop(r *rpcmux.Request) {
	pp := *p
	if len(pp)==0 { return }
	if pp[len(pp)-1]!=r { return }
	pp[len(pp)-1] = nil
	*p = pp[:len(pp)-1]
}
func (p pendings) done(pool *sync.Pool) func(error) {
	return func(e error) {
		if e!=nil {
			s := e.Error()
			for _,r := range p {
				r.Reply(respErr(s,pool))
				r.Release()
			}
		} else {
			for _,r := range p {
				r.Reply(respOk(pool))
				r.Release()
			}
		}
		for i := range p { p[i] = nil }
		pendingsPool.Put(p[:0])
	}
}

var pendingsPool = sync.Pool{New: func() interface{} {
	return make(pendings,0,16)
}}

type Layer struct{
	storage2.LayerPoint
	DB *badger.DB
	read_c,write_c chan *rpcmux.Request
	sig_sync chan int
}
func (l *Layer) reader() {
	var req *rpcmux.Request
	for {
		select {
		case <- l.Die: return
		case req = <- l.read_c:
		}
		if req==nil {
			continue
		}
		//msg := req.Msg.(*kvtp.Request)
	}
}
func (l *Layer) writer() {
	var req *rpcmux.Request
	
	pends := pendingsPool.Get().(pendings)
	
	tx := l.DB.NewTransaction(true)
	for {
		select {
		case <- l.Die:
			tx.Discard()
			return
		case req = <- l.write_c:
		}
		if req==nil {
			tx.CommitWith(pends.done(l.Resps))
			tx = l.DB.NewTransaction(true)
			pends = pendingsPool.Get().(pendings)
			select {
			case <- l.Die:
				tx.Discard()
				return
			case l.sig_sync <- 1:
			}
			continue
		}
		msg := req.Msg.(*kvtp.Request)
		//if ksg.Cmd != kvtp.CMD_Put
		if !pends.add(req) {
			tx.CommitWith(pends.done(l.Resps))
			tx = l.DB.NewTransaction(true)
			pends = pendingsPool.Get().(pendings)
		}
		switch msg.Cmd {
		case kvtp.CMD_Put:
			ent := &badger.Entry{Key:msg.Key,Value:msg.Val,ExpiresAt:msg.ExpiresAt}
			err := tx.SetEntry(ent)
			if err==badger.ErrTxnTooBig {
				pends.pop(req)
				tx.CommitWith(pends.done(l.Resps))
				tx = l.DB.NewTransaction(true)
				pends = pendingsPool.Get().(pendings)
				
				pends.add(req)
				err = tx.SetEntry(ent)
			}
			if err!=nil {
				pends.pop(req)
				req.Reply(respErr(err.Error(),l.Resps))
				req.Release()
			}
		}
	}
}
func (l *Layer) Loop() {
	var req *rpcmux.Request
	l.read_c = make(chan *rpcmux.Request,16)
	l.write_c = make(chan *rpcmux.Request,16)
	l.sig_sync = make(chan int,1)
	
	queue := make([]*rpcmux.Request,0,16)
	state := state_none
	for {
		select {
		case <- l.Die: return
		case req = <- l.Source:
		}
		msg := req.Msg.(*kvtp.Request)
		switch msg.Cmd {
		case kvtp.CMD_Put:
			switch {
			case len(queue)==0: fallthrough
			case state==state_none:
				state = state_write
				fallthrough
			case state==state_write:
				l.write_c <- req
			case len(queue)<16:
				queue = append(queue,req)
			default:
				// sync.
				l.write_c <- nil
				l.read_c <- nil
				
				// flush.
				for _,req2 := range queue { l.read_c <- req2 }
				queue = queue[:0]
				
				state = state_write
				l.write_c <- req
			}
		case kvtp.CMD_Get:
			switch {
			case len(queue)==0: fallthrough
			case state==state_none:
				state = state_read
				fallthrough
			case state==state_read:
				l.read_c <- req
			case len(queue)<16:
				queue = append(queue,req)
			default:
				// flush.
				for _,req2 := range queue { l.write_c <- req2 }
				queue = queue[:0]
				
				state = state_read
				l.read_c <- req
			}
		}
	}
}