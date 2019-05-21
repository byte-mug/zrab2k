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


package rpcmux

import "sync"
import "context"
import "fmt"

func debug(i ...interface{}) {
	fmt.Println(i...)
}

type Message interface {
	Seq() uint64
	SetSeq(u uint64)
}

type Stream struct {
	Err error
	Die <- chan struct{}
	In <- chan Message
	Out chan <- Message
	
	// memory management
	InRelease func(m Message) // release messages, received through s.In
	
	// Support for canceling requests.
	Cancel func() Message // generate a cancel-message.
	IsCancel func(m Message) bool // detect a cancel-message.
}

/*
A Request, a server can respond to.
*/
type Request struct {
	Msg Message
	srv *server
	lctx context.Context
	lcf context.CancelFunc
	seq uint64
	sig chan uint8
}
func (r *Request) clear() {
	*r = Request{sig:r.sig}
}
func (r *Request) cancel() {
	select {
	case r.sig <- 0:
	default:
	}
}
func (r *Request) uncancel() {
	select {
	case <- r.sig:
	default:
	}
}
func (r *Request) testcancel() bool {
	select {
	case <- r.sig:
		r.cancel()
		return true
	default:
	}
	return false
}
func (r *Request) pollfunc() {
	select {
	case <- r.sig: r.lcf()
	case <- r.lctx.Done():
	}
}
func (r *Request) getCtx() context.Context {
	if r.lctx!=nil { return r.lctx }
	return r.srv.ctx
}

/*
Get a context. Useful since the server can cancel requests.
*/
func (r *Request) Context() context.Context {
	if r.lctx==nil {
		r.lctx,r.lcf = context.WithCancel(r.srv.ctx)
		go r.pollfunc()
	}
	return r.lctx
}

/*
This method should be called after the request has been processed (both successfully or unsuccessfully).
*/
func (r *Request) Release() {
	if r.lcf!=nil { r.lcf() }
	srv := r.srv
	r.srv = nil
	srv.rele <- r
}

/*
Replies with a message. Reply() calls m.SetSeq() with the correct sequence number.

Returns true if the message has been pushed to the Stream.Out queue, false otherwise.
*/
func (r *Request) Reply(m Message) (taken bool) {
	m.SetSeq(r.seq)
	select {
	case <- r.getCtx().Done(): return
	case <- r.sig:
		r.cancel()
		return
	default:
	}
	select {
	case <- r.getCtx().Done(): return
	case <- r.sig:
		r.cancel()
		return
	case r.srv.base.Out <- m:
		if r.lcf!=nil { r.lcf() }
		return true
	}
	return
}

func nRequest() interface{} {
	return &Request{sig:make(chan uint8,1)}
}

type server struct {
	ctx  context.Context
	base *Stream
	reqm map[uint64]*Request
	pool sync.Pool
	rele chan *Request
	reqq chan *Request
}
func (srv *server) init(b *Stream) *server{
	if b.InRelease==nil { b.InRelease = func(m Message) {} }
	if b.IsCancel==nil { b.IsCancel = func(m Message) bool { return false } }
	srv.base = b
	srv.reqm = make(map[uint64]*Request)
	srv.pool.New = nRequest
	srv.rele = make(chan *Request,128)
	srv.reqq = make(chan *Request,128)
	return srv
}
func (srv *server) dispatch() {
	var cf context.CancelFunc
	srv.ctx,cf = context.WithCancel(context.Background())
	defer cf()
	for {
		select {
		case <- srv.base.Die: return
		default:
		}
		select {
		case <- srv.base.Die: return
		case req := <- srv.rele:
			if r := srv.reqm[req.seq] ; r==req {
				delete(srv.reqm,req.seq)
			}
			if req.Msg!=nil { srv.base.InRelease(req.Msg) }
			req.clear()
			srv.pool.Put(req)
		case msg := <- srv.base.In:
			if msg==nil { continue }
			seq := msg.Seq()
			if r,ok := srv.reqm[seq] ; ok { r.cancel() }
			r := srv.pool.Get().(*Request)
			r.uncancel()
			r.seq = seq
			r.srv = srv
			r.Msg = msg
			if srv.base.IsCancel(msg) {
				delete(srv.reqm,seq)
			} else {
				srv.reqm[seq] = r
				srv.reqq <- r
			}
		}
	}
}
func (s *Stream) Serve() (requests <- chan *Request) {
	srv := new(server).init(s)
	go srv.dispatch()
	requests = srv.reqq
	return
}

type Response struct {
	cli *client
	lctx  context.Context
	msg Message
	seq uint64
	sig chan uint8
}
func (r *Response) clear() {
	*r = Response{sig:r.sig}
}
func (r *Response) done() {
	select {
	case r.sig <- 0:
	default:
	}
}
func (r *Response) undone() {
	select {
	case <- r.sig:
	default:
	}
}
func (r *Response) testdone() bool {
	select {
	case <- r.sig:
		r.done()
		return true
	default:
	}
	return false
}
func (r *Response) testcancel() bool {
	select {
	case <- r.sig:
		r.done()
		return true
	case <- r.lctx.Done():
		return true
	default:
	}
	return false
}

/*
Saves the response message from recycling.

After this function, the user is responsible for recycling it, if memory pooling desired.
*/
func (r *Response) RetainMsg() {
	r.msg = nil
}

/*
Retrieves the Message, waiting if necessary.

The returned message is subject to recycling using a memory pool provided by the *Stream structure.

If you want to retain it beyond .Release() you must call .RetainMsg().
*/
func (r *Response) Get() (Message,error) {
	select {
	case <- r.sig:
		r.done()
		return r.msg,nil
	case <- r.cli.ctx.Done():
		if r.cli.base.Err!=nil { return nil,r.cli.base.Err }
		return nil,r.cli.ctx.Err()
	case <- r.lctx.Done():
		return nil,r.lctx.Err()
	}
	panic("unreachable")
}

/*
Should be called after the client is done with the response.
*/
func (r *Response) Release() {
	cli := r.cli
	r.cli = nil
	cli.rele <- r
}

func nResponse() interface{} {
	return &Response{sig:make(chan uint8,1)}
}

type client struct{
	ctx  context.Context
	ready chan int
	base *Stream
	seqs chan uint64
	reqm map[uint64]*Response
	pool sync.Pool
	rele chan *Response
	addq chan *Response
}
func (cli *client) init(b *Stream) *client{
	if b.InRelease==nil { b.InRelease = func(m Message) {} }
	if b.Cancel==nil { b.Cancel = func() Message { return nil } }
	
	// XXX: This will be set, in the dispatch loop!
	cli.ctx = nil
	
	// We need to signal, if cli.ctx is set.
	cli.ready = make(chan int)
	
	cli.base = b
	cli.seqs = make(chan uint64,16)
	cli.reqm = make(map[uint64]*Response)
	cli.pool.New = nResponse
	cli.rele = make(chan *Response,128)
	cli.addq = make(chan *Response,128)
	return cli
}
func (cli *client) nextSeq(pseq *uint64) {
	for {
		*pseq++
		if _,ok := cli.reqm[*pseq]; !ok { return }
	}
}
func (cli *client) dispatch() {
	var cf context.CancelFunc
	cli.ctx,cf = context.WithCancel(context.Background())
	defer cf()
	
	// Unlock cli.ready, because cli.ctx is set to a valid context.
	close(cli.ready)
	seq := uint64(0)
	for {
		select {
		case <- cli.base.Die: return
		case cli.seqs <- seq: cli.nextSeq(&seq); continue
		case req := <- cli.addq: cli.reqm[req.seq] = req; continue
		default:
		}
		select {
		case <- cli.base.Die: return
		case cli.seqs <- seq: cli.nextSeq(&seq); continue
		case req := <- cli.addq: cli.reqm[req.seq] = req; continue
		case msg := <- cli.base.In:
			seq := msg.Seq()
			if r := cli.reqm[seq]; r!=nil {
				if !r.testcancel() {
					r.msg = msg
					r.done()
				}
				/* Remove it from the queue. */
				delete(cli.reqm,seq)
			}
		case req := <- cli.rele:
			seq := req.seq
			if r := cli.reqm[seq]; r==req {
				if !r.testdone() { /* Send a cancel request, if supported. */
					msg := cli.base.Cancel()
					if msg!=nil {
						msg.SetSeq(seq)
						cli.base.Out <- msg
					}
				}
				/* Remove it from the queue. */
				delete(cli.reqm,seq)
			}
			if msg := req.msg; msg!=nil {
				cli.base.InRelease(msg)
			}
			req.clear()
			cli.pool.Put(req)
		}
	}
}

/*
Submits a request to the server.

Parameter msg: The request message.

Parameter ctx: A context.Context. Use context.Background() if unsure!

If resp is not-nil, you should call resp.Release() after you are done.
*/
func (cli *client) Request(msg Message,ctx context.Context) (resp *Response,err error) {
	if ctx==nil { panic("ctx == nil") }
	var seq uint64
	
	// Wait for cli.ready, otherwise cli.ctx will be nil.
	<- cli.ready
	
	select {
	case <- cli.ctx.Done(): err = cli.ctx.Err() ; return 
	case seq = <- cli.seqs:
	}
	msg.SetSeq(seq)
	req := cli.pool.Get().(*Response)
	req.cli = cli
	req.seq = seq
	req.lctx = ctx
	select {
	case <- cli.ctx.Done(): err = cli.ctx.Err() ; return 
	case cli.base.Out <- msg:
	}
	select {
	case <- cli.ctx.Done(): err = cli.ctx.Err() ; return 
	case cli.addq <- req:
	}
	resp = req
	return
}

type Client interface {
	// Submits a request to the server.
	// 
	// Parameter msg: The request message.
	// 
	// Parameter ctx: A context.Context. Use context.Background() if unsure!
	// 
	// If resp is not-nil, you should call resp.Release() after you are done.
	Request(msg Message,ctx context.Context) (resp *Response,err error)
}

func (s *Stream) Client() (c Client) {
	cli := new(client).init(s)
	go cli.dispatch()
	<- cli.ready
	c = cli
	return
}

