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

/*
Implements a Protocol for rpcmux, that implements a Key-Value-Store API.

Just be warned. Unstable! API and protocol will change without notice.
*/
package kvtp

import "sync"
import "github.com/byte-mug/zrab2k/rpcmux"
import "github.com/vmihailenco/msgpack"

/*
Commands.
*/
const (
	CMD_Cancel = iota
	CMD_Get
	CMD_GetNoRedirect
	CMD_Put
	CMD_PutNoRedirect
	
	/*
	Returns a Log of hops.
	*/
	CMD_Trace
	
	/*
	Returns "ok" if the key is available, "not_found" otherwise.
	*/
	CMD_Touch
	
)

/*
Responses
*/
const (
	RESP_None = iota
	RESP_Error
	RESP_Value
	RESP_NotFound
)

type Request struct{
	seq uint64
	Cmd uint8
	ExpiresAt uint64 /* time.Unix(), 0 -> no expiration. */
	Key []byte
	Val []byte
}
func (r *Request) Seq() uint64 { return r.seq }
func (r *Request) SetSeq(u uint64) { r.seq = u }
func (r *Request) DecodeMsgpack(m *msgpack.Decoder) error {
	return m.DecodeMulti(&r.seq,&r.Cmd,&r.ExpiresAt,&r.Key,&r.Val)
}
func (r *Request) EncodeMsgpack(m *msgpack.Encoder) error {
	return m.EncodeMulti(&r.seq,&r.Cmd,&r.ExpiresAt,&r.Key,&r.Val)
}

type Response struct{
	seq uint64
	Code uint8
	ExpiresAt uint64 /* time.Unix(), 0 -> no expiration. */
	Val []byte
}
func (r *Response) Seq() uint64 { return r.seq }
func (r *Response) SetSeq(u uint64) { r.seq = u }
func (r *Response) DecodeMsgpack(m *msgpack.Decoder) error {
	return m.DecodeMulti(&r.seq,&r.Code,&r.ExpiresAt,&r.Val)
}
func (r *Response) EncodeMsgpack(m *msgpack.Encoder) error {
	return m.EncodeMulti(&r.seq,&r.Code,&r.ExpiresAt,&r.Val)
}

var _ rpcmux.Message = (*Request)(nil)
var _ rpcmux.Message = (*Response)(nil)

func NewRequest() interface{} { return &Request{Key:make([]byte,0,1<<9),Val:make([]byte,0,1<<14)} }
func NewResponse() interface{} { return &Response{Val:make([]byte,0,1<<14)} }

func ReqIsCancel(m rpcmux.Message) bool {
	r := m.(*Request)
	return r.Cmd==0
}

func ReqCancel(p *sync.Pool) func() rpcmux.Message {
	return func() rpcmux.Message {
		r := p.Get().(*Request)
		r.Cmd = 0
		r.ExpiresAt = 0
		r.Key = r.Key[:0]
		r.Val = r.Val[:0]
		return r
	}
}
func RespDefault(p *sync.Pool) func() rpcmux.Message {
	return func() rpcmux.Message {
		r := p.Get().(*Response)
		r.Code = 0
		r.ExpiresAt = 0
		r.Val = r.Val[:0]
		return r
	}
}
