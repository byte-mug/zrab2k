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

// Warning: Unexpected API changes will happen.
package kvtp

import "sync"
import "github.com/byte-mug/zrab2k/rpcmux"
import "github.com/vmihailenco/msgpack"
import "time"

/*
Commands.
*/
const (
	CMD_Cancel = iota
	
	/* Coordinator Server. */
	CMD_Get
	CMD_Put
	
	/* Replication Server. */
	CMD_RsGet
	CMD_RsPut
	CMD_ReadRepair
	CMD_HintedHandoff
)

/*
Response Codes.
*/
const (
	RESP_None = iota
	
	RESP_WriteResponse
	RESP_ReadFound
	RESP_ReadNotFound
	RESP_Exception
)

/*
Consistency Levels.
*/
const (
	CL_NONE = iota
	CL_ONE
	CL_TWO
)

type Entry struct{
	Time time.Time
	Val  []byte
}
var emptyEntry = new(Entry)
func (r *Entry) DecodeMsgpack(m *msgpack.Decoder) error {
	return m.DecodeMulti(&r.Time,&r.Val)
}
func (r *Entry) EncodeMsgpack(m *msgpack.Encoder) error {
	return m.EncodeMulti(&r.Time,&r.Val)
}

func (r *Entry) put(t time.Time,v []byte) *Entry {
	if r==nil { return &Entry{t,v} }
	r.Time = t
	r.Val = append(r.Val[:0],v...)
	return r
}
func (r *Entry) get(t *time.Time,v *[]byte) {
	if r==nil { r = emptyEntry }
	*t = r.Time
	*v = append((*v)[:0],r.Val...)
}

type Request struct{
	seq uint64
	Cmd uint8
	Consistency uint8
	Time time.Time
	Key []byte
	Val []byte
}
func (r *Request) Seq() uint64 { return r.seq }
func (r *Request) SetSeq(u uint64) { r.seq = u }
func (r *Request) DecodeMsgpack(m *msgpack.Decoder) error {
	return m.DecodeMulti(&r.seq,&r.Cmd,&r.Consistency,&r.Time,&r.Key,&r.Val)
}
func (r *Request) EncodeMsgpack(m *msgpack.Encoder) error {
	return m.EncodeMulti(&r.seq,&r.Cmd,&r.Consistency,&r.Time,&r.Key,&r.Val)
}
func (r *Request) GetEntry(e *Entry) *Entry { return e.put(r.Time,r.Val) }
func (r *Request) FromEntry(e *Entry) { e.get(&r.Time,&r.Val) }

type Response struct{
	seq uint64
	Code uint8
	Time time.Time
	Val []byte
}
func (r *Response) Seq() uint64 { return r.seq }
func (r *Response) SetSeq(u uint64) { r.seq = u }
func (r *Response) DecodeMsgpack(m *msgpack.Decoder) error {
	return m.DecodeMulti(&r.seq,&r.Code,&r.Time,&r.Val)
}
func (r *Response) EncodeMsgpack(m *msgpack.Encoder) error {
	return m.EncodeMulti(&r.seq,&r.Code,&r.Time,&r.Val)
}
func (r *Response) GetEntry(e *Entry) *Entry { return e.put(r.Time,r.Val) }
func (r *Response) FromEntry(e *Entry) { e.get(&r.Time,&r.Val) }


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
		r.Key = r.Key[:0]
		r.Val = r.Val[:0]
		return r
	}
}

