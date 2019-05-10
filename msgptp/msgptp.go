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


package msgptp

import "io"
import "bufio"
import "sync"
import "github.com/byte-mug/zrab2k/rpcmux"
import "github.com/vmihailenco/msgpack"

var block,nonblock chan uint8

func init() {
	block = make(chan uint8)
	nonblock = make(chan uint8)
	close(nonblock)
}

type conn struct {
	rpcmux.Stream
	cdie chan struct{}
	cin  chan rpcmux.Message
	cout chan rpcmux.Message
	conn io.ReadWriteCloser
	buf  *bufio.Writer
	in   *msgpack.Decoder
	out  *msgpack.Encoder
	pin  *sync.Pool
	pout *sync.Pool
}
func (c *conn) init(conn io.ReadWriteCloser) {
	c.Err = nil
	c.cdie = make(chan struct{})
	c.cin = make(chan rpcmux.Message,64)
	c.cout = make(chan rpcmux.Message,64)
	c.In = c.cin
	c.Out = c.cout
	c.Die = c.cdie
	c.conn = conn
	c.buf  = bufio.NewWriter(c.buf)
	c.InRelease = c.releaseIn
}

func NewStream(cc io.ReadWriteCloser, pin, pout *sync.Pool) (*rpcmux.Stream) {
	c := new(conn)
	c.init(cc)
	c.pin  = pin
	c.pout = pout
	
	go c.recvLoop()
	go c.sendLoop()
	return &(c.Stream)
}

func (c *conn) releaseIn(m rpcmux.Message) {
	c.pin.Put(m)
}
func (c *conn) die() {
	defer func(){ recover() }()
	close(c.cdie)
}
func (c *conn) recvLoop() {
	defer c.die()
	for {
		select {
		case <- c.Die: return
		default:
		}
		msg := c.pin.Get().(rpcmux.Message)
		err := c.in.Decode(msg)
		if err!=nil {
			if c.Err==nil { c.Err = err }
			return
		}
		select {
		case <- c.Die: return
		case c.cin <- msg: continue
		}
	}
}
func (c *conn) sendMsg(msg rpcmux.Message) (died bool) {
	err := c.out.Encode(msg)
	if c.pout!=nil { c.pout.Put(msg) }
	if err!=nil {
		if c.Err==nil { c.Err = err }
		return true
	}
	return false
}
func (c *conn) flushBuf() (died bool) {
	err := c.buf.Flush()
	if err!=nil {
		if c.Err==nil { c.Err = err }
		return true
	}
	return false
}
func (c *conn) sendStep(nb <- chan uint8) (died, rupt bool) {
	{
		select {
		case <- c.Die: died = true; return
		case <- nb: rupt = true; return
		case msg := <- c.cout: if c.sendMsg(msg) { died = true; return }
		}
	}
	return
}
func (c *conn) sendLoop() {
	defer c.die()
	
	for {
		select {
		case <- c.Die: return
		default:
		}
		died,_ := c.sendStep(block)
		if died { return }
		for {
			died,rupt := c.sendStep(nonblock)
			if rupt { break }
			if died { return }
		}
		if c.flushBuf() { return }
	}
}

