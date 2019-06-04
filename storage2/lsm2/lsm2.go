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



package lsm2


import (
	"sync"
	"github.com/byte-mug/zrab2k/kvtp"
	"github.com/byte-mug/zrab2k/rpcmux"
	"github.com/dgraph-io/badger"
	"github.com/dgraph-io/badger/y"
	"github.com/byte-mug/zrab2k/storage2"
	"time"
)

import "fmt"

func debug(i ...interface{}) {
	fmt.Println(i...)
}

const (
	t_data = iota
	t_redirect
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

func getstr(i *badger.Item) string {
	var s string
	i.Value(func(val []byte) error {
		s = string(val)
		return nil
	})
	return s
}

func nBatchJob() interface{} {
	return &batchJob{make([]*rpcmux.Request,0,32),nil,nil,nil}
}
var pBatchJob = sync.Pool{New:nBatchJob}
type batchJob struct{
	requests []*rpcmux.Request
	pool *sync.Pool
	sync *chan struct{}
	thro *y.Throttle
}
func (b *batchJob) hasSpace(max int) bool {
	return len(b.requests) < max
}
func (b *batchJob) add(i *rpcmux.Request) {
	b.requests = append(b.requests,i)
}
func (b *batchJob) done(e error) {
	b.thro.Done(nil)
	osync := *b.sync
	*b.sync = make(chan struct{})
	close(osync)
	if e!=nil {
		s := e.Error()
		for _,req := range b.requests{
			req.Reply(respErr(s,b.pool))
			req.Release()
		}
	} else {
		for _,req := range b.requests{
			req.Reply(respOk(b.pool))
			req.Release()
		}
	}
	for i := range b.requests { b.requests[i] = nil }
	b.requests = b.requests[:0]
	b.pool = nil
	b.sync = nil
	b.thro = nil
	pBatchJob.Put(b)
}

type DB struct{
	storage2.EndPoint
	DS storage2.DiskSpace
	RR storage2.RedirectReader
	RW storage2.RedirectWriter
	DB *badger.DB
	read chan *rpcmux.Request
	sync chan struct{}
}
func (db *DB) Init(readers int) {
	db.read = make(chan *rpcmux.Request,16)
	db.sync = make(chan struct{})
	if db.DS==nil { db.DS = storage2.InfiniteDiskSpace() }
	go db.writer()
	for i := 0 ; i<readers ; i++ { go db.reader() }
}
func (db *DB) writer() {
	var req *rpcmux.Request
	
	var tmout <- chan time.Time
	thro := y.NewThrottle(16)
	
	tx := db.DB.NewTransaction(true)
	bj := pBatchJob.Get().(*batchJob)
	bj.pool = db.Resps
	bj.sync = &db.sync
	bj.thro = thro
	for {
		req = nil
		// Peek!
		select {
		case req = <- db.Source: goto skip
		default:
		}
		
		// Wait!
		select {
		case <- db.Die:
			tx.Discard()
			return
		case req = <- db.Source:
		case <- tmout:
		}
skip:
		if req==nil || !bj.hasSpace(32) {
			thro.Do()
			tx.CommitWith(bj.done)
			tx = db.DB.NewTransaction(true)
			bj = pBatchJob.Get().(*batchJob)
			bj.pool = db.Resps
			bj.sync = &db.sync
			bj.thro = thro
			tmout = nil
		}
		if req==nil { continue }
		msg := req.Msg.(*kvtp.Request)
		switch msg.Cmd {
		case kvtp.CMD_Put,kvtp.CMD_PutNoRedirect:
			if !db.DS.HasEnoughDiskSpace(msg.Key,msg.Val) {
				str,ok := "",false
				ent := &badger.Entry{Key:append([]byte{},msg.Key...),UserMeta:t_redirect,ExpiresAt:msg.ExpiresAt}
				if db.RW!=nil && msg.Cmd!=kvtp.CMD_PutNoRedirect {
					msg.Cmd = kvtp.CMD_PutNoRedirect
					str,ok = db.RW.RedirectWrite(req)
				}
				if ok {
					ent.Value = []byte(str)
					err := tx.SetEntry(ent)
					if err==badger.ErrTxnTooBig {
						// Flush Txn
						thro.Do()
						tx.CommitWith(bj.done)
						tx = db.DB.NewTransaction(true)
						bj = pBatchJob.Get().(*batchJob)
						bj.pool = db.Resps
						bj.sync = &db.sync
						bj.thro = thro
						tmout = nil
						
						// Retry
						tx.SetEntry(ent)
					}
				} else {
					req.Reply(respErr("Disk full and not redirection",db.Resps))
					req.Release()
				}
				continue
			}
			ent := &badger.Entry{Key: msg.Key, Value: msg.Val, ExpiresAt: msg.ExpiresAt}
			err := tx.SetEntry(ent)
			if err==badger.ErrTxnTooBig {
				// Flush Txn
				thro.Do()
				tx.CommitWith(bj.done)
				tx = db.DB.NewTransaction(true)
				bj = pBatchJob.Get().(*batchJob)
				bj.pool = db.Resps
				bj.sync = &db.sync
				bj.thro = thro
				tmout = nil
				
				// Retry
				err = tx.SetEntry(ent)
			}
			if err!=nil {
				req.Reply(respErr(err.Error(),db.Resps))
				req.Release()
				continue
			}
			if tmout==nil {
				tmout = time.After(time.Millisecond*10)
			}
			db.DS.AccountForDiskSpace(msg.Key,msg.Val)
			bj.add(req)
		case kvtp.CMD_Get,kvtp.CMD_Touch,kvtp.CMD_Trace:
			db.read <- req
		default:
			req.Reply(respErr("Command Unsupported",db.Resps))
			req.Release()
		}
	}
}
func (db *DB) reader() {
	var req *rpcmux.Request
	
	sync := db.sync
	
	tx := db.DB.NewTransaction(false)
	for {
		select {
		case <- db.Die:
			tx.Discard()
			return
		case req = <- db.read:
		}
		
		/* Sync! */
		select {
		case <- sync:
			tx.Discard()
			sync = db.sync
			tx = db.DB.NewTransaction(false)
		default:
		}
		
		msg := req.Msg.(*kvtp.Request)
		item,err := tx.Get(msg.Key)
		if err==nil {
			switch item.UserMeta() {
			case t_data:
			case t_redirect:
				if db.RR!=nil && msg.Cmd!=kvtp.CMD_GetNoRedirect {
					str := getstr(item)
					db.RR.RedirectRead(str,req)
					continue
				}
			default:
				{
					resp := db.Resps.Get().(*kvtp.Response)
					resp.Val = resp.Val[:0]
					resp.Code = kvtp.RESP_NotFound
					req.Reply(resp)
					req.Release()
				}
				continue
			}
		}
		
		resp := db.Resps.Get().(*kvtp.Response)
		resp.Val = resp.Val[:0]
		switch msg.Cmd {
		case kvtp.CMD_Get,kvtp.CMD_GetNoRedirect:
			val := resp.Val
			if err==nil {
				resp.Code = kvtp.RESP_Value
				resp.Val,err = item.ValueCopy(val)
			}
			if err==badger.ErrKeyNotFound {
				resp.Code = kvtp.RESP_NotFound
				resp.Val = val
			} else if err!=nil {
				resp.Code = kvtp.RESP_Error
				resp.Val = append(val,err.Error()...)
			}
		case kvtp.CMD_Trace:
			resp.Code = kvtp.RESP_Value
			if err==nil {
				resp.Val = append(resp.Val,"\n--ok"...)
			} else {
				resp.Val = append(resp.Val,"\n--not_found"...)
			}
		case kvtp.CMD_Touch:
			resp.Code = kvtp.RESP_Value
			if err==nil {
				resp.Val = append(resp.Val,"ok"...)
			} else {
				resp.Val = append(resp.Val,"not_found"...)
			}
		}
		
		req.Reply(resp)
		req.Release()
	}
}

//
