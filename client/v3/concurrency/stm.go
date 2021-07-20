// Copyright 2016 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package concurrency

import (
	"context"
	"math"

	v3 "go.etcd.io/etcd/client/v3"
)

// STM is an interface for software transactional memory.
type STM interface {
	// Get returns the value for a key and inserts the key in the txn's read set.
	// If Get fails, it aborts the transaction with an error, never returning.
	Get(key ...string) string
	// Put adds a value for a key to the write set.
	Put(key, val string, opts ...v3.OpOption)
	// Rev returns the revision of a key in the read set.
	Rev(key string) int64
	// Del deletes a key.
	Del(key string)

	// commit attempts to apply the txn's changes to the server.
	commit() *v3.TxnResponse
	reset()
}

// Isolation is an enumeration of transactional isolation levels which
// describes how transactions should interfere and conflict.
type Isolation int

const (
	// SerializableSnapshot provides serializable isolation and also checks
	// for write conflicts.
	SerializableSnapshot Isolation = iota
	// Serializable reads within the same transaction attempt return data
	// from the at the revision of the first read.
	Serializable
	// RepeatableReads reads within the same transaction attempt always
	// return the same data.
	RepeatableReads
	// ReadCommitted reads keys from any committed revision.
	ReadCommitted
)

// stmError safely passes STM errors through panic to the STM error channel.
type stmError struct{ err error }

type stmOptions struct {
	iso      Isolation
	ctx      context.Context
	prefetch []string
}

type stmOption func(*stmOptions)

// WithIsolation specifies the transaction isolation level.
func WithIsolation(lvl Isolation) stmOption {
	return func(so *stmOptions) { so.iso = lvl }
}

// WithAbortContext specifies the context for permanently aborting the transaction.
func WithAbortContext(ctx context.Context) stmOption {
	return func(so *stmOptions) { so.ctx = ctx }
}

// WithPrefetch is a hint to prefetch a list of keys before trying to apply.
// If an STM transaction will unconditionally fetch a set of keys, prefetching
// those keys will save the round-trip cost from requesting each key one by one
// with Get().
func WithPrefetch(keys ...string) stmOption {
	return func(so *stmOptions) { so.prefetch = append(so.prefetch, keys...) }
}

// apply: 业务逻辑实现
// NewSTM initiates a new STM instance, using serializable snapshot isolation by default.
func NewSTM(c *v3.Client, apply func(STM) error, so ...stmOption) (*v3.TxnResponse, error) {
	opts := &stmOptions{ctx: c.Ctx()}
	//应用配置选项
	for _, f := range so {
		f(opts)
	}
	if len(opts.prefetch) != 0 {
		f := apply
		apply = func(s STM) error {
			s.Get(opts.prefetch...)
			return f(s)
		}
	}
	//mkSTM 根据不同的隔离级别创建不同的stm实例
	//runSTM 根据创建的 stm 实例与业务逻辑实现开始运行
	return runSTM(mkSTM(c, opts), apply)
}

//mkSTM 根据不同的隔离级别创建不同的stm实例
func mkSTM(c *v3.Client, opts *stmOptions) STM {
	switch opts.iso {
	//可序列化快照
	case SerializableSnapshot:
		s := &stmSerializable{
			stm:      stm{client: c, ctx: opts.ctx},
			prefetch: make(map[string]*v3.GetResponse),
		}
		s.conflicts = func() []v3.Cmp {
			return append(s.rset.cmps(), s.wset.cmps(s.rset.first()+1)...)
		}
		return s
	//可序列化
	case Serializable:
		s := &stmSerializable{
			stm:      stm{client: c, ctx: opts.ctx},
			prefetch: make(map[string]*v3.GetResponse),
		}
		s.conflicts = func() []v3.Cmp { return s.rset.cmps() }
		return s
	//可重复度
	case RepeatableReads:
		s := &stm{client: c, ctx: opts.ctx, getOpts: []v3.OpOption{v3.WithSerializable()}}
		s.conflicts = func() []v3.Cmp { return s.rset.cmps() }
		return s
	//读已提交
	case ReadCommitted:
		s := &stm{client: c, ctx: opts.ctx, getOpts: []v3.OpOption{v3.WithSerializable()}}
		s.conflicts = func() []v3.Cmp { return nil }
		return s
	default:
		panic("unsupported stm")
	}
}

type stmResponse struct {
	resp *v3.TxnResponse
	err  error
}

func runSTM(s STM, apply func(STM) error) (*v3.TxnResponse, error) {
	outc := make(chan stmResponse, 1)
	//异步处理
	go func() {
		defer func() {
			if r := recover(); r != nil {
				e, ok := r.(stmError)
				if !ok {
					// client apply panicked
					// 如果不是 stmError 类型的错误，继续将panic往上报
					panic(r)
				}
				outc <- stmResponse{nil, e.err}
			}
		}()
		var out stmResponse
		//因为有可能产生冲突，所以需要 for 循环去不断重试。
		for {
			//初始化 rset，wset
			s.reset()
			//执行业务逻辑，如果出错就退出循环
			if out.err = apply(s); out.err != nil {
				break
			}
			//尝试提交事务，如果提交有返回，就退出循环
			//提交分两种实现：stm 与 stmSerializable
			if out.resp = s.commit(); out.resp != nil {
				break
			}
		}
		outc <- out
	}()
	//等待异步执行的结果
	r := <-outc
	return r.resp, r.err
}

// 通过 etcd 实现可重复读取的软事务
type stm struct {
	client *v3.Client
	ctx    context.Context
	// rset 是一个 map，保存读键值对和 revisions
	rset readSet
	// wset 是一个 map，保存覆盖的键及其值
	wset writeSet
	// getOpts 用于获取数据时设置 option, 不同的隔离级别会不一样
	getOpts []v3.OpOption
	// conflicts 计算 txn 上的当前冲突
	//是最后生成的冲突检测 options, 不同的隔离级别会不一样
	conflicts func() []v3.Cmp
}

type stmPut struct {
	val string
	op  v3.Op
}

type readSet map[string]*v3.GetResponse

func (rs readSet) add(keys []string, txnresp *v3.TxnResponse) {
	for i, resp := range txnresp.Responses {
		rs[keys[i]] = (*v3.GetResponse)(resp.GetResponseRange())
	}
}

// first returns the store revision from the first fetch
func (rs readSet) first() int64 {
	ret := int64(math.MaxInt64 - 1)
	for _, resp := range rs {
		if rev := resp.Header.Revision; rev < ret {
			ret = rev
		}
	}
	return ret
}

// cmps guards the txn from updates to read set
func (rs readSet) cmps() []v3.Cmp {
	cmps := make([]v3.Cmp, 0, len(rs))
	for k, rk := range rs {
		cmps = append(cmps, isKeyCurrent(k, rk))
	}
	return cmps
}

type writeSet map[string]stmPut

func (ws writeSet) get(keys ...string) *stmPut {
	for _, key := range keys {
		if wv, ok := ws[key]; ok {
			return &wv
		}
	}
	return nil
}

// cmps returns a cmp list testing no writes have happened past rev
func (ws writeSet) cmps(rev int64) []v3.Cmp {
	cmps := make([]v3.Cmp, 0, len(ws))
	for key := range ws {
		cmps = append(cmps, v3.Compare(v3.ModRevision(key), "<", rev))
	}
	return cmps
}

// puts is the list of ops for all pending writes
func (ws writeSet) puts() []v3.Op {
	puts := make([]v3.Op, 0, len(ws))
	for _, v := range ws {
		puts = append(puts, v.op)
	}
	return puts
}

//stm 读取数据
func (s *stm) Get(keys ...string) string {
	//先去 wset 读取数据
	if wv := s.wset.get(keys...); wv != nil {
		return wv.val
	}
	//没有的话
	return respToValue(s.fetch(keys...))
}

//stm 更新数据
func (s *stm) Put(key, val string, opts ...v3.OpOption) {
	//更新到 wset 中
	s.wset[key] = stmPut{val, v3.OpPut(key, val, opts...)}
}

func (s *stm) Del(key string) { s.wset[key] = stmPut{"", v3.OpDelete(key)} }

func (s *stm) Rev(key string) int64 {
	if resp := s.fetch(key); resp != nil && len(resp.Kvs) != 0 {
		return resp.Kvs[0].ModRevision
	}
	return 0
}

//stm的提交操作
func (s *stm) commit() *v3.TxnResponse {
	//如果没有发生冲突，就提交事务
	txnresp, err := s.client.Txn(s.ctx).If(s.conflicts()...).Then(s.wset.puts()...).Commit()
	if err != nil {
		panic(stmError{err})
	}
	if txnresp.Succeeded {
		return txnresp
	}
	return nil
}

func (s *stm) fetch(keys ...string) *v3.GetResponse {
	if len(keys) == 0 {
		return nil
	}
	ops := make([]v3.Op, len(keys))
	for i, key := range keys {
		//去 rset 中获取
		if resp, ok := s.rset[key]; ok {
			return resp
		}
		//还没有的话，组装 op
		ops[i] = v3.OpGet(key, s.getOpts...)
	}
	//从 etcd 中获取
	txnresp, err := s.client.Txn(s.ctx).Then(ops...).Commit()
	if err != nil {
		panic(stmError{err})
	}
	//再更新到 rset 中
	s.rset.add(keys, txnresp)
	return (*v3.GetResponse)(txnresp.Responses[0].GetResponseRange())
}

func (s *stm) reset() {
	s.rset = make(map[string]*v3.GetResponse)
	s.wset = make(map[string]stmPut)
}

//stmSerializable 包含 stm，再多一个预取 key的map prefetch
type stmSerializable struct {
	stm
	prefetch map[string]*v3.GetResponse
}

//stmSerializable 读取数据
func (s *stmSerializable) Get(keys ...string) string {
	//先去 wset 读取
	if wv := s.wset.get(keys...); wv != nil {
		return wv.val
	}
	firstRead := len(s.rset) == 0 //标记 rset 长度是否为0
	for _, key := range keys {
		//没有读取到，再去 prefetch 遍历读取
		//如果在 prefetch 中存在，就获取 resp，就删除该 key
		//再保存到 rset 中
		if resp, ok := s.prefetch[key]; ok {
			delete(s.prefetch, key)
			s.rset[key] = resp
		}
	}
	//再调用 stm 的查询函数，这个时候 rset 已经存在该 key 了
	resp := s.stm.fetch(keys...)
	if firstRead {
		//第一次读 key 时需要将 Revision 保存到 getOpts 中，后续读取的数据版本都要小于这个版本。
		s.getOpts = []v3.OpOption{
			v3.WithRev(resp.Header.Revision),
			v3.WithSerializable(),
		}
	}
	return respToValue(resp)
}

func (s *stmSerializable) Rev(key string) int64 {
	s.Get(key)
	return s.stm.Rev(key)
}

func (s *stmSerializable) gets() ([]string, []v3.Op) {
	keys := make([]string, 0, len(s.rset))
	ops := make([]v3.Op, 0, len(s.rset))
	for k := range s.rset {
		keys = append(keys, k)
		ops = append(ops, v3.OpGet(k))
	}
	return keys, ops
}

//stmSerializable 的提交操作
func (s *stmSerializable) commit() *v3.TxnResponse {
	keys, getops := s.gets()
	txn := s.client.Txn(s.ctx).If(s.conflicts()...).Then(s.wset.puts()...)
	//使用 Else 在发生冲突时，预取 key 以节省往返行程
	txnresp, err := txn.Else(getops...).Commit()
	if err != nil {
		panic(stmError{err})
	}
	if txnresp.Succeeded {
		return txnresp
	}
	//事务提交不成功，就将预取的 key 保存到 rset 与 prefetch 中
	s.rset.add(keys, txnresp)
	s.prefetch = s.rset
	s.getOpts = nil
	return nil
}

func isKeyCurrent(k string, r *v3.GetResponse) v3.Cmp {
	if len(r.Kvs) != 0 {
		return v3.Compare(v3.ModRevision(k), "=", r.Kvs[0].ModRevision)
	}
	return v3.Compare(v3.ModRevision(k), "=", 0)
}

func respToValue(resp *v3.GetResponse) string {
	if resp == nil || len(resp.Kvs) == 0 {
		return ""
	}
	return string(resp.Kvs[0].Value)
}

// NewSTMRepeatable is deprecated.
func NewSTMRepeatable(ctx context.Context, c *v3.Client, apply func(STM) error) (*v3.TxnResponse, error) {
	return NewSTM(c, apply, WithAbortContext(ctx), WithIsolation(RepeatableReads))
}

// NewSTMSerializable is deprecated.
func NewSTMSerializable(ctx context.Context, c *v3.Client, apply func(STM) error) (*v3.TxnResponse, error) {
	return NewSTM(c, apply, WithAbortContext(ctx), WithIsolation(Serializable))
}

// NewSTMReadCommitted is deprecated.
func NewSTMReadCommitted(ctx context.Context, c *v3.Client, apply func(STM) error) (*v3.TxnResponse, error) {
	return NewSTM(c, apply, WithAbortContext(ctx), WithIsolation(ReadCommitted))
}