// Copyright 2017 The etcd Authors
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

package mvcc

import (
	"context"

	"go.etcd.io/etcd/api/v3/mvccpb"
	"go.etcd.io/etcd/pkg/v3/traceutil"
	"go.etcd.io/etcd/server/v3/lease"
	"go.etcd.io/etcd/server/v3/mvcc/backend"
	"go.uber.org/zap"
)

type storeTxnRead struct {
	s  *store
	tx backend.ReadTx

	firstRev int64 //第一个版本号
	rev      int64 //当前的版本号

	trace *traceutil.Trace
}

func (s *store) Read(trace *traceutil.Trace) TxnRead {
	s.mu.RLock()
	s.revMu.RLock()
	// backend holds b.readTx.RLock() only when creating the concurrentReadTx. After
	// ConcurrentReadTx is created, it will not block write transaction.
	tx := s.b.ConcurrentReadTx()
	tx.RLock() // RLock is no-op. concurrentReadTx does not need to be locked after it is created.
	firstRev, rev := s.compactMainRev, s.currentRev
	s.revMu.RUnlock()
	return newMetricsTxnRead(&storeTxnRead{s, tx, firstRev, rev, trace})
}

func (tr *storeTxnRead) FirstRev() int64 { return tr.firstRev }
func (tr *storeTxnRead) Rev() int64      { return tr.rev }

func (tr *storeTxnRead) Range(ctx context.Context, key, end []byte, ro RangeOptions) (r *RangeResult, err error) {
	return tr.rangeKeys(ctx, key, end, tr.Rev(), ro)
}

func (tr *storeTxnRead) End() {
	tr.tx.RUnlock() // RUnlock signals the end of concurrentReadTx.
	tr.s.mu.RUnlock()
}

type storeTxnWrite struct {
	storeTxnRead
	tx backend.BatchTx
	// beginRev is the revision where the txn begins; it will write to the next revision.
	beginRev int64
	changes  []mvccpb.KeyValue
}

func (s *store) Write(trace *traceutil.Trace) TxnWrite {
	s.mu.RLock()
	tx := s.b.BatchTx()
	tx.Lock()
	tw := &storeTxnWrite{
		storeTxnRead: storeTxnRead{s, tx, 0, 0, trace},
		tx:           tx,
		beginRev:     s.currentRev,
		changes:      make([]mvccpb.KeyValue, 0, 4),
	}
	return newMetricsTxnWrite(tw)
}

func (tw *storeTxnWrite) Rev() int64 { return tw.beginRev }

func (tw *storeTxnWrite) Range(ctx context.Context, key, end []byte, ro RangeOptions) (r *RangeResult, err error) {
	rev := tw.beginRev
	if len(tw.changes) > 0 {
		rev++
	}
	return tw.rangeKeys(ctx, key, end, rev, ro)
}

func (tw *storeTxnWrite) DeleteRange(key, end []byte) (int64, int64) {
	if n := tw.deleteRange(key, end); n != 0 || len(tw.changes) > 0 {
		return n, tw.beginRev + 1
	}
	return 0, tw.beginRev
}

func (tw *storeTxnWrite) Put(key, value []byte, lease lease.LeaseID) int64 {
	tw.put(key, value, lease)
	return tw.beginRev + 1
}

func (tw *storeTxnWrite) End() {
	// only update index if the txn modifies the mvcc state.
	if len(tw.changes) != 0 {
		tw.s.saveIndex(tw.tx)
		// hold revMu lock to prevent new read txns from opening until writeback.
		tw.s.revMu.Lock()
		tw.s.currentRev++
	}
	tw.tx.Unlock()
	if len(tw.changes) != 0 {
		tw.s.revMu.Unlock()
	}
	tw.s.mu.RUnlock()
}


//Range 操作是 Read 读事务，会开启一个事务 txn
//curRev：当前事务的当前版本号
//ro：查询参数选项，包含查询的条数，是否计数，以及查询的当前版本号
func (tr *storeTxnRead) rangeKeys(ctx context.Context, key, end []byte, curRev int64, ro RangeOptions) (*RangeResult, error) {
	rev := ro.Rev
	//这里会检查版本，如果查询的版本号超过事务当前版本号视为无效
	if rev > curRev {
		return &RangeResult{KVs: nil, Count: -1, Rev: curRev}, ErrFutureRev
	}
	//查询的版本号<=0，会取事务当前版本号
	if rev <= 0 {
		rev = curRev
	}
	//已经被压缩的不能查询
	if rev < tr.s.compactMainRev {
		return &RangeResult{KVs: nil, Count: -1, Rev: 0}, ErrCompacted
	}
	//如果需要计数
	if ro.Count {
		total := tr.s.kvindex.CountRevisions(key, end, rev, int(ro.Limit))
		tr.trace.Step("count revisions from in-memory index tree")
		//这里返回 KVS 为 nil
		return &RangeResult{KVs: nil, Count: total, Rev: curRev}, nil
	}
	//根据指定版本去 kvindex 即内存 btree 中查找所有符合 rev 版本从 key 到 end 的版本信息
	revpairs := tr.s.kvindex.Revisions(key, end, rev, int(ro.Limit))
	tr.trace.Step("range keys from in-memory index tree")
	if len(revpairs) == 0 {
		return &RangeResult{KVs: nil, Count: 0, Rev: curRev}, nil
	}

	limit := int(ro.Limit)
	if limit <= 0 || limit > len(revpairs) {
		limit = len(revpairs)
	}

	kvs := make([]mvccpb.KeyValue, limit)
	revBytes := newRevBytes()
	//循环从内存中查找到的版本信息，去磁盘查询真实数据
	for i, revpair := range revpairs[:len(kvs)] {
		//判断是否需要退出
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}
		//将rev 转为 Bytes
		revToBytes(revpair, revBytes)
		//UnsafeRange 去底层磁盘 boltdb 中获取真实 key/value
		_, vs := tr.tx.UnsafeRange(keyBucketName, revBytes, nil, 0)
		if len(vs) != 1 {
			tr.s.lg.Fatal(
				"range failed to find revision pair",
				zap.Int64("revision-main", revpair.main),
				zap.Int64("revision-sub", revpair.sub),
			)
		}
		if err := kvs[i].Unmarshal(vs[0]); err != nil {
			tr.s.lg.Fatal(
				"failed to unmarshal mvccpb.KeyValue",
				zap.Error(err),
			)
		}
	}
	tr.trace.Step("range keys from bolt db")
	return &RangeResult{KVs: kvs, Count: len(revpairs), Rev: curRev}, nil
}

//更新数据，并关联租约与key
func (tw *storeTxnWrite) put(key, value []byte, leaseID lease.LeaseID) {
	rev := tw.beginRev + 1
	c := rev
	oldLease := lease.NoLease //装载之前存在的key的租约ID，这里预先初始化为 NoLease

	//根据 key, rev 查找内存 kvindex, 看看是否有当前 key 的版本记录。
	//如果该key之前存在，则使用其先前创建的key，并获取其先前key的租约ID
	//created： 表示创建时的版本号，也就是 CreateRevision
	//ver : version
	_, created, ver, err := tw.s.kvindex.Get(key, rev)
	if err == nil {//如果没找到，会报错 ErrRevisionNotFound
		c = created.main
		oldLease = tw.s.le.GetLease(lease.LeaseItem{Key: string(key)})
	}
	tw.trace.Step("get key's previous created_revision and leaseID")
	ibytes := newRevBytes()
	idxRev := revision{main: rev, sub: int64(len(tw.changes))}
	//这里将 rev 转换为 byte
	revToBytes(idxRev, ibytes)

	ver = ver + 1
	//构建 mvccpb.KeyValue 时携带 lease id, 会持久化到 boltdb
	//系统重启时从 boltdb keyBucketName 获取所有 key/value 时顺便恢复所有的租约
	//这里还会确定这次操作的 ModRevision 与 CreateRevision，Version
	kv := mvccpb.KeyValue{
		Key:            key,
		Value:          value,
		CreateRevision: c,
		ModRevision:    rev,
		Version:        ver,
		Lease:          int64(leaseID),
	}
	//序列化
	d, err := kv.Marshal()
	if err != nil {
		tw.storeTxnRead.s.lg.Fatal(
			"failed to marshal mvccpb.KeyValue",
			zap.Error(err),
		)
	}

	tw.trace.Step("marshal mvccpb.KeyValue")

	//UnsafeSeqPut 操作写磁盘 boltdb, key 是 Revision, value 是 mvccpb.KeyValue 序列化后的数据
	tw.tx.UnsafeSeqPut(keyBucketName, ibytes, d)
	//更新内存中的 kvindex btree
	tw.s.kvindex.Put(key, idxRev)
	tw.changes = append(tw.changes, kv)
	tw.trace.Step("store kv pair into bolt db")

	//如果 oldLease 有效的话，那么要先 Detach 取消key 与 该 lease 的关联
	if oldLease != lease.NoLease {
		if tw.s.le == nil {
			panic("no lessor to detach lease")
		}
		//在租约的map里删除
		err = tw.s.le.Detach(oldLease, []lease.LeaseItem{{Key: string(key)}})
		if err != nil {
			tw.storeTxnRead.s.lg.Error(
				"failed to detach old lease from a key",
				zap.Error(err),
			)
		}
	}
	if leaseID != lease.NoLease {
		if tw.s.le == nil {
			panic("no lessor to attach lease")
		}
		//最后调用 Attach 关联 key 与新的 lease
		//添加到租约map里，但没有持久化到DB
		//todo 什么时候会持久化道DB
		err = tw.s.le.Attach(leaseID, []lease.LeaseItem{{Key: string(key)}})
		if err != nil {
			panic("unexpected error from lease Attach")
		}
	}
	tw.trace.Step("attach lease to kv pair")
}

func (tw *storeTxnWrite) deleteRange(key, end []byte) int64 {
	rrev := tw.beginRev
	if len(tw.changes) > 0 {
		rrev++
	}
	//根据key,end, rev 查找内存 kvindex, 看看是否有当前 key 的版本记录列表。
	keys, _ := tw.s.kvindex.Range(key, end, rrev)
	if len(keys) == 0 {
		return 0
	}
	//遍历当前 key 的版本记录列表进行删除
	for _, key := range keys {
		tw.delete(key)
	}
	return int64(len(keys))
}

func (tw *storeTxnWrite) delete(key []byte) {
	ibytes := newRevBytes()
	idxRev := revision{main: tw.beginRev + 1, sub: int64(len(tw.changes))}
	//将 rev 转换为 byte
	revToBytes(idxRev, ibytes)
	//添加删除标记到 revision 的字节数组中
	ibytes = appendMarkTombstone(tw.storeTxnRead.s.lg, ibytes)
	//并且生成一个只包含 key 的 mvccpb.KeyValue，并保存到磁盘中
	kv := mvccpb.KeyValue{Key: key}

	//先序列化
	d, err := kv.Marshal()
	if err != nil {
		tw.storeTxnRead.s.lg.Fatal(
			"failed to marshal mvccpb.KeyValue",
			zap.Error(err),
		)
	}
	//再保存到磁盘里，磁盘里也只是带一个删除标记的 revision，并不会释放磁盘空间
	//真正的删除是在compact、Defrag阶段处理
	tw.tx.UnsafeSeqPut(keyBucketName, ibytes, d)
	//更新 keyIndex 的 revision 为 带有删除标记的 revision，并不会释放内存空间
	//并且appned一个新的空 generation（目的就是结束当前的generation）
	err = tw.s.kvindex.Tombstone(key, idxRev)
	if err != nil {
		tw.storeTxnRead.s.lg.Fatal(
			"failed to tombstone an existing key",
			zap.String("key", string(key)),
			zap.Error(err),
		)
	}
	//保存变动的 mvccpb.KeyValue
	tw.changes = append(tw.changes, kv)

	//获取 key 的租约信息
	item := lease.LeaseItem{Key: string(key)}
	leaseID := tw.s.le.GetLease(item)

	//如果key带有租约信息，还需要将该租约与该key解绑
	if leaseID != lease.NoLease {
		err = tw.s.le.Detach(leaseID, []lease.LeaseItem{item})
		if err != nil {
			tw.storeTxnRead.s.lg.Error(
				"failed to detach old lease from a key",
				zap.Error(err),
			)
		}
	}
}

func (tw *storeTxnWrite) Changes() []mvccpb.KeyValue { return tw.changes }