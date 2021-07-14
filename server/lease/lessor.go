// Copyright 2015 The etcd Authors
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

package lease

import (
	"container/heap"
	"context"
	"encoding/binary"
	"errors"
	"math"
	"sort"
	"sync"
	"time"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/server/v3/etcdserver/cindex"
	"go.etcd.io/etcd/server/v3/lease/leasepb"
	"go.etcd.io/etcd/server/v3/mvcc/backend"
	"go.uber.org/zap"
)

// NoLease 是一个特殊的 LeaseID，表示没有租约。
const NoLease = LeaseID(0)

// MaxLeaseTTL 是最大租约 TTL 值
const MaxLeaseTTL = 9000000000

var (
	forever = time.Time{}

	leaseBucketName = []byte("lease")

	// 每秒撤销的最大租约数；可配置用于测试
	leaseRevokeRate = 1000

	// 每秒记录到共识日志的最大租用检查点数；可配置用于测试
	leaseCheckpointRate = 1000

	// 租用检查点的默认间隔
	defaultLeaseCheckpointInterval = 5 * time.Minute

	//批处理到单个一致性日志条目中的最大租约检查点数
	maxLeaseCheckpointBatchSize = 1000

	//检查是否吊销过期租约的默认间隔
	defaultExpiredleaseRetryInterval = 3 * time.Second

	ErrNotPrimary       = errors.New("not a primary lessor")
	ErrLeaseNotFound    = errors.New("lease not found")
	ErrLeaseExists      = errors.New("lease already exists")
	ErrLeaseTTLTooLarge = errors.New("too large lease TTL")
)

// TxnDelete 是一个只允许删除的 TxnWrite。在这里定义是为了避免与 mvcc 的循环依赖。
type TxnDelete interface {
	DeleteRange(key, end []byte) (n, rev int64)
	End()
}

// RangeDeleter is a TxnDelete constructor.
type RangeDeleter func() TxnDelete

// Checkpointer permits checkpointing of lease remaining TTLs to the consensus log. Defined here to
// avoid circular dependency with mvcc.
type Checkpointer func(ctx context.Context, lc *pb.LeaseCheckpointRequest)

type LeaseID int64

// Lessor owns leases. It can grant, revoke, renew and modify leases for lessee.
type Lessor interface {
	// SetRangeDeleter lets the lessor create TxnDeletes to the store.
	// Lessor deletes the items in the revoked or expired lease by creating
	// new TxnDeletes.
	SetRangeDeleter(rd RangeDeleter)

	SetCheckpointer(cp Checkpointer)

	// Grant 创建一个至少在 TTL 秒后到期的租约。
	Grant(id LeaseID, ttl int64) (*Lease, error)
	// Revoke revokes a lease with given ID. The item attached to the
	// given lease will be removed. If the ID does not exist, an error
	// will be returned.
	Revoke(id LeaseID) error

	// Checkpoint applies the remainingTTL of a lease. The remainingTTL is used in Promote to set
	// the expiry of leases to less than the full TTL when possible.
	Checkpoint(id LeaseID, remainingTTL int64) error

	// Attach 将给定的租赁项附加到具有给定 LeaseID 的租赁。如果租约不存在，将返回错误。
	Attach(id LeaseID, items []LeaseItem) error

	// GetLease returns LeaseID for given item.
	// If no lease found, NoLease value will be returned.
	GetLease(item LeaseItem) LeaseID

	// Detach 从具有给定 LeaseID 的租约中分离给定的租用项。如果租约不存在，将返回错误。
	Detach(id LeaseID, items []LeaseItem) error

	// Promote promotes the lessor to be the primary lessor. Primary lessor manages
	// the expiration and renew of leases.
	// Newly promoted lessor renew the TTL of all lease to extend + previous TTL.
	Promote(extend time.Duration)

	// Demote demotes the lessor from being the primary lessor.
	Demote()

	// Renew 使用给定的 ID 续订租约。它返回更新后的 TTL。如果 ID 不存在，将返回错误。
	Renew(id LeaseID) (int64, error)

	// Lookup gives the lease at a given lease id, if any
	Lookup(id LeaseID) *Lease

	// Leases lists all leases.
	Leases() []*Lease

	// ExpiredLeasesC returns a chan that is used to receive expired leases.
	ExpiredLeasesC() <-chan []*Lease

	// Recover recovers the lessor state from the given backend and RangeDeleter.
	Recover(b backend.Backend, rd RangeDeleter)

	// Stop stops the lessor for managing leases. The behavior of calling Stop multiple
	// times is undefined.
	Stop()
}

// lessor implements Lessor interface.
// TODO: use clockwork for testability.
type lessor struct {
	mu sync.RWMutex

	// demotec is set when the lessor is the primary.
	// demotec will be closed if the lessor is demoted.
	demotec chan struct{}

	leaseMap             map[LeaseID]*Lease
	leaseExpiredNotifier *LeaseExpiredNotifier
	leaseCheckpointHeap  LeaseQueue
	itemMap              map[LeaseItem]LeaseID

	//当租约到期时，出租人将通过 RangeDeleter 删除租用的范围（或键）。
	rd RangeDeleter

	// When a lease's deadline should be persisted to preserve the remaining TTL across leader
	// elections and restarts, the lessor will checkpoint the lease by the Checkpointer.
	cp Checkpointer

	// backend 保留租约。我们现在只保留租约 ID 和到期时间。可以通过迭代 kv 中的所有密钥来恢复租用的项目。
	b backend.Backend

	// minLeaseTTL 是可以授予租用的最小租用 TTL。任何对较短 TTL 的请求都会扩展到最小 TTL。
	minLeaseTTL int64
	//保存过期的租约
	expiredC chan []*Lease
	// stopC 是一个通道，它的关闭表明租约应该停止
	stopC chan struct{}
	// doneC 是一个通道，它的关闭表明租约应该停止
	doneC chan struct{}

	lg *zap.Logger

	//租约检查点之间的等待时间。
	checkpointInterval time.Duration
	//检查过期租约是否被撤销的时间间隔
	expiredLeaseRetryInterval time.Duration
	ci                        cindex.ConsistentIndexer
}

type LessorConfig struct {
	MinLeaseTTL                int64
	CheckpointInterval         time.Duration
	ExpiredLeasesRetryInterval time.Duration
}

//创建一个租约
func NewLessor(lg *zap.Logger, b backend.Backend, cfg LessorConfig, ci cindex.ConsistentIndexer) Lessor {
	return newLessor(lg, b, cfg, ci)
}

func newLessor(lg *zap.Logger, b backend.Backend, cfg LessorConfig, ci cindex.ConsistentIndexer) *lessor {
	//设置一些参数
	checkpointInterval := cfg.CheckpointInterval
	expiredLeaseRetryInterval := cfg.ExpiredLeasesRetryInterval
	if checkpointInterval == 0 {
		checkpointInterval = defaultLeaseCheckpointInterval
	}
	if expiredLeaseRetryInterval == 0 {
		expiredLeaseRetryInterval = defaultExpiredleaseRetryInterval
	}
	//实例化一个租约
	l := &lessor{
		leaseMap:                  make(map[LeaseID]*Lease),
		itemMap:                   make(map[LeaseItem]LeaseID),
		leaseExpiredNotifier:      newLeaseExpiredNotifier(),
		leaseCheckpointHeap:       make(LeaseQueue, 0),
		b:                         b,
		minLeaseTTL:               cfg.MinLeaseTTL,
		checkpointInterval:        checkpointInterval,
		expiredLeaseRetryInterval: expiredLeaseRetryInterval,
		// expiredC is a small buffered chan to avoid unnecessary blocking.
		expiredC: make(chan []*Lease, 16),
		stopC:    make(chan struct{}),
		doneC:    make(chan struct{}),
		lg:       lg,
		ci:       ci,
	}
	l.initAndRecover()
    //会调用 runLoop 开启一个异步 goroutine
	go l.runLoop()

	return l
}

// isPrimary indicates if this lessor is the primary lessor. The primary
// lessor manages lease expiration and renew.
//
// in etcd, raft leader is the primary. Thus there might be two primary
// leaders at the same time (raft allows concurrent leader but with different term)
// for at most a leader election timeout.
// The old primary leader cannot affect the correctness since its proposal has a
// smaller term and will not be committed.
//
// TODO: raft follower do not forward lease management proposals. There might be a
// very small window (within second normally which depends on go scheduling) that
// a raft follow is the primary between the raft leader demotion and lessor demotion.
// Usually this should not be a problem. Lease should not be that sensitive to timing.
func (le *lessor) isPrimary() bool {
	return le.demotec != nil
}

func (le *lessor) SetRangeDeleter(rd RangeDeleter) {
	le.mu.Lock()
	defer le.mu.Unlock()

	le.rd = rd
}

func (le *lessor) SetCheckpointer(cp Checkpointer) {
	le.mu.Lock()
	defer le.mu.Unlock()

	le.cp = cp
}

//服务端-申请租约的具体实现
func (le *lessor) Grant(id LeaseID, ttl int64) (*Lease, error) {
	//id 不能为 0
	if id == NoLease {
		return nil, ErrLeaseNotFound
	}
	//ttl 有限制
	if ttl > MaxLeaseTTL {
		return nil, ErrLeaseTTLTooLarge
	}

	// TODO: when lessor is under high load, it should give out lease
	// with longer TTL to reduce renew load.
	// 创建 Lease 结构体，每一个租约一个结构体
	l := &Lease{
		ID:      id,
		ttl:     ttl,
		itemSet: make(map[LeaseItem]struct{}),
		revokec: make(chan struct{}),
	}

	le.mu.Lock()
	defer le.mu.Unlock()
	//首先在 leaseMap 中查找是否己经有同一个 leasor 了，不能重复创建
	if _, ok := le.leaseMap[id]; ok {
		return nil, ErrLeaseExists
	}
	//ttl 太小，就默认为 minLeaseTTL
	//todo minLeaseTTL 是怎么设置的
	if l.ttl < le.minLeaseTTL {
		l.ttl = le.minLeaseTTL
	}

	//检查当前 etcd 是不是 leader 节点
	//etcd lease 只有 leader 是过期的，follower 永远不过期
	if le.isPrimary() {
		//更新租约到期
		l.refresh(0)
	} else {
		//永不到期
		l.forever()
	}

	//将租约id 与 租约保存到 map里
	le.leaseMap[id] = l
	//将租约信息持久化到 boltdb, 这里只包括 leaseid, ttl, remainingTTL。并不包括被关联的 key 信息
	l.persistTo(le.b, le.ci)

	//计数统计类相关
	leaseTotalTTLs.Observe(float64(l.ttl))
	leaseGranted.Inc()

	//如果是主节点
	if le.isPrimary() {
		item := &LeaseWithTime{id: l.ID, time: l.expiry}
		//加入到heap 或 heap更新
		le.leaseExpiredNotifier.RegisterOrUpdate(item)
		//leader 将会调用 scheduleCheckpointIfNeeded 更新 checkpointHeap,
		le.scheduleCheckpointIfNeeded(l)
	}

	return l, nil
}

//服务端-租约撤销的具体实现
func (le *lessor) Revoke(id LeaseID) error {
	le.mu.Lock()
	//根据租约ID去map里查询，没查询到就返回错误
	l := le.leaseMap[id]
	if l == nil {
		le.mu.Unlock()
		return ErrLeaseNotFound
	}
	defer close(l.revokec)
	// unlock before doing external work
	le.mu.Unlock()

	//没有设置范围删除函数(是一个带有事务的只允许删除的函数)，直接返回
	if le.rd == nil {
		return nil
	}
	//获取事务删除函数
	txn := le.rd()

	//排序key，因此所有成员之间的删除顺序相同，否则后端哈希将不同
	keys := l.Keys()
	sort.StringSlice(keys).Sort()
	for _, key := range keys {
		//然后遍历关联的 keys, 一一删除
		txn.DeleteRange([]byte(key), nil)
	}

	le.mu.Lock()
	defer le.mu.Unlock()
	//从 leaseMap 中删除
	delete(le.leaseMap, l.ID)
	//租约删除需要与 kv 删除在同一个后端事务中
	//最后从 boltdb 的 leaseBucket 中删除该 lessor，那么下次 expireExists 检查时会从小顶堆中删除 item（小顶堆里是保存的 LeaseWithTime 对象）
	le.b.BatchTx().UnsafeDelete(leaseBucketName, int64ToBytes(int64(l.ID)))
	// if len(keys) > 0, txn.End() will call ci.UnsafeSave function.
	if le.ci != nil && len(keys) == 0 {
		le.ci.UnsafeSave(le.b.BatchTx())
	}

	txn.End()

	leaseRevoked.Inc()
	return nil
}

func (le *lessor) Checkpoint(id LeaseID, remainingTTL int64) error {
	le.mu.Lock()
	defer le.mu.Unlock()

	if l, ok := le.leaseMap[id]; ok {
		// when checkpointing, we only update the remainingTTL, Promote is responsible for applying this to lease expiry
		l.remainingTTL = remainingTTL
		if le.isPrimary() {
			// schedule the next checkpoint as needed
			le.scheduleCheckpointIfNeeded(l)
		}
	}
	return nil
}

// Renew 续订现有租约。如果给定的租约不存在或已过期，将返回错误。
func (le *lessor) Renew(id LeaseID) (int64, error) {
	le.mu.RLock()
	if !le.isPrimary() {
		//当前节点如果不是 leader，返回错误
		le.mu.RUnlock()
		return -1, ErrNotPrimary
	}
	//demotec 在本节点是leader时会设置，在本节点不是 leader 时会关闭
	demotec := le.demotec

	//通过 租约id 从 leaseMap 中获取 Lease，如果没有返回错误
	l := le.leaseMap[id]
	if l == nil {
		le.mu.RUnlock()
		return -1, ErrLeaseNotFound
	}
	//如果设置了 checkpoint 回调，并且该 lease 有剩余 ttl, 那么设置 clearRemainingTTL 为 true
	clearRemainingTTL := le.cp != nil && l.remainingTTL > 0

	le.mu.RUnlock()
	//如果 expired 判断当前己经过期了，这时就不可以 renew 了，要等待删除操作完成后返回错误
	if l.expired() {
		select {
		//过期的租约可能正在等待撤销或通过法定人数撤销。
		//准确地说，更新请求必须等待删除完成。
		case <-l.revokec:
			return -1, ErrLeaseNotFound
		case <-demotec://如果leader变更，过期的租约可能会撤销失败。调用者将根据 ErrNotPrimary 错误重试。
			return -1, ErrNotPrimary
		case <-le.stopC: //如果租约被关闭
			return -1, ErrNotPrimary
		}
	}

	// Clear remaining TTL when we renew if it is set
	// By applying a RAFT entry only when the remainingTTL is already set, we limit the number
	// of RAFT entries written per lease to a max of 2 per checkpoint interval.
	//操作 clearRemainingTTL 逻辑
	if clearRemainingTTL {
		le.cp(context.Background(), &pb.LeaseCheckpointRequest{Checkpoints: []*pb.LeaseCheckpoint{{ID: int64(l.ID), Remaining_TTL: 0}}})
	}

	le.mu.Lock()
	l.refresh(0)
	//重新生成 LeaseWithTime, 并更新 leaseExpiredNotifier 小顶堆
	item := &LeaseWithTime{id: l.ID, time: l.expiry}
	le.leaseExpiredNotifier.RegisterOrUpdate(item)
	le.mu.Unlock()

	//统计数据
	leaseRenewed.Inc()
	return l.ttl, nil
}

func (le *lessor) Lookup(id LeaseID) *Lease {
	le.mu.RLock()
	defer le.mu.RUnlock()
	return le.leaseMap[id]
}

func (le *lessor) unsafeLeases() []*Lease {
	leases := make([]*Lease, 0, len(le.leaseMap))
	for _, l := range le.leaseMap {
		leases = append(leases, l)
	}
	return leases
}

func (le *lessor) Leases() []*Lease {
	le.mu.RLock()
	ls := le.unsafeLeases()
	le.mu.RUnlock()
	sort.Sort(leasesByExpiry(ls))
	return ls
}

func (le *lessor) Promote(extend time.Duration) {
	le.mu.Lock()
	defer le.mu.Unlock()

	le.demotec = make(chan struct{})

	// refresh the expiries of all leases.
	for _, l := range le.leaseMap {
		l.refresh(extend)
		item := &LeaseWithTime{id: l.ID, time: l.expiry}
		le.leaseExpiredNotifier.RegisterOrUpdate(item)
	}

	if len(le.leaseMap) < leaseRevokeRate {
		// no possibility of lease pile-up
		return
	}

	// adjust expiries in case of overlap
	leases := le.unsafeLeases()
	sort.Sort(leasesByExpiry(leases))

	baseWindow := leases[0].Remaining()
	nextWindow := baseWindow + time.Second
	expires := 0
	// have fewer expires than the total revoke rate so piled up leases
	// don't consume the entire revoke limit
	targetExpiresPerSecond := (3 * leaseRevokeRate) / 4
	for _, l := range leases {
		remaining := l.Remaining()
		if remaining > nextWindow {
			baseWindow = remaining
			nextWindow = baseWindow + time.Second
			expires = 1
			continue
		}
		expires++
		if expires <= targetExpiresPerSecond {
			continue
		}
		rateDelay := float64(time.Second) * (float64(expires) / float64(targetExpiresPerSecond))
		// If leases are extended by n seconds, leases n seconds ahead of the
		// base window should be extended by only one second.
		rateDelay -= float64(remaining - baseWindow)
		delay := time.Duration(rateDelay)
		nextWindow = baseWindow + delay
		l.refresh(delay + extend)
		item := &LeaseWithTime{id: l.ID, time: l.expiry}
		le.leaseExpiredNotifier.RegisterOrUpdate(item)
		le.scheduleCheckpointIfNeeded(l)
	}
}

type leasesByExpiry []*Lease

func (le leasesByExpiry) Len() int           { return len(le) }
func (le leasesByExpiry) Less(i, j int) bool { return le[i].Remaining() < le[j].Remaining() }
func (le leasesByExpiry) Swap(i, j int)      { le[i], le[j] = le[j], le[i] }

func (le *lessor) Demote() {
	le.mu.Lock()
	defer le.mu.Unlock()

	// set the expiries of all leases to forever
	for _, l := range le.leaseMap {
		l.forever()
	}

	le.clearScheduledLeasesCheckpoints()
	le.clearLeaseExpiredNotifier()

	if le.demotec != nil {
		close(le.demotec)
		le.demotec = nil
	}
}

// Attach attaches items to the lease with given ID. When the lease
// expires, the attached items will be automatically removed.
// If the given lease does not exist, an error will be returned.
func (le *lessor) Attach(id LeaseID, items []LeaseItem) error {
	le.mu.Lock()
	defer le.mu.Unlock()

	l := le.leaseMap[id]
	if l == nil {
		return ErrLeaseNotFound
	}

	l.mu.Lock()
	for _, it := range items {
		l.itemSet[it] = struct{}{}
		le.itemMap[it] = id
	}
	l.mu.Unlock()
	return nil
}

func (le *lessor) GetLease(item LeaseItem) LeaseID {
	le.mu.RLock()
	id := le.itemMap[item]
	le.mu.RUnlock()
	return id
}

//实际上就是在map里删除租约
func (le *lessor) Detach(id LeaseID, items []LeaseItem) error {
	le.mu.Lock()
	defer le.mu.Unlock()

	l := le.leaseMap[id]
	if l == nil {
		return ErrLeaseNotFound
	}

	l.mu.Lock()
	for _, it := range items {
		delete(l.itemSet, it)
		delete(le.itemMap, it)
	}
	l.mu.Unlock()
	return nil
}

func (le *lessor) Recover(b backend.Backend, rd RangeDeleter) {
	le.mu.Lock()
	defer le.mu.Unlock()

	le.b = b
	le.rd = rd
	le.leaseMap = make(map[LeaseID]*Lease)
	le.itemMap = make(map[LeaseItem]LeaseID)
	le.initAndRecover()
}

func (le *lessor) ExpiredLeasesC() <-chan []*Lease {
	return le.expiredC
}

func (le *lessor) Stop() {
	close(le.stopC)
	<-le.doneC
}

//每隔 500ms, 调用一次 revokeExpiredLeases 函数回收过期 lease
func (le *lessor) runLoop() {
	defer close(le.doneC)

	for {
		le.revokeExpiredLeases()
		le.checkpointScheduledLeases()

		select {
		case <-time.After(500 * time.Millisecond):
		case <-le.stopC:
			return
		}
	}
}

// revokeExpiredLeases 找到所有过期的租约，并将它们发送到过期通道以进行撤销
func (le *lessor) revokeExpiredLeases() {
	var ls []*Lease

	//撤销租约速率限制：500
	revokeLimit := leaseRevokeRate / 2

	le.mu.RLock()
	if le.isPrimary() {
		//只有当前节点是 isPrimary 时，才调用 findExpiredLeases 获取过期的 leases
		ls = le.findExpiredLeases(revokeLimit)
	}
	le.mu.RUnlock()

	if len(ls) != 0 {
		select {
		case <-le.stopC:
			return
		case le.expiredC <- ls://获取过期的 leases, 然后扔到 expiredC channel 里，供上层使用
		default:
			// the receiver of expiredC is probably busy handling
			// other stuff
			// let's try this next time after 500ms
		}
	}
}

// checkpointScheduledLeases 找到所有到期的预定租约检查点并将它们提交给检查点以将它们保存到共识日志中。
func (le *lessor) checkpointScheduledLeases() {
	var cps []*pb.LeaseCheckpoint

	// rate limit：500
	for i := 0; i < leaseCheckpointRate/2; i++ {
		le.mu.Lock()
		if le.isPrimary() {
			//只有 leader 才会进行查询操作
			cps = le.findDueScheduledCheckpoints(maxLeaseCheckpointBatchSize)
		}
		le.mu.Unlock()

		if len(cps) != 0 {
			le.cp(context.Background(), &pb.LeaseCheckpointRequest{Checkpoints: cps})
		}
		if len(cps) < maxLeaseCheckpointBatchSize {
			return
		}
	}
}

func (le *lessor) clearScheduledLeasesCheckpoints() {
	le.leaseCheckpointHeap = make(LeaseQueue, 0)
}

func (le *lessor) clearLeaseExpiredNotifier() {
	le.leaseExpiredNotifier = newLeaseExpiredNotifier()
}

// expireExists returns true if expiry items exist.
// It pops only when expiry item exists.
// "next" is true, to indicate that it may exist in next attempt.
func (le *lessor) expireExists() (l *Lease, ok bool, next bool) {
	if le.leaseExpiredNotifier.Len() == 0 {
		return nil, false, false
	}

	//从堆里 Poll 小顶堆最小的数据，然后查看是否超时了
	//如果超时并没有立刻从堆中删除，需要设置了一个 expiredLeaseRetryInterval 的超时时间
	//设置的意义是配合 leaseMap 使用的，可以确保删除 lessor
	item := le.leaseExpiredNotifier.Poll()
	l = le.leaseMap[item.id]
	if l == nil {
		// 如果 lease 为nil，说明 1.lease 早已过期或被撤销 2.无需撤销
		le.leaseExpiredNotifier.Unregister() // O(log N)
		return nil, false, true
	}
	now := time.Now()
	if now.Before(item.time) /* item.time: expiration time */ {
		// Candidate expirations are caught up, reinsert this item
		// and no need to revoke (nothing is expiry)
		return l, false, false
	}

	//重试间隔后重新检查 revoke 是否完成
	item.time = now.Add(le.expiredLeaseRetryInterval)
	le.leaseExpiredNotifier.RegisterOrUpdate(item)
	return l, true, false
}

// findExpiredLeases 在leaseMap 中循环租用直到达到到期限制，并返回需要撤销的到期租用
func (le *lessor) findExpiredLeases(limit int) []*Lease {
	leases := make([]*Lease, 0, 16)

	for {
		l, ok, next := le.expireExists()
		if !ok && !next {
			break
		}
		if !ok {
			continue
		}
		if next {
			continue
		}

		if l.expired() {
			leases = append(leases, l)

			//过期 lease 不能一直删除，要做限速，所以要判断 limit（为500）
			if len(leases) == limit {
				break
			}
		}
	}

	return leases
}

func (le *lessor) scheduleCheckpointIfNeeded(lease *Lease) {
	if le.cp == nil {
		return
	}

	if lease.RemainingTTL() > int64(le.checkpointInterval.Seconds()) {
		if le.lg != nil {
			le.lg.Debug("Scheduling lease checkpoint",
				zap.Int64("leaseID", int64(lease.ID)),
				zap.Duration("intervalSeconds", le.checkpointInterval),
			)
		}
		heap.Push(&le.leaseCheckpointHeap, &LeaseWithTime{
			id:   lease.ID,
			time: time.Now().Add(le.checkpointInterval),
		})
	}
}

func (le *lessor) findDueScheduledCheckpoints(checkpointLimit int) []*pb.LeaseCheckpoint {
	if le.cp == nil {
		return nil
	}

	now := time.Now()
	cps := []*pb.LeaseCheckpoint{}
	for le.leaseCheckpointHeap.Len() > 0 && len(cps) < checkpointLimit {
		lt := le.leaseCheckpointHeap[0]
		if lt.time.After(now) /* lt.time: next checkpoint time */ {
			return cps
		}
		heap.Pop(&le.leaseCheckpointHeap)
		var l *Lease
		var ok bool
		if l, ok = le.leaseMap[lt.id]; !ok {
			continue
		}
		if !now.Before(l.expiry) {
			continue
		}
		remainingTTL := int64(math.Ceil(l.expiry.Sub(now).Seconds()))
		if remainingTTL >= l.ttl {
			continue
		}
		if le.lg != nil {
			le.lg.Debug("Checkpointing lease",
				zap.Int64("leaseID", int64(lt.id)),
				zap.Int64("remainingTTL", remainingTTL),
			)
		}
		cps = append(cps, &pb.LeaseCheckpoint{ID: int64(lt.id), Remaining_TTL: remainingTTL})
	}
	return cps
}

//当 etcd 重启时会执行下面操作 一块是 lessor.initAndRecover, 一块是 etcdServer.NewServer
//从 boltdb 的 leaseBucketName bucket 中遍历所有数据，然后加载到map，并添加到 leaseExpiredNotifier 小顶堆中，与之关联的 keys 是一个空集合 itemSet
//itemSet 会在 etcdServer.NewServer 的初始化时加载
func (le *lessor) initAndRecover() {
	tx := le.b.BatchTx()
	tx.Lock()

	//创建 bucket
	tx.UnsafeCreateBucket(leaseBucketName)
	//db查询
	_, vs := tx.UnsafeRange(leaseBucketName, int64ToBytes(0), int64ToBytes(math.MaxInt64), 0)
	// TODO: copy vs and do decoding outside tx lock if lock contention becomes an issue.
	for i := range vs {
		var lpb leasepb.Lease
		err := lpb.Unmarshal(vs[i])
		if err != nil {
			tx.Unlock()
			panic("failed to unmarshal lease proto item")
		}
		//for 循环，组装租约map
		ID := LeaseID(lpb.ID)
		if lpb.TTL < le.minLeaseTTL {
			lpb.TTL = le.minLeaseTTL
		}
		le.leaseMap[ID] = &Lease{
			ID:  ID,
			ttl: lpb.TTL,
			// itemSet will be filled in when recover key-value pairs
			// set expiry to forever, refresh when promoted
			itemSet: make(map[LeaseItem]struct{}),
			expiry:  forever,
			revokec: make(chan struct{}),
		}
	}
	le.leaseExpiredNotifier.Init()
	heap.Init(&le.leaseCheckpointHeap)
	tx.Unlock()

	le.b.ForceCommit()
}

type Lease struct {
	ID           LeaseID
	ttl          int64 // time to live of the lease in seconds
	remainingTTL int64 // remaining time to live in seconds, if zero valued it is considered unset and the full ttl should be used
	// expiryMu protects concurrent accesses to expiry
	expiryMu sync.RWMutex
	// expiry is time when lease should expire. no expiration when expiry.IsZero() is true
	expiry time.Time

	// mu protects concurrent accesses to itemSet
	mu      sync.RWMutex
	itemSet map[LeaseItem]struct{}
	revokec chan struct{}
}

func (l *Lease) expired() bool {
	return l.Remaining() <= 0
}

func (l *Lease) persistTo(b backend.Backend, ci cindex.ConsistentIndexer) {
	key := int64ToBytes(int64(l.ID))

	lpb := leasepb.Lease{ID: int64(l.ID), TTL: l.ttl, RemainingTTL: l.remainingTTL}
	val, err := lpb.Marshal()
	if err != nil {
		panic("failed to marshal lease proto item")
	}

	b.BatchTx().Lock()
	b.BatchTx().UnsafePut(leaseBucketName, key, val)
	if ci != nil {
		ci.UnsafeSave(b.BatchTx())
	}
	b.BatchTx().Unlock()
}

// TTL returns the TTL of the Lease.
func (l *Lease) TTL() int64 {
	return l.ttl
}

// RemainingTTL 返回 租约 的最后一个检查点剩余的 TTL。
func (l *Lease) RemainingTTL() int64 {
	if l.remainingTTL > 0 {
		return l.remainingTTL
	}
	return l.ttl
}

// refresh 更新租约到期
func (l *Lease) refresh(extend time.Duration) {
	newExpiry := time.Now().Add(extend + time.Duration(l.RemainingTTL())*time.Second)
	l.expiryMu.Lock()
	defer l.expiryMu.Unlock()
	l.expiry = newExpiry
}

// forever sets the expiry of lease to be forever.
func (l *Lease) forever() {
	l.expiryMu.Lock()
	defer l.expiryMu.Unlock()
	l.expiry = forever
}

// Keys 返回所有附加到租约的钥匙。
func (l *Lease) Keys() []string {
	l.mu.RLock()
	keys := make([]string, 0, len(l.itemSet))
	for k := range l.itemSet {
		keys = append(keys, k.Key)
	}
	l.mu.RUnlock()
	return keys
}

// Remaining returns the remaining time of the lease.
func (l *Lease) Remaining() time.Duration {
	l.expiryMu.RLock()
	defer l.expiryMu.RUnlock()
	if l.expiry.IsZero() {
		return time.Duration(math.MaxInt64)
	}
	return time.Until(l.expiry)
}

type LeaseItem struct {
	Key string
}

func int64ToBytes(n int64) []byte {
	bytes := make([]byte, 8)
	binary.BigEndian.PutUint64(bytes, uint64(n))
	return bytes
}

// FakeLessor is a fake implementation of Lessor interface.
// Used for testing only.
type FakeLessor struct{}

func (fl *FakeLessor) SetRangeDeleter(dr RangeDeleter) {}

func (fl *FakeLessor) SetCheckpointer(cp Checkpointer) {}

func (fl *FakeLessor) Grant(id LeaseID, ttl int64) (*Lease, error) { return nil, nil }

func (fl *FakeLessor) Revoke(id LeaseID) error { return nil }

func (fl *FakeLessor) Checkpoint(id LeaseID, remainingTTL int64) error { return nil }

func (fl *FakeLessor) Attach(id LeaseID, items []LeaseItem) error { return nil }

func (fl *FakeLessor) GetLease(item LeaseItem) LeaseID            { return 0 }
func (fl *FakeLessor) Detach(id LeaseID, items []LeaseItem) error { return nil }

func (fl *FakeLessor) Promote(extend time.Duration) {}

func (fl *FakeLessor) Demote() {}

func (fl *FakeLessor) Renew(id LeaseID) (int64, error) { return 10, nil }

func (fl *FakeLessor) Lookup(id LeaseID) *Lease { return nil }

func (fl *FakeLessor) Leases() []*Lease { return nil }

func (fl *FakeLessor) ExpiredLeasesC() <-chan []*Lease { return nil }

func (fl *FakeLessor) Recover(b backend.Backend, rd RangeDeleter) {}

func (fl *FakeLessor) Stop() {}

type FakeTxnDelete struct {
	backend.BatchTx
}

func (ftd *FakeTxnDelete) DeleteRange(key, end []byte) (n, rev int64) { return 0, 0 }
func (ftd *FakeTxnDelete) End()                                       { ftd.Unlock() }