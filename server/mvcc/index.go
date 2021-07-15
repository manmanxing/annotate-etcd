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

package mvcc

import (
	"sort"
	"sync"

	"github.com/google/btree"
	"go.uber.org/zap"
)

type index interface {
	Get(key []byte, atRev int64) (rev, created revision, ver int64, err error)
	Range(key, end []byte, atRev int64) ([][]byte, []revision)
	Revisions(key, end []byte, atRev int64, limit int) []revision
	CountRevisions(key, end []byte, atRev int64, limit int) int
	Put(key []byte, rev revision)
	Tombstone(key []byte, rev revision) error
	RangeSince(key, end []byte, rev int64) []revision
	Compact(rev int64) map[revision]struct{}
	Keep(rev int64) map[revision]struct{}
	Equal(b index) bool

	Insert(ki *keyIndex)
	KeyIndex(ki *keyIndex) *keyIndex
}

//btree 的相关操作
type treeIndex struct {
	sync.RWMutex
	tree *btree.BTree
	lg   *zap.Logger
}

func newTreeIndex(lg *zap.Logger) index {
	return &treeIndex{
		tree: btree.New(32),
		lg:   lg,
	}
}

//添加/修改
func (ti *treeIndex) Put(key []byte, rev revision) {
	//因为 btree 上存储key 是 keyIndex 结构，因此这里需要组装
	keyi := &keyIndex{key: key}

	ti.Lock()
	defer ti.Unlock()
	//去 btree 上查找
	item := ti.tree.Get(keyi)
	if item == nil {//如果查不到，说明是新增
		//更新 keyIndex 的 revision
		keyi.put(ti.lg, rev.main, rev.sub)
		//将给定的key添加到 btree 中。
		ti.tree.ReplaceOrInsert(keyi)
		return
	}
	okeyi := item.(*keyIndex)
	//如果查询到，则只需要更新 keyIndex 的 revision
	okeyi.put(ti.lg, rev.main, rev.sub)
}

//查询
func (ti *treeIndex) Get(key []byte, atRev int64) (modified, created revision, ver int64, err error) {
	keyi := &keyIndex{key: key}
	ti.RLock()
	defer ti.RUnlock()
	//去 btree 上查找
	if keyi = ti.keyIndex(keyi); keyi == nil {
		return revision{}, revision{}, 0, ErrRevisionNotFound
	}
	//能查询到，再计算该key的 createdRevision and version and Revision
	return keyi.get(ti.lg, atRev)
}

func (ti *treeIndex) KeyIndex(keyi *keyIndex) *keyIndex {
	ti.RLock()
	defer ti.RUnlock()
	return ti.keyIndex(keyi)
}

func (ti *treeIndex) keyIndex(keyi *keyIndex) *keyIndex {
	if item := ti.tree.Get(keyi); item != nil {
		return item.(*keyIndex)
	}
	return nil
}

func (ti *treeIndex) visit(key, end []byte, f func(ki *keyIndex) bool) {
	keyi, endi := &keyIndex{key: key}, &keyIndex{key: end}

	ti.RLock()
	defer ti.RUnlock()

	ti.tree.AscendGreaterOrEqual(keyi, func(item btree.Item) bool {
		if len(endi.key) > 0 && !item.Less(endi) {
			return false
		}
		if !f(item.(*keyIndex)) {
			return false
		}
		return true
	})
}

func (ti *treeIndex) Revisions(key, end []byte, atRev int64, limit int) (revs []revision) {
	if end == nil {
		rev, _, _, err := ti.Get(key, atRev)
		if err != nil {
			return nil
		}
		return []revision{rev}
	}
	ti.visit(key, end, func(ki *keyIndex) bool {
		if rev, _, _, err := ki.get(ti.lg, atRev); err == nil {
			revs = append(revs, rev)
			if len(revs) == limit {
				return false
			}
		}
		return true
	})
	return revs
}

func (ti *treeIndex) CountRevisions(key, end []byte, atRev int64, limit int) int {
	if end == nil {
		_, _, _, err := ti.Get(key, atRev)
		if err != nil {
			return 0
		}
		return 1
	}
	total := 0
	ti.visit(key, end, func(ki *keyIndex) bool {
		if _, _, _, err := ki.get(ti.lg, atRev); err == nil {
			total++
			if total == limit {
				return false
			}
		}
		return true
	})
	return total
}

func (ti *treeIndex) Range(key, end []byte, atRev int64) (keys [][]byte, revs []revision) {
	if end == nil {
		rev, _, _, err := ti.Get(key, atRev)
		if err != nil {
			return nil, nil
		}
		return [][]byte{key}, []revision{rev}
	}
	ti.visit(key, end, func(ki *keyIndex) bool {
		if rev, _, _, err := ki.get(ti.lg, atRev); err == nil {
			revs = append(revs, rev)
			keys = append(keys, ki.key)
		}
		return true
	})
	return keys, revs
}

func (ti *treeIndex) Tombstone(key []byte, rev revision) error {
	keyi := &keyIndex{key: key}

	ti.Lock()
	defer ti.Unlock()
	//会去内存的 brtree 里去查 keyIndex
	item := ti.tree.Get(keyi)//这里是一个 interface
	if item == nil {
		return ErrRevisionNotFound
	}

	ki := item.(*keyIndex)//类型转换
	return ki.tombstone(ti.lg, rev.main, rev.sub)
}

// RangeSince returns all revisions from key(including) to end(excluding)
// at or after the given rev. The returned slice is sorted in the order
// of revision.
func (ti *treeIndex) RangeSince(key, end []byte, rev int64) []revision {
	keyi := &keyIndex{key: key}

	ti.RLock()
	defer ti.RUnlock()

	if end == nil {
		item := ti.tree.Get(keyi)
		if item == nil {
			return nil
		}
		keyi = item.(*keyIndex)
		return keyi.since(ti.lg, rev)
	}

	endi := &keyIndex{key: end}
	var revs []revision
	ti.tree.AscendGreaterOrEqual(keyi, func(item btree.Item) bool {
		if len(endi.key) > 0 && !item.Less(endi) {
			return false
		}
		curKeyi := item.(*keyIndex)
		revs = append(revs, curKeyi.since(ti.lg, rev)...)
		return true
	})
	sort.Sort(revisions(revs))

	return revs
}

//btree 上进行 compact
func (ti *treeIndex) Compact(rev int64) map[revision]struct{} {
	//保存可用的key
	available := make(map[revision]struct{})
	ti.lg.Info("compact tree index", zap.Int64("revision", rev))
	ti.Lock()
	//这里对 tree 进行 clone，可以并发使用
	clone := ti.tree.Clone()
	ti.Unlock()

	//遍历是 tree
	clone.Ascend(func(item btree.Item) bool {
		keyi := item.(*keyIndex)
		//此处需要锁定以防止在 compact 进行时修改 keyIndex 或在删除前将 revision 添加为空
		ti.Lock()
		//遍历过滤出那些key需要保留，哪些需要删
		keyi.compact(ti.lg, rev, available)
		if keyi.isEmpty() {
			//如果是需要删除的就从btree中删除
			item := ti.tree.Delete(keyi)
			if item == nil {
				ti.lg.Panic("failed to delete during compaction")
			}
		}
		ti.Unlock()
		return true
	})
	return available
}

// Keep finds all revisions to be kept for a Compaction at the given rev.
func (ti *treeIndex) Keep(rev int64) map[revision]struct{} {
	available := make(map[revision]struct{})
	ti.RLock()
	defer ti.RUnlock()
	ti.tree.Ascend(func(i btree.Item) bool {
		keyi := i.(*keyIndex)
		keyi.keep(rev, available)
		return true
	})
	return available
}

func (ti *treeIndex) Equal(bi index) bool {
	b := bi.(*treeIndex)

	if ti.tree.Len() != b.tree.Len() {
		return false
	}

	equal := true

	ti.tree.Ascend(func(item btree.Item) bool {
		aki := item.(*keyIndex)
		bki := b.tree.Get(item).(*keyIndex)
		if !aki.equal(bki) {
			equal = false
			return false
		}
		return true
	})

	return equal
}

func (ti *treeIndex) Insert(ki *keyIndex) {
	ti.Lock()
	defer ti.Unlock()
	ti.tree.ReplaceOrInsert(ki)
}