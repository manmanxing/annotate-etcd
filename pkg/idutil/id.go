package idutil

import (
	"math"
	"sync/atomic"
	"time"
)

const (
	tsLen     = 5 * 8
	cntLen    = 8
	suffixLen = tsLen + cntLen
)


/**
包 idutil 实现了用于生成唯一的随机 ID 的实用函数。

Generator 根据计数器、时间戳和节点成员 ID 生成唯一标识符。

初始 id 格式如下：
高位 2 个字节来自 memberID，接下来 5 个字节来自时间戳，低位 1 个字节是计数器。
|前缀     |后缀              |
| 2 字节 | 5 个字节 | 1 字节  |
|会员ID |时间戳     | cnt    |

当机器在 1 ms 之后和 35 年之前重新启动时，时间戳 5 个字节是不同的。

它增加后缀以生成下一个 id。

计数字段可能会溢出到时间戳字段，这是故意的。
它有助于将事件窗口扩展到 2^56。这不会破坏重启后生成的 id 是唯一的，因为 etcd 吞吐量是 << 256reqms(250k reqssecond)。
 */


type Generator struct {
	// high order 2 bytes
	prefix uint64
	// low order 6 bytes
	suffix uint64
}

func NewGenerator(memberID uint16, now time.Time) *Generator {
	prefix := uint64(memberID) << suffixLen
	unixMilli := uint64(now.UnixNano()) / uint64(time.Millisecond/time.Nanosecond)
	suffix := lowbit(unixMilli, tsLen) << cntLen
	return &Generator{
		prefix: prefix,
		suffix: suffix,
	}
}

// Next 生成唯一的 id。
func (g *Generator) Next() uint64 {
	suffix := atomic.AddUint64(&g.suffix, 1)
	id := g.prefix | lowbit(suffix, suffixLen)
	return id
}

func lowbit(x uint64, n uint) uint64 {
	return x & (math.MaxUint64 >> (64 - n))
}