// Copyright 2019 The etcd Authors
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

package tracker

// StateType is the state of a tracked follower.
type StateType uint64

const (
	//表示Leader节点一次不能向目标节点发送多条消息，只能待一条消息被响应之后，才能发送下一条消息。
	//当刚刚复制完快照数据、上次MsgApp消息被拒绝（或是发送失败）或是Leader节点初始化时，都会导致目标节点的Progress切换到该状态。
	StateProbe StateType = iota
	//表示正常的Entry记录复制状态，Leader节点向目标节点发送完消息之后，无须等待响应，即可开始后续消息的发送。
	StateReplicate
	//表示Leader节点正在向目标节点发送快照数据。
	StateSnapshot
)

var prstmap = [...]string{
	"StateProbe",
	"StateReplicate",
	"StateSnapshot",
}

func (st StateType) String() string { return prstmap[uint64(st)] }