package raft

import "fmt"

// LogEntry log item of FSM
type LogEntry struct {
	Term    int // 任期
	Command any // 命令
}

func (le *LogEntry) String() string {
	return fmt.Sprintf("term: %d, command: %v", le.Term, le.Command)
}

// rLog 日志类型
// why? 因为 Raft 需要日志来持久化状态，这其中涉及到日志获取，长度等操作
// 故将其封装为一个结构体，对外屏蔽细节
type rLog struct {
	Entries           []LogEntry
	LastIncludedIndex int // 快照中的最后一个日志序号
	LastIncludedTerm  int // 快照中的最后一个日志任期
}

func defaultRLog() rLog {
	return rLog{
		// 第0位当作哨兵处理，即 lastIncluded
		Entries: []LogEntry{{
			Term:    0,
			Command: nil,
		}},
		LastIncludedIndex: 0,
		LastIncludedTerm:  0,
	}
}

//
// 所有的 index 参数都是绝对序号
//

// entryAt 回去 index 对应的日志
func (l *rLog) entryAt(index int) LogEntry {
	// 9 3
	if index < l.LastIncludedIndex || index >= l.LastIncludedIndex+len(l.Entries) {
		panic(fmt.Sprintf("valid range: [%d, %d), but index is: %d ",
			l.LastIncludedIndex, l.LastIncludedIndex+len(l.Entries), index))
	}
	return l.Entries[index-l.LastIncludedIndex]
}

// append 追加日志
func (l *rLog) append(entrys ...LogEntry) {
	// data race: https://zhuanlan.zhihu.com/p/228166716
	// copy 必须是 len，而不是 cap
	tmp := make([]LogEntry, len(l.Entries), len(l.Entries)+len(entrys))
	copy(tmp, l.Entries)
	tmp = append(tmp, entrys...)
	l.Entries = tmp
}

// subEntries Entries = Entries[from, to)
func (l *rLog) subEntries(from, to int) {
	if from < l.LastIncludedIndex || from > l.LastIncludedIndex+len(l.Entries) {
		panic(fmt.Sprintf("valid range: [%d, %d), but from is: %d ",
			l.LastIncludedIndex, l.LastIncludedIndex+len(l.Entries), from))
	}
	if to < l.LastIncludedIndex || to > l.LastIncludedIndex+len(l.Entries) {
		panic(fmt.Sprintf("valid range: [%d, %d), but to is: %d ",
			l.LastIncludedIndex, l.LastIncludedIndex+len(l.Entries), to))
	}
	from -= l.LastIncludedIndex
	to -= l.LastIncludedIndex
	tmp := make([]LogEntry, len(l.Entries), len(l.Entries))
	copy(tmp, l.Entries)
	// copy on write, 避免 log data race
	l.Entries = tmp[from:to]
}

// subTo Entries = Entries[LastIncludedIndex, to)
func (l *rLog) subTo(to int) {
	l.subEntries(l.LastIncludedIndex, to)
}

// subFrom Entries = Entries[from, size)
func (l *rLog) subFrom(from int) {
	l.subEntries(from, l.size())
}

// getEntries
func (l *rLog) getEntries(from int) []LogEntry {
	if from < l.LastIncludedIndex || from > l.LastIncludedIndex+len(l.Entries) {
		panic(fmt.Sprintf("valid range: [%d, %d), but from is: %d ",
			l.LastIncludedIndex, l.LastIncludedIndex+len(l.Entries), from))
	}

	from -= l.LastIncludedIndex
	return l.Entries[from:]
}

// 最后序号
func (l *rLog) last() int {
	return len(l.Entries) + l.LastIncludedIndex - 1
}

// 最后任期
func (l *rLog) lastTerm() int {
	return l.Entries[len(l.Entries)-1].Term
}

// 第一个序号
func (l *rLog) first() int {
	return l.LastIncludedIndex
}

// 第一个任期
func (l *rLog) firstTerm() int {
	return l.entryAt(l.first()).Term
}

// 日志长度
func (l *rLog) size() int {
	return len(l.Entries) + l.LastIncludedIndex
}
