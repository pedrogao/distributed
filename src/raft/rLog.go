package raft

import "fmt"

type LogEntry struct {
	Term    int // 任期
	Command any // 命令
}

func (le LogEntry) String() string {
	return fmt.Sprintf("term: %d, command: %v", le.Term, le.Command)
}

// rLog 日志类型
// why? 因为 Raft 需要日志来持久化状态，这其中涉及到日志获取，长度等操作
// 故将其封装为一个结构体，对外屏蔽细节
type rLog struct {
	Entries           []LogEntry
	LastIncludedIndex int
	LastIncludedTerm  int
}

func defaultRLog() rLog {
	return rLog{
		Entries: []LogEntry{
			{
				Term:    0,
				Command: nil,
			},
		},
		LastIncludedIndex: 0,
		LastIncludedTerm:  0,
	}
}

// entryAt 回去 index 对应的日志
func (l *rLog) entryAt(index int) LogEntry {
	if index < l.LastIncludedIndex || index > l.LastIncludedIndex+len(l.Entries) {
		panic(fmt.Sprintf("lastIncludeIndex: %d, but index: %d is invalid", l.LastIncludedIndex, index))
	}
	return l.Entries[index-l.LastIncludedIndex]
}

// append 追加日志
func (l *rLog) append(entry ...LogEntry) {
	l.Entries = append(l.Entries, entry...)
}

// subEntries entries = entries[from, to)
func (l *rLog) subEntries(from, to int) {
	tmp := make([]LogEntry, l.size())
	copy(tmp, l.Entries)
	// copy on write, 避免 log data race
	l.Entries = tmp[from:to]
}

// subTo entries = entries[0, to)
func (l *rLog) subTo(to int) {
	l.subEntries(0, to)
}

// subFrom entries = entries[from, size)
func (l *rLog) subFrom(from int) {
	l.subEntries(from, l.size())
}

// getEntries
func (l *rLog) getEntries(from int) []LogEntry {
	return l.Entries[from:]
}

// 最后序号
func (l *rLog) last() int {
	if len(l.Entries) == 0 && l.LastIncludedIndex == 0 {
		return 0
	}
	return len(l.Entries) + l.LastIncludedIndex - 1
}

// 最后任期
func (l *rLog) lastTerm() int {
	return l.Entries[l.last()-l.LastIncludedIndex].Term
}

// 第一个序号
func (l *rLog) first() int {
	return l.LastIncludedIndex
}

// 第一个任期
func (l *rLog) firstTerm() int {
	return l.Entries[0].Term
}

// 日志长度
func (l *rLog) size() int {
	return len(l.Entries) + l.LastIncludedIndex
}
