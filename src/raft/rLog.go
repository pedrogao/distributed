package raft

import "fmt"

type LogEntry struct {
	Term    int // 任期
	Command any // 命令
}

func (le LogEntry) String() string {
	return fmt.Sprintf("term: %d, command: %v", le.Term, le.Command)
}

type rLog struct {
	Entries []LogEntry
}

func defaultRLog() rLog {
	return rLog{
		// 注意：defaultRLog 函数返回一个默认的 rLog，根据 Raft 论文中的阐述，
		// 日志切片的第一个作为占位使用，因此在初始化时，我们推入了一个 Command 为 nil 的日志。
		Entries: []LogEntry{
			// 默认日志有一条空项
			{
				Term:    0,
				Command: nil,
			},
		},
	}
}

// subTo entries = entries[0, to)
func (l *rLog) subTo(to int) {
	l.subEntries(0, to)
}

// subFrom entries = entries[from, size)
func (l *rLog) subFrom(from int) {
	l.subEntries(from, l.size())
}

// subEntries entries = entries[from, to)
func (l *rLog) subEntries(from, to int) {
	tmp := make([]LogEntry, l.size())
	copy(tmp, l.Entries)
	// copy on write, 避免 log data race
	l.Entries = tmp[from:to]
}

// getEntries
func (l *rLog) getEntries(from int) []LogEntry {
	return l.Entries[from:]
}

func (l *rLog) entryAt(index int) LogEntry {
	return l.Entries[index-l.first()]
}

func (l *rLog) append(entry ...LogEntry) {
	l.Entries = append(l.Entries, entry...)
}

func (l *rLog) last() int {
	if len(l.Entries) == 0 {
		return 0
	}
	return len(l.Entries) - 1
}

func (l *rLog) lastTerm() int {
	return l.Entries[l.last()].Term
}

func (l *rLog) first() int {
	return 0
}

func (l *rLog) firstTerm() int {
	return l.Entries[l.first()].Term
}

func (l *rLog) size() int {
	return len(l.Entries)
}
