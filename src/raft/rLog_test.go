package raft

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_rLog_entryAt(t *testing.T) {
	rlog := defaultRLog()
	rlog.append(
		LogEntry{
			Term:    0,
			Command: 0,
		},
		LogEntry{
			Term:    1,
			Command: 1,
		},
	)
	entry := rlog.entryAt(1)
	assert.Equal(t, entry.Term, 0)
	assert.Equal(t, entry.Command, 0)

	entry = rlog.entryAt(2)
	assert.Equal(t, entry.Term, 1)
	assert.Equal(t, entry.Command, 1)

	rlog.append(
		LogEntry{
			Term:    1,
			Command: 2,
		},
	)
	entry = rlog.entryAt(3)
	assert.Equal(t, entry.Term, 1)
	assert.Equal(t, entry.Command, 2)

	// 设置 last
	rlog.LastIncludedIndex = 9
	rlog.LastIncludedTerm = 0
	entry = rlog.entryAt(9 + 2)
	assert.Equal(t, entry.Term, 1)
	assert.Equal(t, entry.Command, 1)
}

func Test_rLog_last(t *testing.T) {
	rlog := defaultRLog()
	rlog.append(
		LogEntry{
			Term:    0,
			Command: 0,
		},
		LogEntry{
			Term:    1,
			Command: 1,
		},
	)
	assert.Equal(t, rlog.first(), 0)
	assert.Equal(t, rlog.firstTerm(), 0)
	assert.Equal(t, rlog.last(), 2)
	assert.Equal(t, rlog.lastTerm(), 1)
	assert.Equal(t, rlog.size(), 3)

	rlog.append(
		LogEntry{
			Term:    1,
			Command: 2,
		},
	)
	assert.Equal(t, rlog.first(), 0)
	assert.Equal(t, rlog.firstTerm(), 0)
	assert.Equal(t, rlog.last(), 3)
	assert.Equal(t, rlog.lastTerm(), 1)
	assert.Equal(t, rlog.size(), 4)

	// 设置 last
	// 0-9 => 10
	rlog.LastIncludedIndex = 9
	rlog.LastIncludedTerm = 0
	assert.Equal(t, rlog.first(), 9)
	assert.Equal(t, rlog.firstTerm(), 0)
	assert.Equal(t, rlog.last(), 12)
	assert.Equal(t, rlog.lastTerm(), 1)
	assert.Equal(t, rlog.size(), 13)
}

func Test_rLog_subEntries(t *testing.T) {
	rlog := defaultRLog()
	rlog.append(
		LogEntry{
			Term:    0,
			Command: 0,
		},
		LogEntry{
			Term:    1,
			Command: 1,
		},
	)
	rlog.subEntries(1, 3)
	assert.Equal(t, rlog.first(), 0)
	assert.Equal(t, rlog.entryAt(rlog.first()).Command, 0)
	assert.Equal(t, rlog.firstTerm(), 0)

	rlog = defaultRLog()
	rlog.append(
		LogEntry{
			Term:    0,
			Command: 0,
		},
		LogEntry{
			Term:    1,
			Command: 1,
		},
	)
	rlog.subEntries(0, 1)
	assert.Equal(t, rlog.first(), 0)
	assert.Equal(t, rlog.entryAt(rlog.first()).Command, nil)
	assert.Equal(t, rlog.firstTerm(), 0)

	rlog = defaultRLog()
	rlog.append(
		LogEntry{
			Term:    0,
			Command: 0,
		},
		LogEntry{
			Term:    1,
			Command: 1,
		},
	)
	rlog.subEntries(0, 3)
	assert.Equal(t, rlog.last(), 2)
}

func Test_rLog_getEntries(t *testing.T) {
	rlog := defaultRLog()
	rlog.append(
		LogEntry{
			Term:    0,
			Command: 0,
		},
		LogEntry{
			Term:    1,
			Command: 1,
		},
	)
	entries := rlog.getEntries(1)
	assert.Equal(t, len(entries), 2)
	assert.Equal(t, entries[0].Term, 0)
	assert.Equal(t, entries[1].Command, 1)

	rlog = defaultRLog()
	rlog.append(
		LogEntry{
			Term:    2,
			Command: 0,
		},
		LogEntry{
			Term:    3,
			Command: 1,
		},
	)
	rlog.subFrom(1)
	assert.Equal(t, rlog.firstTerm(), 2)

	rlog = defaultRLog()
	rlog.append(
		LogEntry{
			Term:    2,
			Command: 0,
		},
		LogEntry{
			Term:    3,
			Command: 1,
		},
	)
	rlog.subTo(1)
	assert.Equal(t, len(rlog.Entries), 1)
}
