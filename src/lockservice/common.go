package lockservice

import (
	"time"
)

//
// RPC definitions for a simple lock service.
//
// You will need to modify this file.
//

const (
	RetryTimes = 5
)

type Error = string

const (
	ErrLockFailed       Error = "ErrLockFailed"
	ErrUnlockFailed     Error = "ErrUnlockFailed"
	ErrNoSuckLock       Error = "ErrNoSuckLock"
	ErrTryUpdatePrimary Error = "ErrTryUpdatePrimary"
	ErrBackup           Error = "ErrBackup"
)

// LockArgs
// Lock(LockName) returns OK=true if the lock is not held.
// If it is held, it returns OK=false immediately.
type LockArgs struct {
	// Go's net/rpc requires that these field
	// names start with upper case letters!
	LockName string // lock name
	Number   int64
}

type LockReply struct {
	OK  bool
	Err Error
}

// UnlockArgs
// Unlock(LockName) returns OK=true if the lock was held.
// It returns OK=false if the lock was not held.
//
type UnlockArgs struct {
	LockName string
	Number   int64
}

type UnlockReply struct {
	OK  bool
	Err Error
}

//
// Forward & Update 主从复制
//

// UpdateArgs 单个更新
type UpdateArgs struct {
	Lock   string
	Val    bool
	Number int64
}

type UpdateReply struct {
	Err Error
}

func nrand() int64 {
	return time.Now().UnixNano()
}

func sliceIndex(slice []int64, val int64) int {
	for i := len(slice) - 1; i >= 0; i-- {
		v := slice[i]
		if v == val {
			return i
		}
	}
	return -1
}

func versionIndex(vs []*version, seqno int64) int {
	if vs == nil {
		return -1
	}
	for i := len(vs) - 1; i >= 0; i-- {
		v := vs[i]
		if v.Seqno == seqno {
			return i
		}
	}
	return -1
}

// 之前的版本
func biggerVersionIndex(vs []*version, seqno int64) int {
	if vs == nil {
		return -1
	}
	for i := len(vs) - 1; i >= 0; i-- {
		v := vs[i]
		if v.Seqno < seqno {
			return i
		}
	}
	return -1
}
