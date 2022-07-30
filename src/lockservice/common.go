package lockservice

import (
	"github.com/pedrogao/common"
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
	return common.RandInt64()
}
