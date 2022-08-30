package kvraft

import (
	"time"
)

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
	ErrNoLeader    = "ErrNoLeader" // leader还没被选出来
	ErrTimeout     = "ErrTimeout"
	ErrShutdown    = "ErrShutdown"
)

// 默认超时时间
var defaultTimeout = time.Millisecond * 500

type Err string

// PutAppendArgs Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	CommandId int64 // 命令id，从0递增
	ClientId  int64 // 客户端id，随机
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	CommandId int64 // 命令id，从0递增
	ClientId  int64 // 客户端id，随机
}

type GetReply struct {
	Err   Err
	Value string
}
