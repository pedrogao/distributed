package kvraft

import (
	"crypto/rand"
	"math/big"
	"time"

	"pedrogao/distributed/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	leaderId int
	// clientId + commandId 实现线性一致性
	// 即重复set请求，也能保证 apply 一次
	commandId int64 // 命令id，从0递增
	clientId  int64 // 客户端id，随机
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.clientId = nrand() // clerk id
	ck.leaderId = 0       // servers 中的leaderId，写请求只能走 leader
	ck.commandId = 0      // 命令id，从 0 递增
	return ck
}

// Get
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
	// leader 都是试出来的
	args := &GetArgs{
		Key:       key,
		CommandId: ck.commandId,
		ClientId:  ck.clientId,
	}
	reply := &GetReply{}
	// 如果失败，就一直重试
	internal := time.Millisecond * 100
	for !ck.servers[ck.leaderId].Call("KVServer.Get", args, reply) ||
		reply.Err == ErrWrongLeader || reply.Err == ErrTimeout {
		DPrintf("call Get fail from leader: %d, try again, args: %v, reply: %v ", ck.leaderId, args, reply)
		ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
		time.Sleep(internal)
	}
	// 成功
	ck.commandId += 1
	if reply.Err != OK {
		DPrintf("call Get err from leader: %d, try again, args: %v, reply: %v ", ck.leaderId, args, reply)
		return ""
	}

	return reply.Value
}

// PutAppend
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// leader 都是试出来的
	args := &PutAppendArgs{
		Key:       key,
		Value:     value,
		Op:        op,
		CommandId: ck.commandId,
		ClientId:  ck.clientId,
	}
	reply := &PutAppendReply{}
	// 如果失败，就一直重试
	internal := time.Millisecond * 100
	for !ck.servers[ck.leaderId].Call("KVServer.PutAppend", args, reply) ||
		reply.Err == ErrWrongLeader || reply.Err == ErrTimeout {
		DPrintf("call PutAppend fail from leader: %d, try again, args: %v, reply: %v ", ck.leaderId, args, reply)
		ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
		time.Sleep(internal)
	}
	// 成功
	ck.commandId += 1
	if reply.Err != OK {
		DPrintf("call PutAppend err from leader: %d, try again, args: %v, reply: %v ", ck.leaderId, args, reply)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
