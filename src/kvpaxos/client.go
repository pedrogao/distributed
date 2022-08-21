package kvpaxos

import (
	"crypto/rand"
	"math/big"
	"net/rpc"
	"time"
)

type Clerk struct {
	servers []string
	// You will have to modify this struct.
	clientId int64
	seq      int
	leaderId int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []string) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.clientId = nrand()
	ck.seq = 0
	ck.leaderId = 0
	return ck
}

func call(srv string, rpcname string,
	args any, reply any) bool {
	c, errx := rpc.Dial("unix", srv)
	if errx != nil {
		DPrintf("rpc dial err: %s", errx)
		return false
	}
	defer c.Close()

	err := c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	DPrintf("%v", err)
	return false
}

// Get
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
func (ck *Clerk) Get(key string) string {
	// You will have to modify this function.
	// leader 都是试出来的
	args := &GetArgs{
		Key:      key,
		Seq:      ck.seq,
		ClientId: ck.clientId,
	}
	ck.seq += 1
	reply := &GetReply{}
	leaderId := ck.leaderId
	// 如果失败，就一直重试
	internal := time.Millisecond * 10
	for {
		ok := call(ck.servers[leaderId], "KVPaxos.Get", args, reply)
		if !ok || reply.Err == ErrWrongLeader || reply.Err == ErrShutdown {
			DPrintf("call Get fail from leader: %d, try again, args: %+v, reply: %+v ",
				leaderId, args, reply)
			leaderId = (leaderId + 1) % len(ck.servers)
			continue
		}
		// 还没有选出 leader，休眠一会儿
		if reply.Err == ErrNoLeader {
			time.Sleep(internal)
			continue
		}
		ck.leaderId = leaderId // update
		if reply.Err == ErrNoKey {
			DPrintf("call Get no key from leader: %d, args: %+v, reply: %+v ",
				leaderId, args, reply)
			return ""
		}
		if reply.Err == OK {
			DPrintf("call Get ok from leader: %d, args: %+v, reply: %+v ", leaderId, args, reply)
			return reply.Value
		}
		DPrintf("call Get Err from leader: %d, try again, args: %+v, reply: %+v ", leaderId, args, reply)
	}
}

// PutAppend
// shared by Put and Append.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	args := &PutAppendArgs{
		Key:      key,
		Value:    value,
		Op:       op,
		Seq:      ck.seq,
		ClientId: ck.clientId,
	}
	ck.seq += 1
	leaderId := ck.leaderId
	reply := &PutAppendReply{}
	// 如果失败，就一直重试
	internal := time.Millisecond * 10
	for {
		ok := call(ck.servers[leaderId], "KVPaxos.PutAppend", args, reply)
		if !ok || reply.Err == ErrWrongLeader || reply.Err == ErrShutdown {
			DPrintf("call PutAppend fail from leader: %d, try again, args: %+v, reply: %+v ",
				leaderId, args, reply)
			leaderId = (leaderId + 1) % len(ck.servers)
			continue
		}
		// 还没有选出 leader，休眠一会儿
		if reply.Err == ErrNoLeader {
			time.Sleep(internal)
			continue
		}
		ck.leaderId = leaderId // update
		if reply.Err == OK {
			DPrintf("call PutAppend ok from leader: %d, args: %+v, reply: %+v ",
				leaderId, args, reply)
			return
		}
		DPrintf("call PutAppend Err from leader: %d, try again, args: %+v, reply: %+v ",
			leaderId, args, reply)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}

func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
