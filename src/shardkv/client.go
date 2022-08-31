package shardkv

//
// client code to talk to a sharded key/value service.
//
// the client first talks to the shardctrler to find out
// the assignment of shards (keys) to groups, and then
// talks to the group that holds the key's shard.
//

import (
	"crypto/rand"
	"math/big"
	"time"

	"pedrogao/distributed/labrpc"
	"pedrogao/distributed/shardctrler"
)

//
// which shard is a key in?
// please use this function,
// and please do not change it.
//
func key2shard(key string) int {
	shard := 0
	if len(key) > 0 {
		shard = int(key[0])
	}
	shard %= shardctrler.NShards
	return shard
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

type Clerk struct {
	sm      *shardctrler.Clerk
	config  shardctrler.Config
	makeEnd func(string) *labrpc.ClientEnd
	// You will have to modify this struct.
	clientId int64
	seq      int
}

// MakeClerk
// the tester calls MakeClerk.
//
// ctrlers[] is needed to call shardctrler.MakeClerk().
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs.
//
func MakeClerk(ctrlers []*labrpc.ClientEnd, makeEnd func(string) *labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.sm = shardctrler.MakeClerk(ctrlers)
	ck.makeEnd = makeEnd
	// You'll have to add code here.
	ck.clientId = nrand()
	ck.seq = 0
	return ck
}

// Get
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
// You will have to modify this function.
//
func (ck *Clerk) Get(key string) string {
	args := GetArgs{
		Key:      key,
		Seq:      ck.seq,
		ClientId: ck.clientId,
	}
	internal := time.Millisecond * 10
	for {
		shard := key2shard(key)        // 拿到分片id
		gid := ck.config.Shards[shard] // 拿到分组id
		if servers, ok := ck.config.Groups[gid]; ok {
			// try each server for the shard.
			for si := 0; si < len(servers); si++ {
				srv := ck.makeEnd(servers[si]) // client
				var reply GetReply
				ok := srv.Call("ShardKV.Get", &args, &reply)
				if ok && (reply.Err == OK || reply.Err == ErrNoKey) {
					return reply.Value
				}
				if ok && reply.Err == ErrWrongGroup {
					// 错误分组，那么 break 更换分组
					break
				}
				// ... not ok, or ErrWrongLeader
				if ok && reply.Err == ErrNoKey {
					// no key
					return ""
				}
				if ok && reply.Err == ErrNoLeader {
					// 还没有选出 leader，休眠一会儿
					time.Sleep(internal)
					continue
				}
				if ok && reply.Err == ErrShutdown {
					return ""
				}
			}
		}
		time.Sleep(100 * time.Millisecond)
		// ask controller for the latest configuration.
		ck.config = ck.sm.Query(-1)
	}

	return ""
}

// PutAppend
// shared by Put and Append.
// You will have to modify this function.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	args := PutAppendArgs{
		Key:      key,
		Value:    value,
		Op:       op,
		Seq:      ck.seq,
		ClientId: ck.clientId,
	}
	internal := time.Millisecond * 10
	for {
		shard := key2shard(key)
		gid := ck.config.Shards[shard]
		if servers, ok := ck.config.Groups[gid]; ok {
			for si := 0; si < len(servers); si++ {
				srv := ck.makeEnd(servers[si])
				var reply PutAppendReply
				ok := srv.Call("ShardKV.PutAppend", &args, &reply)
				if ok && reply.Err == OK {
					return
				}
				if ok && reply.Err == ErrWrongGroup {
					break
				}
				// ... not ok, or ErrWrongLeader
				if ok && reply.Err == ErrNoLeader {
					// 还没有选出 leader，休眠一会儿
					time.Sleep(internal)
					continue
				}
				if ok && reply.Err == ErrShutdown {
					return
				}
			}
		}
		time.Sleep(100 * time.Millisecond)
		// ask controller for the latest configuration.
		ck.config = ck.sm.Query(-1)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
