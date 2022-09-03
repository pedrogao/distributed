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

const (
	// periodically poll shardctrler to learn about new shard config
	clientRefreshConfigInterval = 100
	// client be told to wait awhile and retry
	clientWaitAndRetryInterval = 50
)

type Clerk struct {
	sm      *shardctrler.Clerk // shard manager
	config  shardctrler.Config // latest known shard config
	makeEnd func(string) *labrpc.ClientEnd
	// You will have to modify this struct.
	me          int64       // my client id
	groupLeader map[int]int // for each group, remember which server turned out to be the leader for the last RPC
	opId        int         // operation id, increase monotonically
}

func MakeClerk(ctrlers []*labrpc.ClientEnd, makeEnd func(string) *labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.sm = shardctrler.MakeClerk(ctrlers)
	ck.makeEnd = makeEnd
	// You'll have to add code here.
	ck.me = nrand()
	ck.groupLeader = make(map[int]int)
	ck.opId = 1
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
// You will have to modify this function.
//
func (ck *Clerk) Get(key string) string {
	args := &GetArgs{
		Key:      key,
		ClientId: ck.me,
		OpId:     ck.opId,
	}
	ck.opId++
	shard := key2shard(key)

	for {
		args.ConfigNum = ck.config.Num
		sleepInterval := clientRefreshConfigInterval

		gid := ck.config.Shards[shard]
		servers := ck.config.Groups[gid]
		serverId := ck.groupLeader[gid]
		for i, nServer := 0, len(servers); i < nServer; {
			srv := ck.makeEnd(servers[serverId])
			reply := &GetReply{}
			ok := srv.Call("ShardKV.Get", args, reply)
			if !ok || reply.Err == ErrWrongLeader || reply.Err == ErrShutdown {
				serverId = (serverId + 1) % nServer
				i++
				continue
			}
			if reply.Err == ErrInitElection {
				time.Sleep(clientWaitAndRetryInterval * time.Millisecond)
				continue
			}

			ck.groupLeader[gid] = serverId
			if reply.Err == ErrUnknownConfig || reply.Err == ErrInMigration {
				time.Sleep(clientWaitAndRetryInterval * time.Millisecond)
				continue
			}
			if reply.Err == ErrWrongGroup {
				break
			}
			if reply.Err == ErrOutdatedConfig {
				sleepInterval = 0
				break
			}
			if reply.Err == ErrNoKey {
				return ""
			}
			if reply.Err == OK {
				return reply.Value
			}
		}

		time.Sleep(time.Duration(sleepInterval) * time.Millisecond)
		// ask controller for the latest configuration.
		config := ck.sm.Query(-1)
		if config.Num < 0 {
			return ""
		}
		ck.config = config
	}
}

//
// shared by Put and Append.
// You will have to modify this function.
//
func (ck *Clerk) PutAppend(key string, value string, op opType) {
	args := &PutAppendArgs{
		Key:      key,
		Value:    value,
		Op:       op,
		ClientId: ck.me,
		OpId:     ck.opId,
	}
	ck.opId++
	shard := key2shard(key)

	for {
		args.ConfigNum = ck.config.Num
		sleepInterval := clientRefreshConfigInterval

		gid := ck.config.Shards[shard]
		servers := ck.config.Groups[gid]
		serverId := ck.groupLeader[gid]
		for i, nServer := 0, len(servers); i < nServer; {
			srv := ck.makeEnd(servers[serverId])
			reply := &PutAppendReply{}
			ok := srv.Call("ShardKV.PutAppend", args, reply)
			if !ok || reply.Err == ErrWrongLeader || reply.Err == ErrShutdown {
				serverId = (serverId + 1) % nServer
				i++
				continue
			}
			if reply.Err == ErrInitElection {
				time.Sleep(clientWaitAndRetryInterval * time.Millisecond)
				continue
			}

			ck.groupLeader[gid] = serverId
			if reply.Err == ErrUnknownConfig || reply.Err == ErrInMigration {
				time.Sleep(clientWaitAndRetryInterval * time.Millisecond)
				continue
			}
			if reply.Err == ErrWrongGroup {
				break
			}
			if reply.Err == ErrOutdatedConfig {
				sleepInterval = 0
				break
			}
			if reply.Err == OK {
				return
			}
		}

		time.Sleep(time.Duration(sleepInterval) * time.Millisecond)
		// ask controller for the latest configuration.
		config := ck.sm.Query(-1)
		if config.Num < 0 {
			return
		}
		ck.config = config
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, opPut)
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, opAppend)
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}
