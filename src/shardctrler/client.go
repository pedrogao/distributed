package shardctrler

//
// Shardctrler clerk.
//

import (
	"crypto/rand"
	"math/big"
	"time"

	"pedrogao/distributed/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// Your data here.
	clientId int64
	seq      int
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
	// Your code here.
	ck.clientId = nrand()
	ck.seq = 0
	return ck
}

// Query 根据版本查询配置
func (ck *Clerk) Query(num int) Config {
	args := &QueryArgs{}
	// Your code here.
	args.Num = num
	args.Seq = ck.seq
	args.ClientId = ck.clientId
	ck.seq++
	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply QueryReply
			ok := srv.Call("ShardCtrler.Query", args, &reply)
			if ok && reply.WrongLeader == false && reply.Err == OK {
				DPrintf("client: %d post Query ok args: %+v, reply: %+v ", ck.clientId, args, reply)
				return reply.Config
			}
			DPrintf("client: %d post Query err args: %+v, reply: %+v ", ck.clientId, args, reply)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// Join 加入一组副本
func (ck *Clerk) Join(servers map[int][]string) {
	args := &JoinArgs{
		Seq:      ck.seq,
		ClientId: ck.clientId,
	}
	// Your code here.
	args.Servers = servers
	ck.seq++
	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply JoinReply
			ok := srv.Call("ShardCtrler.Join", args, &reply)
			if ok && reply.WrongLeader == false && reply.Err == OK {
				DPrintf("client: %d post Join ok args: %+v, reply: %+v ", ck.clientId, args, reply)
				return
			}
			DPrintf("client: %d post Join err args: %+v, reply: %+v ", ck.clientId, args, reply)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// Leave 一组副本离开
func (ck *Clerk) Leave(gids []int) {
	args := &LeaveArgs{
		Seq:      ck.seq,
		ClientId: ck.clientId,
	}
	// Your code here.
	args.GIDs = gids
	ck.seq++
	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply LeaveReply
			ok := srv.Call("ShardCtrler.Leave", args, &reply)
			if ok && reply.WrongLeader == false && reply.Err == OK {
				DPrintf("client: %d post Leave ok args: %+v, reply: %+v ", ck.clientId, args, reply)
				return
			}
			DPrintf("client: %d post Leave err args: %+v, reply: %+v ", ck.clientId, args, reply)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// Move 副本移动
func (ck *Clerk) Move(shard int, gid int) {
	args := &MoveArgs{
		Seq:      ck.seq,
		ClientId: ck.clientId,
	}
	// Your code here.
	args.Shard = shard
	args.GID = gid
	ck.seq++
	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply MoveReply
			ok := srv.Call("ShardCtrler.Move", args, &reply)
			if ok && reply.WrongLeader == false && reply.Err == OK {
				DPrintf("client: %d post Move ok args: %+v, reply: %+v ", ck.clientId, args, reply)
				return
			}
			DPrintf("client: %d post Move err args: %+v, reply: %+v ", ck.clientId, args, reply)
		}
		time.Sleep(100 * time.Millisecond)
	}
}
