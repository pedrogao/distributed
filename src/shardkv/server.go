package shardkv

import (
	"bytes"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pedrogao/log"

	"pedrogao/distributed/labgob"
	"pedrogao/distributed/labrpc"
	"pedrogao/distributed/raft"
)

var (
	// Debug Debugging
	Debug  = false
	logger = log.New(log.WithSkipLevel(3))
)

func init() {
	if os.Getenv("debug") != "" || Debug {
		logger.SetOptions(log.WithLevel(log.DebugLevel))
	} else {
		logger.SetOptions(log.WithLevel(log.ErrorLevel))
	}
}

func DPrintf(format string, a ...any) {
	logger.Debugf(format, a...)
}

type Action = string

const (
	GetAction    Action = "Get"
	PutAction    Action = "Put"
	AppendAction Action = "Append"
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Action string
	Key    string
	Value  string

	Seq      int
	ClientId int64
}

type clientRecord struct {
	Seq   int
	Index int
	Resp  *clientResp
}

type clientResp struct {
	Value string
	Err   Err
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	makeEnd      func(string) *labrpc.ClientEnd
	gid          int                 // group id
	ctrlers      []*labrpc.ClientEnd // config controllers
	maxraftstate int                 // snapshot if log grows this big

	// Your definitions here.
	dead        int32
	store       map[string]string       // kv存储
	recordMap   map[int64]*clientRecord // 操作记录
	notifyMap   map[int]chan *clientResp
	lastApplied int64 // 最后已应用 command
	persister   *raft.Persister
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	if kv.killed() {
		reply.Err = ErrShutdown
		return
	}

	// shard := key2shard(args.Key)
	// gid := kv.config.Shards[shard]

	// submit op
	op := Op{
		Action:   GetAction,
		Key:      args.Key,
		Seq:      args.Seq,
		ClientId: args.ClientId,
	}
	index, term, isLeader := kv.rf.Start(op)
	if term == 0 { // term = 0 的时候，leader 还没选出来
		DPrintf("%d Get no leader, args: %+v, reply: %+v ", kv.Me(), args, reply)
		reply.Err = ErrNoLeader
		return
	}
	if !isLeader {
		DPrintf("%d Get wrong leader, args: %+v, reply: %+v ", kv.Me(), args, reply)
		reply.Err = ErrWrongLeader
		return
	}

	// get 请求是无需判断重复的，因为天然就无需 apply 多次，本身就具有幂等性
	notify := make(chan *clientResp)
	kv.guard(func() {
		kv.notifyMap[index] = notify
	})

	select {
	case resp := <-notify:
		// 成功同步
		reply.Value = resp.Value
		reply.Err = resp.Err
		DPrintf("%d Get Op ok, op: %+v ", kv.Me(), op)
	case <-time.After(defaultTimeout):
		// 超时
		reply.Err = ErrTimeout
		DPrintf("%d Get Op timeout, op: %+v ", kv.Me(), op)
	}
	// 删除无用的 notify
	kv.guard(func() {
		delete(kv.notifyMap, index)
	})
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	if kv.killed() {
		reply.Err = ErrShutdown
		return
	}

	kv.mu.Lock()
	if kv.isLatestRequest(args.ClientId, args.Seq) {
		// 如果是重复提交
		record := kv.recordMap[args.ClientId]
		reply.Err = record.Resp.Err // 使用上次的 Resp
		DPrintf("%d PutAppend duplicate, args: %+v, reply: %+v ", kv.Me(), args, reply)
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	argOp := args.Op
	op := Op{
		Action:   argOp,
		Key:      args.Key,
		Value:    args.Value,
		Seq:      args.Seq,
		ClientId: args.ClientId,
	}
	index, term, isLeader := kv.rf.Start(op)
	if term == 0 {
		DPrintf("%d PutAppend no leader, args: %+v, reply: %+v ", kv.Me(), args, reply)
		reply.Err = ErrNoLeader
		return
	}
	if !isLeader {
		DPrintf("%d PutAppend wrong leader, args: %+v, reply: %+v ", kv.Me(), args, reply)
		reply.Err = ErrWrongLeader
		return
	}
	DPrintf("%d PutAppend Op committed, op: %+v, index: %d", kv.Me(), op, index)

	// 只有 leader 需要 notify 客户端
	notify := make(chan *clientResp)

	kv.guard(func() {
		kv.notifyMap[index] = notify
	})

	select {
	case resp := <-notify: // 阻塞
		// 成功同步
		reply.Err = resp.Err
		DPrintf("%d PutAppend Op ok, op: %+v ", kv.Me(), op)
	case <-time.After(defaultTimeout):
		// 超时
		DPrintf("%d PutAppend Op timeout, op: %+v ", kv.Me(), op)
		reply.Err = ErrTimeout
	}
	// 删除无用的 notify
	kv.guard(func() {
		delete(kv.notifyMap, index)
	})
}

func (kv *ShardKV) isLatestRequest(clientId int64, seq int) bool {
	record, ok := kv.recordMap[clientId]
	if !ok {
		return false
	}
	// 等于：当前请求重复提交
	// 小于：以前的请求重复提交
	return seq <= record.Seq
}

// Kill
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
	atomic.StoreInt32(&kv.dead, 1)
}

func (kv *ShardKV) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *ShardKV) Me() int {
	val := int32(kv.me)
	return int(atomic.LoadInt32(&val))
}

func (kv *ShardKV) apply() {
	for !kv.killed() {
		select {
		case message := <-kv.applyCh:
			DPrintf("%d handle apply message: %+v ", kv.Me(), message.CommandIndex)
			if message.CommandValid {
				index := message.CommandIndex
				if int64(index) <= atomic.LoadInt64(&kv.lastApplied) {
					DPrintf("%d discard apply op, message: %+v, lastApplied: %+v ", kv.Me(), message, kv.lastApplied)
					continue
				}
				kv.mu.Lock()
				// 记录 lastApplied
				atomic.StoreInt64(&kv.lastApplied, int64(index))
				command, ok := message.Command.(Op)
				if !ok {
					log.Fatalf("invalid message command: %+v", message.Command)
				}
				clientId := command.ClientId
				seq := command.Seq
				resp := &clientResp{}
				switch command.Action {
				case GetAction:
					val, exists := kv.store[command.Key] // 读取数据
					if !exists {
						resp.Err = ErrNoKey
					} else {
						resp.Err = OK
						resp.Value = val
					}
					DPrintf("%d apply op successful, action: %+v, Resp: %+v ", kv.Me(), command.Action, resp)
				case PutAction, AppendAction: // put 更新，append 添加
					if kv.isLatestRequest(clientId, seq) {
						// 如果是重复提交
						record := kv.recordMap[clientId]
						resp = record.Resp // 使用上次的 Resp
						DPrintf("%d apply op exists, action: %+v, Resp: %+v ", kv.Me(), command.Action, resp)
					} else {
						resp.Value = command.Value
						resp.Err = OK
						record := &clientRecord{
							Seq:   seq,
							Index: index,
							Resp:  resp,
						}
						kv.recordMap[clientId] = record
						old, exist := kv.store[command.Key]
						if command.Action == AppendAction && exist {
							kv.store[command.Key] = old + command.Value
						} else {
							kv.store[command.Key] = command.Value
						}
						DPrintf("%d apply op successful, action: %+v, Resp: %+v ", kv.Me(), command.Action, resp)
					}
				}
				term, isLeader := kv.rf.GetState()
				if term == message.CommandTerm && isLeader {
					// 注意：只有 leader 会收到请求
					notify, exist := kv.notifyMap[index]
					DPrintf("%d notify op, exist: %+v, action: %+v, Resp: %+v ", kv.Me(), exist, command.Action, resp)
					if exist {
						notify <- resp
					}
				}
				// 主动 snapshot
				if kv.needSnapshot() {
					kv.doSnapshot(index)
				}
				kv.mu.Unlock()
			} else if message.SnapshotValid {
				// 处理快照
				// 如果快照序号小于等于已应用的序号，那么无需安装快照
				if atomic.LoadInt64(&kv.lastApplied) >= int64(message.SnapshotIndex) {
					DPrintf("%d discard snapshot op, message: %+v, lastApplied: %+v ", kv.Me(), message, kv.lastApplied)
					continue
				}
				kv.mu.Lock()
				DPrintf("%d request for snapshot, message: %+v ", kv.Me(), message)
				if kv.rf.CondInstallSnapshot(message.SnapshotTerm, message.SnapshotIndex, message.Snapshot) {
					DPrintf("%d request for snapshot successful, message: %+v ", kv.Me(), message)
					// follower 读快照
					store, recordMap, err := kv.decodeSnapshot(message.Snapshot) // 读取快照数据，store & record
					if err != nil {
						log.Fatalf("read snapshot err: %v", err)
					}
					kv.store = store
					kv.recordMap = recordMap

					atomic.StoreInt64(&kv.lastApplied, int64(message.SnapshotIndex)) // 记录 lastApplied
				}
				kv.mu.Unlock()
			} else {
				log.Fatalf("invalid message: %+v", message)
			}
		}
	}
}

func (kv *ShardKV) needSnapshot() bool {
	if kv.maxraftstate < 0 {
		return false
	}
	sz := kv.persister.RaftStateSize() // 与 maxraftstate 比较，判断是否需要快照
	DPrintf("%d need snapshot, sz: %d, maxraftstate: %d ", kv.Me(), sz, kv.maxraftstate)
	return sz > int(float32(kv.maxraftstate)*0.9)
}

func (kv *ShardKV) doSnapshot(index int) {
	DPrintf("%d do snapshot, index: %d ", kv.Me(), index)
	snapshotBytes, err := kv.encodeState()
	if err != nil {
		log.Fatalf("do snapshot err: %v", err)
	}
	kv.rf.Snapshot(index, snapshotBytes)
}

func (kv *ShardKV) encodeState() ([]byte, error) {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	err := e.Encode(kv.store)
	if err != nil {
		return nil, err
	}
	err = e.Encode(kv.recordMap)
	if err != nil {
		return nil, err
	}

	return w.Bytes(), nil
}

func (kv *ShardKV) decodeSnapshot(data []byte) (store map[string]string,
	recordMap map[int64]*clientRecord, err error) {
	if len(data) == 0 {
		err = fmt.Errorf("snapshot data empty")
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	err = d.Decode(&store)
	if err != nil {
		return
	}
	err = d.Decode(&recordMap)
	if err != nil {
		return
	}

	return
}

func (kv *ShardKV) guard(fn func()) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	fn()
}

// StartServer
// servers[] contains the ports of the servers in this group.
//
// param me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int,
	gid int, ctrlers []*labrpc.ClientEnd, makeEnd func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.makeEnd = makeEnd
	kv.gid = gid
	kv.ctrlers = ctrlers

	// Your initialization code here.

	// Use something like this to talk to the shardctrler:
	// kv.mck = shardctrler.MakeClerk(kv.ctrlers)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.persister = persister
	snapshot := persister.ReadSnapshot()
	var (
		store     map[string]string
		recordMap map[int64]*clientRecord
		err       error
	)
	if len(snapshot) > 0 {
		store, recordMap, err = kv.decodeSnapshot(snapshot) // 读取快照数据，store & record
	} else {
		store = map[string]string{}
		recordMap = map[int64]*clientRecord{}
	}

	if err != nil {
		log.Fatalf("% peer read snapshot err: %v", me, err)
	}

	kv.store = store
	kv.recordMap = recordMap
	kv.notifyMap = map[int]chan *clientResp{}

	return kv
}
