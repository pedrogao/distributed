package kvraft

import (
	"os"
	"sync"
	"sync/atomic"
	"time"

	"pedrogao/distributed/labgob"
	"pedrogao/distributed/labrpc"
	"pedrogao/distributed/raft"

	"github.com/pedrogao/log"
)

var (
	// Debug Debugging
	Debug  = true
	logger = log.New(log.WithSkipLevel(3))
)

func init() {
	if os.Getenv("debug") != "" || Debug {
		logger.SetOptions(log.WithLevel(log.DebugLevel))
	} else {
		logger.SetOptions(log.WithLevel(log.ErrorLevel))
	}
}

func DPrintf(format string, a ...any) /*(n int, err error)*/ {
	logger.Debugf(format, a...)
	return
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
	Action    string
	Key       string
	Value     string
	CommandId int64
	ClientId  int64
}

// 每个 client 都需要一个唯一的标识符，它的每个不同命令需要有一个顺序递增的 commandId，clientId 和这个 commandId，
// clientId 可以唯一确定一个不同的命令，从而使得各个 raft 节点可以记录保存各命令是否已应用以及应用以后的结果。
type clientRecord struct {
	commandId int64
	applied   bool
	index     int
	term      int
	resp      *clientResp
}

type clientResp struct {
	value string
	err   Err
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	// if maxraftstate is -1,  you don't need to snapshot.
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	store     map[string]string       // kv存储
	recordMap map[int64]*clientRecord // 客户端最后操作记录
	notifyMap map[int64]chan *clientResp
}

// 如果 kvserver 不是多数的一部分，则不应完成 Get() RPC（这样它就不会提供陈旧的数据）。
// 一个简单的解决方案是在 Raft 日志中输入每个 Get()（以及每个 Put() 和 Append()）。
// Get 也需要提交 raft 日志，保证客户端不会读到脏数据
func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	key := args.Key
	// 检查重复？

	// val, exists := kv.store[key]
	// if !exists {
	// 	reply.Err = ErrNoKey
	// 	return
	// }
	op := Op{
		Action:    GetAction,
		Key:       key,
		CommandId: args.CommandId,
		ClientId:  args.ClientId,
	}
	index, term, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	record := &clientRecord{
		commandId: args.CommandId,
		applied:   false,
		index:     index,
		term:      term,
	}
	kv.mu.Lock()
	kv.recordMap[args.ClientId] = record
	notify, ok := kv.notifyMap[args.ClientId]
	if !ok {
		notify = make(chan *clientResp, 1)
		kv.notifyMap[args.ClientId] = notify
	}
	kv.mu.Unlock() // 解锁

	select {
	case resp := <-notify:
		// 成功同步
		reply.Value = resp.value
		reply.Err = resp.err
	case <-time.After(defaultTimeout):
		// 超时
		reply.Err = ErrTimeout
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// FIXME 如果第一次出错了，就会导致后续的所有均返回同一个错误
	if kv.isDuplicateRequest(args.ClientId, args.CommandId) {
		kv.mu.Lock()
		defer kv.mu.Unlock()
		record := kv.recordMap[args.ClientId]
		defer DPrintf("%d PutAppend dumplicate, args: %v, reply: %v ", kv.me, args, reply)
		if record.resp == nil {
			reply.Err = ErrWrongLeader
			return
		}
		reply.Err = record.resp.err // 此时 op 还没 apply，所以没有 resp
		return
	}

	kv.mu.Lock()
	notify, ok := kv.notifyMap[args.ClientId]
	if !ok {
		notify = make(chan *clientResp, 1)
		kv.notifyMap[args.ClientId] = notify
	}
	kv.mu.Unlock() // 解锁

	argOp := args.Op
	op := Op{
		Action:    argOp,
		Key:       args.Key,
		Value:     args.Value,
		CommandId: args.CommandId,
		ClientId:  args.ClientId,
	}
	index, term, isLeader := kv.rf.Start(op)
	if !isLeader {
		DPrintf("%d PutAppend wrong leader, args: %v, reply: %v ", kv.me, args, reply)
		reply.Err = ErrWrongLeader
		return
	}
	DPrintf("%d PutAppend Op successful, op: %v ", kv.me, op)
	record := &clientRecord{
		commandId: args.CommandId,
		applied:   false,
		index:     index,
		term:      term,
	}
	kv.mu.Lock()
	kv.recordMap[args.ClientId] = record
	kv.mu.Unlock() // 解锁
	// TODO Start Op 成功后，应该等待服务同步完成
	select {
	case resp := <-notify: // 阻塞
		// 成功同步
		reply.Err = resp.err
	case <-time.After(defaultTimeout):
		// 超时
		reply.Err = ErrTimeout
	}
}

func (kv *KVServer) isDuplicateRequest(clientId, commandId int64) bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	record, ok := kv.recordMap[clientId]
	if !ok {
		return false
	}
	return record.commandId == commandId
}

func (kv *KVServer) apply() {
	for !kv.killed() {
		select {
		case message := <-kv.applyCh:
			if message.CommandValid {
				// index := message.CommandIndex
				command, ok := message.Command.(Op)
				if !ok {
					log.Fatalf("invalid message command: %v", message.Command)
				}
				clientId := command.ClientId
				commandId := command.CommandId
				switch command.Action {
				case GetAction:
					resp := &clientResp{}
					kv.mu.Lock()
					notify := kv.notifyMap[clientId]
					val, exists := kv.store[command.Key]
					kv.mu.Unlock()
					if !exists {
						resp.err = ErrNoKey
					} else {
						resp.value = val
					}
					DPrintf("%d apply op successful, action: %v, resp: %v ", kv.me, command.Action, resp)
					notify <- resp
				case PutAction, AppendAction: // put 更新，append 添加
					resp := &clientResp{}
					kv.mu.Lock()
					notify := kv.notifyMap[clientId]
					kv.store[command.Key] = command.Value
					resp.value = command.Value
					record, exists := kv.recordMap[clientId]
					if exists && record.commandId == commandId {
						record.resp = resp
					}
					kv.mu.Unlock()
					DPrintf("%d apply op successful, action: %v, resp: %v ", kv.me, command.Action, resp)
					notify <- resp
					// case AppendAction:
				}
			} else if message.SnapshotValid {
				// TODO
			} else {
				log.Fatalf("invalid message: %v", message)
			}
		}
	}
}

func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// StartKVServer
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.store = map[string]string{}
	kv.recordMap = map[int64]*clientRecord{}
	kv.notifyMap = map[int64]chan *clientResp{}
	go kv.apply()

	return kv
}
