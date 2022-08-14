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

	CommandId int64
	ClientId  int64
}

// 每个 client 都需要一个唯一的标识符，它的每个不同命令需要有一个顺序递增的 commandId，clientId 和这个 commandId，
// clientId 可以唯一确定一个不同的命令，从而使得各个 raft 节点可以记录保存各命令是否已应用以及应用以后的结果。
type clientRecord struct {
	commandId int64
	index     int
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
	store       sync.Map // kv存储
	recordMap   sync.Map // 操作记录
	notifyMap   sync.Map
	lastApplied int64 // 最后已应用 command
}

// Get 如果 kvserver 不是多数的一部分，则不应完成 Get() RPC（这样它就不会提供陈旧的数据）。
// 一个简单的解决方案是在 Raft 日志中输入每个 Get()（以及每个 Put() 和 Append()）。
// Get 也需要提交 raft 日志，保证客户端不会读到脏数据
func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	if kv.killed() {
		reply.Err = ErrShutdown
		return
	}

	// submit op
	op := Op{
		Action:    GetAction,
		Key:       args.Key,
		CommandId: args.CommandId,
		ClientId:  args.ClientId,
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
	kv.notifyMap.Store(index, notify)

	select {
	case resp := <-notify:
		// 成功同步
		reply.Value = resp.value
		reply.Err = resp.err
		DPrintf("%d Get Op ok, op: %+v ", kv.Me(), op)
	case <-time.After(defaultTimeout):
		// 超时
		reply.Err = ErrTimeout
		DPrintf("%d Get Op timeout, op: %+v ", kv.Me(), op)
	}
	// 删除无用的 notify
	go func() {
		kv.notifyMap.Delete(index)
	}()
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	if kv.killed() {
		reply.Err = ErrShutdown
		return
	}

	if kv.isLatestRequest(args.ClientId, args.CommandId) {
		// 如果是重复提交
		record, _ := kv.recordMap.Load(args.ClientId)
		cr := record.(*clientRecord)
		reply.Err = cr.resp.err // 使用上次的 resp
		DPrintf("%d PutAppend duplicate, args: %+v, reply: %+v ", kv.Me(), args, reply)
		return
	}

	argOp := args.Op
	op := Op{
		Action:    argOp,
		Key:       args.Key,
		Value:     args.Value,
		CommandId: args.CommandId,
		ClientId:  args.ClientId,
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
	DPrintf("%d PutAppend Op successful, op: %+v ", kv.Me(), op)

	// 只有 leader 需要 notify 客户端
	notify := make(chan *clientResp)
	kv.notifyMap.Store(index, notify)

	select {
	case resp := <-notify: // 阻塞
		// 成功同步
		reply.Err = resp.err
		DPrintf("%d PutAppend Op ok, op: %+v ", kv.Me(), op)
	case <-time.After(defaultTimeout):
		// 超时
		DPrintf("%d PutAppend Op timeout, op: %+v ", kv.Me(), op)
		reply.Err = ErrTimeout
	}
	// 删除无用的 notify
	go func() {
		kv.notifyMap.Delete(index)
	}()
}

func (kv *KVServer) Me() int {
	val := int32(kv.me)
	return int(atomic.LoadInt32(&val))
}

func (kv *KVServer) isLatestRequest(clientId, commandId int64) bool {
	record, ok := kv.recordMap.Load(clientId)
	if !ok {
		return false
	}
	cr := record.(*clientRecord)
	// 等于：当前请求重复提交
	// 小于：以前的请求重复提交
	return commandId <= cr.commandId
}

func (kv *KVServer) apply() {
	for !kv.killed() {
		select {
		case message := <-kv.applyCh:
			if message.CommandValid {
				index := message.CommandIndex
				if int64(index) <= atomic.LoadInt64(&kv.lastApplied) {
					DPrintf("%d discard apply op, message: %+v, lastApplied: %+v ", kv.Me(), message, kv.lastApplied)
					continue
				}
				atomic.StoreInt64(&kv.lastApplied, int64(index))

				command, ok := message.Command.(Op)
				if !ok {
					log.Fatalf("invalid message command: %+v", message.Command)
				}
				clientId := command.ClientId
				commandId := command.CommandId
				resp := &clientResp{}
				switch command.Action {
				case GetAction:
					val, exists := kv.store.Load(command.Key) // 读取数据
					if !exists {
						resp.err = ErrNoKey
					} else {
						resp.err = OK
						resp.value = val.(string)
					}
					DPrintf("%d apply op successful, action: %+v, resp: %+v ", kv.Me(), command.Action, resp)
				case PutAction, AppendAction: // put 更新，append 添加
					if kv.isLatestRequest(clientId, commandId) {
						// 如果是重复提交
						record, _ := kv.recordMap.Load(clientId)
						resp = record.(*clientRecord).resp // 使用上次的 resp
						DPrintf("%d apply op exists, action: %+v, resp: %+v ", kv.Me(), command.Action, resp)
					} else {
						// 非重复提交
						// 相同的 clientId、commandId 日志虽然会被提交
						// 但是只会 apply 一次，这样就不会因 apply 多次而出现数据问题
						// 所以 client 的最后一次请求必须记住
						resp.value = command.Value
						resp.err = OK
						record := &clientRecord{
							commandId: commandId,
							index:     index,
							resp:      resp,
						}
						kv.recordMap.Store(clientId, record)
						// 更新 store
						// 注意：Append 是追加，put 是更新
						old, exist := kv.store.Load(command.Key)
						if command.Action == AppendAction && exist {
							// 如果是 append 且已存在，那么将 value 追加到后面
							kv.store.Store(command.Key, old.(string)+command.Value)
						} else {
							// 否则统一当成 put 处理
							kv.store.Store(command.Key, command.Value)
						}
						DPrintf("%d apply op successful, action: %+v, resp: %+v ", kv.Me(), command.Action, resp)
					}
				}
				// leader回应客户端请求
				term, isLeader := kv.rf.GetState()
				// 任期相同，且是 leader 才需要通过对应的客户端
				// 为啥需要任期？
				// 因此 leader 可能前一秒是，下一次可能就不是了
				// 节点状态发生了变更，那么 term 肯定会变，因此需要判断
				if term == message.CommandTerm && isLeader {
					// 注意：只有 leader 会收到请求
					notify, exist := kv.notifyMap.Load(index)
					DPrintf("%d notify op, exist: %+v, action: %+v, resp: %+v ", kv.Me(), exist, command.Action, resp)
					if exist {
						// 为啥在任期一致且是leader的情况下，会存在 notify 不存在的情况了？
						// 超时会删除 notify，因此重试的时候会找不到
						notify.(chan *clientResp) <- resp
					}
				}
			} else if message.SnapshotValid {
				// TODO
			} else {
				log.Fatalf("invalid message: %+v", message)
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
// I am the index of the current server in servers[].
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
	kv.store = sync.Map{}
	kv.recordMap = sync.Map{}
	kv.notifyMap = sync.Map{}
	go kv.apply()

	return kv
}
