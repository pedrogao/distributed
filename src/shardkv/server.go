package shardkv

import (
	"bytes"
	"fmt"
	"os"
	"sync"
	"sync/atomic"

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

	CommandId int64
	ClientId  int64
}

type clientRecord struct {
	CommandId int64
	Index     int
	Resp      *clientResp
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
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
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
			log.Fatalf("invalid message: %+v", message)
		}
	}
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
