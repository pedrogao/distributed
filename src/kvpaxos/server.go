package kvpaxos

import (
	"encoding/gob"
	"fmt"
	"math/rand"
	"net"
	"net/rpc"
	"os"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/pedrogao/log"

	"pedrogao/distributed/paxos"
)

var (
	// Debug Debugging
	Debug  = false
	logger = log.New(log.WithSkipLevel(3))
)

func init() {
	log.SetOptions(log.WithLevel(log.ErrorLevel))
	if os.Getenv("debug") != "" || Debug {
		logger.SetOptions(log.WithLevel(log.DebugLevel))
	} else {
		logger.SetOptions(log.WithLevel(log.ErrorLevel))
	}
}

func DPrintf(format string, a ...any) {
	logger.Debugf(format, a...)
	return
}

type Action = string

const (
	GetAction    Action = "Get"
	PutAction    Action = "Put"
	AppendAction Action = "Append"
)

// Op paxos 提案格式
type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Key    string
	Value  string
	Action string // "Put", "Append" or "Get"

	ClientId int64
	Seq      int
}

type KVPaxos struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       int32 // for testing
	unreliable int32 // for testing
	px         *paxos.Paxos

	// Your definitions here.
	store       map[string]string
	lastApplied int
	recordMap   map[int64]*clientRecord // 操作记录
}

type clientRecord struct {
	Seq int
	// Index int
	Resp *clientResp
}

type clientResp struct {
	Value string
	Err   Err
}

func (kv *KVPaxos) Get(args *GetArgs, reply *GetReply) error {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	op := Op{
		Key:      args.Key,
		Action:   GetAction,
		ClientId: args.ClientId,
		Seq:      args.Seq,
	}
	// 下一个应用序号
	seq := kv.lastApplied + 1
	for {
		// 提交
		kv.px.Start(seq, op)
		// 等待，暂时不考虑超时，一直循环
		val := kv.wait(seq)
		if val == op { // 提交的 op 被 decided 后 break
			break
		}
		seq++
	}

	// apply
	for ; kv.lastApplied+1 < seq; kv.lastApplied++ {
		val := kv.wait(kv.lastApplied + 1)
		kv.apply(val.(Op))
	}

	ret, exists := kv.store[op.Key]
	if !exists {
		reply.Err = ErrNoKey
		DPrintf("%d Get Op err, op: %+v ", kv.Me(), op)
		return nil
	}

	reply.Value = ret
	reply.Err = OK
	DPrintf("%d Get Op ok, op: %+v ", kv.Me(), op)
	kv.px.Done(kv.lastApplied)

	return nil
}

func (kv *KVPaxos) isLatestRequest(clientId int64, seq int) bool {
	record, ok := kv.recordMap[clientId]
	if !ok {
		return false
	}
	// 等于：当前请求重复提交
	// 小于：以前的请求重复提交
	return seq <= record.Seq
}

func (kv *KVPaxos) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if kv.isLatestRequest(args.ClientId, args.Seq) {
		reply.Err = kv.recordMap[args.ClientId].Resp.Err
		DPrintf("%d PutAppend duplicate, args: %+v, reply: %+v ", kv.Me(), args, reply)
		return nil
	}

	op := Op{
		Key:      args.Key,
		Value:    args.Value,
		Action:   args.Op,
		ClientId: args.ClientId,
		Seq:      args.Seq,
	}
	// 下一个应用序号
	seq := kv.lastApplied + 1
	// 提交
	for {
		DPrintf("%d PutAppend seq, op: %+v, seq: %+v ", kv.Me(), op, seq)
		kv.px.Start(seq, op)
		// 等待，暂时不考虑超时，一直循环
		val := kv.wait(seq)
		if val == op { // 提交的 op 被 decided 后 break
			break
		}
		seq++
	}
	// put、append可以无需apply，在 get的时候再 apply
	reply.Err = OK
	DPrintf("%d PutAppend Op ok, op: %+v ", kv.Me(), op)

	return nil
}

func (kv *KVPaxos) wait(seq int) any {
	to := 10 * time.Millisecond

	for {
		status, val := kv.px.Status(seq)
		if status == paxos.Decided {
			return val
		}
		if status == paxos.Forgotten { // forgotten
			break
		}
		time.Sleep(to)
		if to < 3*time.Second {
			to *= 2
		}
	}

	return nil
}

func (kv *KVPaxos) apply(op Op) (ret string, exists bool) {
	DPrintf("%d handle apply op: %+v ", kv.Me(), op)

	resp := &clientResp{}
	resp.Err = OK

	switch op.Action {
	case GetAction:
		ret, exists = kv.store[op.Key]
	case PutAction:
		kv.store[op.Key] = op.Value
		resp.Value = op.Value
	case AppendAction:
		old, exist := kv.store[op.Key]
		if exist {
			// 如果是 append 且已存在，那么将 Value 追加到后面
			newVal := old + op.Value
			kv.store[op.Key] = newVal
			resp.Value = newVal
		} else {
			// 否则统一当成 put 处理
			kv.store[op.Key] = op.Value
			resp.Value = op.Value
		}
	}

	record := &clientRecord{
		Seq:  op.Seq,
		Resp: resp,
	}
	kv.recordMap[op.ClientId] = record

	return
}

func (kv *KVPaxos) Me() int {
	val := int32(kv.me)
	return int(atomic.LoadInt32(&val))
}

// tell the server to shut itself down.
// please do not change these two functions.
func (kv *KVPaxos) kill() {
	DPrintf("Kill(%d): die", kv.me)
	atomic.StoreInt32(&kv.dead, 1)
	_ = kv.l.Close()
	kv.px.Kill()
}

// call this to find out if the server is dead.
func (kv *KVPaxos) isDead() bool {
	return atomic.LoadInt32(&kv.dead) != 0
}

// please do not change these two functions.
func (kv *KVPaxos) setUnreliable(what bool) {
	if what {
		atomic.StoreInt32(&kv.unreliable, 1)
	} else {
		atomic.StoreInt32(&kv.unreliable, 0)
	}
}

func (kv *KVPaxos) isUnreliable() bool {
	return atomic.LoadInt32(&kv.unreliable) != 0
}

// StartServer
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
//
func StartServer(servers []string, me int) *KVPaxos {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})

	kv := new(KVPaxos)
	kv.me = me

	// Your initialization code here.
	kv.store = map[string]string{}
	kv.recordMap = map[int64]*clientRecord{}
	kv.lastApplied = 0

	rpcs := rpc.NewServer()
	rpcs.Register(kv)

	kv.px = paxos.Make(servers, me, rpcs)

	os.Remove(servers[me])
	l, e := net.Listen("unix", servers[me])
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	kv.l = l

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for kv.isDead() == false {
			conn, err := kv.l.Accept()
			if err == nil && kv.isDead() == false {
				if kv.isUnreliable() && (rand.Int63()%1000) < 100 {
					// discard the request.
					_ = conn.Close()
				} else if kv.isUnreliable() && (rand.Int63()%1000) < 200 {
					// process the request but force discard of reply.
					c1 := conn.(*net.UnixConn)
					f, _ := c1.File()
					err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
					if err != nil {
						fmt.Printf("shutdown: %v\n", err)
					}
					go rpcs.ServeConn(conn)
				} else {
					go rpcs.ServeConn(conn)
				}
			} else if err == nil {
				_ = conn.Close()
			}
			if err != nil && kv.isDead() == false {
				fmt.Printf("KVPaxos(%v) accept: %v\n", me, err.Error())
				kv.kill()
			}
		}
	}()

	return kv
}
