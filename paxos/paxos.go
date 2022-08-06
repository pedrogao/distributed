package paxos

//
// Paxos library, to be included in an application.
// Multiple applications will run, each including
// a Paxos peer.
//
// Manages a sequence of agreed-on values.
// The set of peers is fixed.
// Copes with network failures (partition, msg loss, &c).
// Does not store anything persistently, so cannot handle crash+restart.
//
// The application interface:
//
// px = paxos.Make(peers []string, me string)
// px.Start(seq int, v interface{}) -- start agreement on new instance
// px.Status(seq int) (Fate, v interface{}) -- get info about an instance
// px.Done(seq int) -- ok to forget all instances <= seq
// px.Max() int -- highest instance seq known, or -1
// px.Min() int -- instances before this seq have been forgotten
//

import (
	"math/rand"
	"net"
	"net/rpc"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/pedrogao/log"
)

// px.Status() return values, indicating
// whether an agreement has been decided,
// or Paxos has not yet reached agreement,
// or it was agreed but forgotten (i.e. < Min()).
type Fate int

const (
	Decided   Fate = iota + 1
	Pending        // not yet decided.
	Forgotten      // decided but forgotten.
)

type instance struct {
	state       Fate   // 状态
	proposeNum  string // 提案编号
	acceptNum   string // 接收编号
	acceptValue any    // 接收值
}

type Paxos struct {
	mu         sync.Mutex
	l          net.Listener
	dead       int32 // for testing
	unreliable int32 // for testing
	rpcCount   int32 // for testing
	peers      []string
	me         int // index into peers[]

	// Your data here.
	versions    []int             // 节点版本号
	instanceMap map[int]*instance // 实例表，peerId => instance
}

// PrepareArgs 准备节点，生成提案编号
// 相当于二次提交的准备节点，询问是否可以加锁
// 由于分布式的网络问题，可能因为网络原因导致加锁成功，
// 而一次无法解锁，这样节点就会陷入死锁，因此分布式加锁
// 一般都是可抢占的，即编号高的可以抢占编号低的锁
type PrepareArgs struct {
	Seq        int
	ProposeNum string // 提案编号
}

type PrepareReply struct {
	Success     bool   // 准备是否成功
	AcceptNum   string // 已有提案序号
	AcceptValue any
}

// AcceptArgs 接收阶段，编号、接收值再次请求
type AcceptArgs struct {
	Seq         int
	AcceptNum   string
	AcceptValue any
}

type AcceptReply struct {
	Success bool // 准备接收成功
}

// DecideArgs 决策阶段，接收成功后，二次协商完成，因此决策值是否采用
type DecideArgs struct {
	Seq        int
	ProposeNum string
	Value      any
	Me         int
	Done       int
}

type DecideReply struct {
	Success bool // 准备决定成功
}

func (px *Paxos) Prepare(args *PrepareArgs, reply *PrepareReply) error {
	px.mu.Lock()
	defer px.mu.Unlock()

	ins, exist := px.instanceMap[args.Seq]
	if exist {
		// 已存在
		if args.ProposeNum > ins.proposeNum {
			reply.Success = true
		} else {
			// 如果编号比我小，那么返回失败
			reply.Success = false
		}
	} else {
		// 不存在，创建一个，那么 accept 值都是空
		ins = &instance{
			state: Pending, // 状态
		}
		px.instanceMap[args.Seq] = ins
		reply.Success = true
	}
	// 返回 acceptValue
	if reply.Success {
		reply.AcceptNum = ins.acceptNum
		reply.AcceptValue = ins.acceptValue
		px.instanceMap[args.Seq].proposeNum = args.ProposeNum // 设置提案编号
	}
	return nil
}

func (px *Paxos) Accept(args *AcceptArgs, reply *AcceptReply) error {
	px.mu.Lock()
	defer px.mu.Unlock()
	ins, exist := px.instanceMap[args.Seq]
	if exist {
		// 存在就比较编号大小，与 Prepare 不同，还可以接受等于的
		if args.AcceptNum >= ins.proposeNum {
			reply.Success = true
		} else {
			// 如果编号比我小，那么返回失败
			reply.Success = false
		}
	} else {
		// 不存在，创建一个，那么 accept 值都是空
		ins = &instance{
			state: Pending, // 状态
		}
		px.instanceMap[args.Seq] = ins
		reply.Success = true
	}
	// 成功
	if reply.Success {
		px.instanceMap[args.Seq].acceptNum = args.AcceptNum
		px.instanceMap[args.Seq].acceptValue = args.AcceptValue // accept 才更新值
		px.instanceMap[args.Seq].proposeNum = args.AcceptNum
	}
	return nil
}

func (px *Paxos) Decide(args *DecideArgs, reply *DecideReply) error {
	px.mu.Lock()
	defer px.mu.Unlock()

	ins, exist := px.instanceMap[args.Seq]
	if exist {
		// 更新
		ins.proposeNum = args.ProposeNum
		ins.acceptNum = args.ProposeNum
		ins.acceptValue = args.Value
		ins.state = Decided
		px.instanceMap[args.Seq] = ins
		px.versions[args.Me] = args.Done // 更新已提交值
	} else {
		px.instanceMap[args.Seq] = &instance{
			state: Pending,
		}
	}
	return nil
}

// sendPrepare 发送 Prepare
func (px *Paxos) sendPrepare(seq int, v any) (bool, string, any) {
	proposeNum := px.generateProposeNum()
	args := &PrepareArgs{
		Seq:        seq,
		ProposeNum: proposeNum,
	}
	log.Debugf("PrepareArgs{ Seq: %v, ProposeNum: %v }\n", seq, proposeNum)

	replyProposeNum := ""
	num := 0
	for i, peer := range px.peers {
		var reply PrepareReply
		if i == px.me {
			px.Prepare(args, &reply)
		} else {
			call(peer, "Paxos.Prepare", args, &reply)
		}
		// 成功
		if reply.Success {
			num++
			// 编号大的
			if reply.AcceptNum > replyProposeNum {
				replyProposeNum = reply.AcceptNum
				v = reply.AcceptValue
			}
		}
	}
	// 是否成功，提案编号，接收值
	return num > len(px.peers)/2, replyProposeNum, v
}

func (px *Paxos) sendAccept(seq int, proposeNum string, v any) bool {
	args := &AcceptArgs{
		Seq:         seq,
		AcceptNum:   proposeNum,
		AcceptValue: v,
	}

	log.Debugf("AcceptArgs{ Seq: %v, AcceptPNum: %v, AcceptPValue: %v }\n", seq, proposeNum, v)

	num := 0
	for i, peer := range px.peers {
		var reply AcceptReply
		if i == px.me {
			px.Accept(args, &reply)
		} else {
			call(peer, "Paxos.Accept", args, &reply)
		}
		// 成功
		if reply.Success {
			num++
		}
	}
	return num > len(px.peers)/2
}

func (px *Paxos) sendDecide(seq int, proposeNum string, v any) {
	px.mu.Lock()
	defer px.mu.Unlock()

	px.instanceMap[seq].proposeNum = proposeNum
	px.instanceMap[seq].acceptNum = proposeNum
	px.instanceMap[seq].acceptValue = v
	px.instanceMap[seq].state = Decided
	tmp := px.instanceMap[seq]
	log.Debugf("instance{ state: %v, proposeNum: %v, acceptNum: %v, acceptValue: %v }\n",
		tmp.state, tmp.proposeNum, tmp.acceptNum, tmp.acceptValue)

	args := &DecideArgs{
		Seq:        seq,
		ProposeNum: proposeNum,
		Value:      v,
		Me:         px.me,
		Done:       px.versions[px.me], // 确认值版本
	}

	for i, peer := range px.peers {
		if i == px.me {
			continue
		}

		var reply DecideReply
		go call(peer, "Paxos.Decide", args, &reply)
	}
}

func call(srv string, name string, args any, reply any) bool {
	c, err := rpc.Dial("unix", srv)
	if err != nil {
		err1 := err.(*net.OpError)
		if err1.Err != syscall.ENOENT && err1.Err != syscall.ECONNREFUSED {
			log.Debugf("paxos Dial() failed: %v\n", err1)
		}
		return false
	}
	defer c.Close()

	err = c.Call(name, args, reply)
	if err == nil {
		return true
	}

	log.Debug(err)
	return false
}

//
// the application wants paxos to start agreement on
// instance seq, with proposed value v.
// Start() returns right away; the application will
// call Status() to find out if/when agreement
// is reached.
//
func (px *Paxos) Start(seq int, v any) {
	// Your code here.
	log.Debugf("Paxos server Start, seq = %v, value = %v\n", seq, v)

	go func() {
		// 拒绝
		if seq < px.Min() {
			return
		}

		for {
			// 第一阶段，Prepare
			ok, proposeNum, val := px.sendPrepare(seq, v)

			if ok {
				// 第二阶段，Accept
				ok = px.sendAccept(seq, proposeNum, val)
			}

			if ok {
				// 第三阶段，Decide，相当于 Learner 获取提案
				px.sendDecide(seq, proposeNum, val)
				break
			}

			// 获取 seq 状态
			status, _ := px.Status(seq)
			// 已决定，break
			if status == Decided {
				break
			}
		}
	}()
}

//
// the application on this machine is done with
// all instances <= seq.
//
// see the comments for Min() for more explanation.
//
func (px *Paxos) Done(seq int) {
	// Your code here.
	px.mu.Lock()
	defer px.mu.Unlock()

	if seq > px.versions[px.me] {
		px.versions[px.me] = seq
	}
}

//
// the application wants to know the
// highest instance sequence known to
// this peer.
//
func (px *Paxos) Max() int {
	// Your code here.
	px.mu.Lock()
	defer px.mu.Unlock()

	max := 0
	for i := range px.instanceMap {
		if i > max {
			max = i // 找到最大 seq 值
		}
	}
	return max
}

//
// Min() should return one more than the minimum among z_i,
// where z_i is the highest number ever passed
// to Done() on peer i. A peers z_i is -1 if it has
// never called Done().
//
// Paxos is required to have forgotten all information
// about any instances it knows that are < Min().
// The point is to free up memory in long-running
// Paxos-based servers.
//
// Paxos peers need to exchange their highest Done()
// arguments in order to implement Min(). These
// exchanges can be piggybacked on ordinary Paxos
// agreement protocol messages, so it is OK if one
// peers Min does not reflect another Peers Done()
// until after the next instance is agreed to.
//
// The fact that Min() is defined as a minimum over
// *all* Paxos peers means that Min() cannot increase until
// all peers have been heard from. So if a peer is dead
// or unreachable, other peers Min()s will not increase
// even if all reachable peers call Done. The reason for
// this is that when the unreachable peer comes back to
// life, it will need to catch up on instances that it
// missed -- the other peers therefor cannot forget these
// instances.
//
func (px *Paxos) Min() int {
	// You code here.
	px.mu.Lock()
	defer px.mu.Unlock()

	min := px.versions[px.me]
	log.Infof("min = %v\n", min)
	for i := range px.versions {
		if px.versions[i] < min {
			min = px.versions[i] // 找到最小的版本
		}
	}

	for i, instance := range px.instanceMap {
		if i <= min && instance.state == Decided {
			// 删除掉那些已经没用的状态
			delete(px.instanceMap, i)
		}
	}

	return min + 1
}

//
// the application wants to know whether this
// peer thinks an instance has been decided,
// and if so what the agreed value is. Status()
// should just inspect the local peer state;
// it should not contact other Paxos peers.
//
func (px *Paxos) Status(seq int) (Fate, any) {
	// Your code here.
	if seq < px.Min() {
		return Forgotten, nil
	}

	px.mu.Lock()
	defer px.mu.Unlock()

	ins, exist := px.instanceMap[seq]
	if !exist {
		return Pending, nil
	}

	return ins.state, ins.acceptValue
}

// 生成一个propose编号
func (px *Paxos) generateProposeNum() string {
	begin := time.Date(2018, time.November, 17, 22, 0, 0, 0, time.UTC)
	duration := time.Now().Sub(begin)
	return strconv.FormatInt(duration.Nanoseconds(), 10) + "-" + strconv.Itoa(px.me)
}

//
// tell the peer to shut itself down.
// for testing.
// please do not change these two functions.
//
func (px *Paxos) Kill() {
	atomic.StoreInt32(&px.dead, 1)
	if px.l != nil {
		px.l.Close()
	}
}

//
// has this peer been asked to shut down?
//
func (px *Paxos) isdead() bool {
	return atomic.LoadInt32(&px.dead) != 0
}

// please do not change these two functions.
func (px *Paxos) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&px.unreliable, 1)
	} else {
		atomic.StoreInt32(&px.unreliable, 0)
	}
}

func (px *Paxos) isunreliable() bool {
	return atomic.LoadInt32(&px.unreliable) != 0
}

//
// the application wants to create a paxos peer.
// the ports of all the paxos peers (including this one)
// are in peers[]. this servers port is peers[me].
//
func Make(peers []string, me int, rpcs *rpc.Server) *Paxos {
	px := &Paxos{}
	px.peers = peers
	px.me = me

	// Your initialization code here.
	px.instanceMap = map[int]*instance{}
	// 从0开始计数，因此初始化为1
	px.versions = make([]int, len(peers))
	for i := range px.versions {
		px.versions[i] = -1
	}

	if rpcs != nil {
		// caller will create socket &c
		rpcs.Register(px)
	} else {
		rpcs = rpc.NewServer()
		rpcs.Register(px)

		// prepare to receive connections from clients.
		// change "unix" to "tcp" to use over a network.
		os.Remove(peers[me]) // only needed for "unix"
		l, e := net.Listen("unix", peers[me])
		if e != nil {
			log.Fatal("listen error: ", e)
		}
		px.l = l

		// please do not change any of the following code,
		// or do anything to subvert it.

		// create a thread to accept RPC connections
		go func() {
			for px.isdead() == false {
				conn, err := px.l.Accept()
				if err == nil && px.isdead() == false {
					if px.isunreliable() && (rand.Int63()%1000) < 100 {
						// discard the request.
						conn.Close()
					} else if px.isunreliable() && (rand.Int63()%1000) < 200 {
						// process the request but force discard of reply.
						c1 := conn.(*net.UnixConn)
						f, _ := c1.File()
						err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
						if err != nil {
							log.Debugf("shutdown: %v\n", err)
						}
						atomic.AddInt32(&px.rpcCount, 1)
						go rpcs.ServeConn(conn)
					} else {
						atomic.AddInt32(&px.rpcCount, 1)
						go rpcs.ServeConn(conn)
					}
				} else if err == nil {
					conn.Close()
				}
				if err != nil && px.isdead() == false {
					log.Debugf("Paxos(%v) accept: %v\n", me, err.Error())
				}
			}
		}()
	}

	return px
}
