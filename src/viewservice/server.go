package viewservice

import (
	"net"
	"net/rpc"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pedrogao/log"
)

type ViewServer struct {
	mu   sync.Mutex
	l    net.Listener
	dead int32
	me   string

	// Your declarations here.
	clients    map[string]time.Time
	view       View
	primaryAck bool // 主节点是否ack
	volunteer  string
}

// Ping
// server Ping RPC handler.
func (vs *ViewServer) Ping(args *PingArgs, reply *PingReply) error {
	// Your code here.
	me := args.Me
	viewNum := args.ViewNum

	// set time
	vs.mu.Lock()
	defer vs.mu.Unlock()

	log.Debugf("server ping start from: %s, view: %v, args: %v, reply: %v",
		me, vs.view, args, reply)

	// the first ping from server 第一次 ping
	if viewNum == 0 && vs.view.Primary == "" {
		vs.view.Primary = me
		vs.primaryAck = false
		vs.view.ViewNum++
		reply.View = vs.view // 返回当前视图
		vs.clients[me] = time.Now()
		return nil
	}

	// ack from primary
	// 主节点，且当前 primaryAck 为 false，而且 viewNum 相同
	if me == vs.view.Primary && !vs.primaryAck && viewNum == vs.view.ViewNum {
		vs.primaryAck = true // 设置 primaryAck
		reply.View = vs.view

		vs.clients[me] = time.Now() // 更新时间
		return nil
	}

	// the first ping from backup 第一次 backup ping
	// 备份为空，主节点已经ack，且主节点不是me
	if vs.view.Backup == "" && vs.primaryAck && vs.view.Primary != me {
		// primary acked
		vs.view.Backup = me
		vs.view.ViewNum++
		vs.primaryAck = false // 备份节点加入了视图，但是主节点仍没有回复
		reply.View = vs.view

		vs.clients[me] = time.Now()
		return nil
	}

	// idle server 空闲服务，志愿者
	// 不是主节点、不是备份节点，而且 viewNum 是 0
	if me != vs.view.Backup && me != vs.view.Primary && viewNum == 0 {
		// add new idle server
		vs.volunteer = me
		log.Debugf("server Ping from volunteer: %s", vs.volunteer)
		vs.clients[me] = time.Now()
		return nil
	}

	// primary restart  主节点重启
	if me == vs.view.Primary && viewNum == 0 && vs.primaryAck == true {
		vs.view.Primary = vs.view.Backup // 备份节点成为主节点
		vs.view.Backup = me              // 当前节点成为备份节点
		vs.view.ViewNum++
		vs.primaryAck = false
		reply.View = vs.view

		vs.clients[me] = time.Now()
		return nil
	}

	// default 默认处理
	log.Debugf("sever ping default from: %s, view: %v", me, vs.view)
	vs.clients[me] = time.Now()
	reply.View = vs.view
	return nil
}

// Get
// server Get() RPC handler.
//
func (vs *ViewServer) Get(args *GetArgs, reply *GetReply) error {
	// Your code here.
	vs.mu.Lock()
	defer vs.mu.Unlock()

	reply.View = vs.view
	return nil
}

//
// tick() is called once per PingInterval; it should notice
// if servers have died or recovered, and change the view
// accordingly.
// 定时触发
//
func (vs *ViewServer) tick() {
	// Your code here.
	now := time.Now()

	vs.mu.Lock()
	defer vs.mu.Unlock()
	// 主节点为空，直接 return
	if vs.view.Primary == "" {
		return
	}

	// primary die
	pingTime, ok := vs.clients[vs.view.Primary]
	// 如果主节点超过 5 次 ping 都未回复
	if ok && now.Sub(pingTime) > (DeadPings*PingInterval) {
		log.Debugf("server primary: %s timeout: %v", vs.view.Primary, vs.view)
		// just wait，没有 ack 直接返回
		// 如果 primaryAck 为 false，证明之前也没有主节点，因此无法直接提升备份节点为主节点
		if vs.primaryAck == false {
			return
		}

		// has backup，有备份节点
		if vs.view.Backup != "" {
			vs.view.Primary = vs.view.Backup // 备份节点提升为主节点
			vs.view.ViewNum++                // 视图+1
			vs.primaryAck = false
			log.Debugf("new primary: %s view: %v", vs.view.Primary, vs.view)
			vs.view.Backup = "" // 备份节点为空
			// make a volunteer become a backup 将志愿者提升为备份节点
			if vs.volunteer != "" {
				volTime, ok := vs.clients[vs.volunteer]
				if ok && now.Sub(volTime) < PingInterval { // 如果志愿者有响应，那么将志愿者提升为备份
					vs.view.Backup = vs.volunteer
					vs.volunteer = ""
					log.Debugf("new backup: %s view: %v", vs.view.Backup, vs.view)
				}
			}
		}
		log.Debugf("primary server tick, view: %v", vs.view)
	}
	// 备份节点为空，就无需检测备份节点，直接返回
	if vs.view.Backup == "" {
		log.Debugf("backup empty, view: %v", vs.view)
		return
	}
	// 因为没有其它的讯息，所以只能通过断联来判断死亡
	// backup dies 检查备份节点是否死亡
	pingTime, ok = vs.clients[vs.view.Backup]
	if ok && now.Sub(pingTime) > (DeadPings*PingInterval) { // 备份节点长时间无回复，则表示死亡
		vs.view.Backup = ""   // 备份为空
		vs.view.ViewNum++     // 视图+1
		vs.primaryAck = false // 每次更新视图，就得重置 primaryAck

		log.Debugf("server backup timeout, volunteer: %s", vs.volunteer)
		// make a volunteer become a backup
		if vs.volunteer != "" {
			volTime, _ := vs.clients[vs.volunteer]
			if now.Sub(volTime) < PingInterval {
				vs.view.Backup = vs.volunteer
				vs.volunteer = ""
			}
		}
		log.Debugf("backup server tick, view: %v", vs.view)
	}
}

// Kill
// tells the server to shut itself down.
// for testing.
// please don't change this function.
//
func (vs *ViewServer) Kill() {
	atomic.StoreInt32(&vs.dead, 1)
	vs.l.Close()
}

func StartServer(me string) *ViewServer {
	vs := new(ViewServer)
	vs.me = me
	// Your vs.* initializations here.
	vs.clients = make(map[string]time.Time) // 客户端时间
	vs.view = View{}
	vs.view.ViewNum = 0
	vs.view.Primary = ""  // 无主节点
	vs.view.Backup = ""   // 无从节点
	vs.volunteer = ""     // 志愿者
	vs.primaryAck = false // 无回复

	// tell net/rpc about our RPC server and handlers.
	rpcSrv := rpc.NewServer()
	rpcSrv.Register(vs)

	// prepare to receive connections from clients.
	// change "unix" to "tcp" to use over a network.
	os.Remove(vs.me) // only needed for "unix"
	l, e := net.Listen("unix", vs.me)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	vs.l = l

	// please don't change any of the following code,
	// or do anything to subvert it.

	// create a thread to accept RPC connections from clients.
	go func(vs *ViewServer) {
		for atomic.LoadInt32(&vs.dead) == 0 {
			conn, err := vs.l.Accept()
			if err == nil && atomic.LoadInt32(&vs.dead) == 0 {
				go rpcSrv.ServeConn(conn)
			} else if err == nil {
				conn.Close()
			}
			if err != nil && atomic.LoadInt32(&vs.dead) == 0 {
				log.Errorf("ViewServer(%v) accept: %v\n", me, err.Error())
				vs.Kill()
			}
		}
	}(vs)

	// create a thread to call tick() periodically.
	go func(vs *ViewServer) {
		for atomic.LoadInt32(&vs.dead) == 0 {
			vs.tick()
			time.Sleep(PingInterval)
		}
	}(vs)

	return vs
}
