package pbservice

import (
	"math/rand"
	"net"
	"net/rpc"
	"os"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"pedrogao/distributed/viewservice"

	"github.com/pedrogao/log"
)

type PBServer struct {
	mu         sync.Mutex
	l          net.Listener
	dead       int32 // for testing
	unreliable int32 // for testing
	me         string
	vs         *viewservice.Clerk

	// Your declarations here.
	view      viewservice.View  // 视图
	kv        map[string]string // KV数据
	oldBackup string            // 备份
	alive     int32
}

func (pb *PBServer) Get(args *GetArgs, reply *GetReply) error {
	// Your code here.
	pb.mu.Lock()
	defer pb.mu.Unlock()

	log.Infof("server get, args: %v, reply: %v", args, reply)
	// 非主节点，或者节点已死亡，直接 err 返回
	if pb.view.Primary != pb.me || atomic.LoadInt32(&pb.alive) == 1 {
		reply.Value = ""
		reply.Err = ErrWrongServer
		return nil
	}

	key := args.Key
	value, ok := pb.kv[key]
	if ok {
		reply.Value = value
		reply.Err = OK
	} else {
		reply.Value = ""
		reply.Err = ErrNoKey
	}

	return nil
}

func (pb *PBServer) Put(args *PutArgs, reply *PutReply) error {
	reply.Err = OK
	// Your code here.
	pb.mu.Lock()
	defer pb.mu.Unlock()

	key := args.Key
	value := args.Value
	// 主节点
	if pb.view.Primary != pb.me || atomic.LoadInt32(&pb.alive) == 1 {
		// not the primary
		// 非主节点直接err
		reply.Err = ErrWrongServer
		return nil
	}

	pb.kv[key] = value
	if pb.view.Backup != "" { // 如果有备份，同步到备份节点
		// update backup
		updateArgs := UpdateArgs{Key: key, Value: value}
		updateReply := UpdateReply{}
		// 从节点如果死亡，那么就无法备份，超过三次未成功直接结束
		//  此处死循环，导致无法发送其它请求至 view service
		for i := 1; i <= RetryTimes; i++ {
			ok := call(pb.view.Backup, "PBServer.Update", updateArgs, &updateReply)
			if updateReply.Err != OK || !ok {
				// rpc failed
				log.Debugf("try times: %d update backup err, args: %v, reply: %v", i, updateArgs, updateReply)
			}
			if ok && updateReply.Err == OK {
				log.Debug("update data to backup successful ", pb.view)
				break
			}
			// 这里不能睡眠，因为没有立马更新，导致取数据没有迅速同步
			// time.Sleep(viewservice.PingInterval)
		}
	}

	return nil
}

// Update function to update the put to the backup
func (pb *PBServer) Update(args *UpdateArgs, reply *UpdateReply) error {
	pb.mu.Lock()
	defer pb.mu.Unlock()

	// 从节点更新
	log.Infof("backup:%s update from primary, args: %v, reply: %v", pb.me, args, reply)

	key := args.Key
	value := args.Value

	if pb.view.Backup == pb.me {
		pb.kv[key] = value // 更新
		reply.Err = OK
	} else {
		reply.Err = ErrWrongServer
	}

	return nil
}

// Forward function to forward the kv map to the new backup
func (pb *PBServer) Forward(args *ForwardArgs, reply *ForwardReply) error {
	pb.mu.Lock()
	defer pb.mu.Unlock()

	log.Infof("backup: %s forward from primary, args: %v, reply: %v", pb.me, args, reply)
	// 从节点直接备份，复制 KV
	// 可能视图还未更新
	if pb.view.Backup == pb.me {
		pb.kv = args.KV
		reply.Err = OK
		log.Infof("backup: %s forward from primary successful, args: %v, reply: %v", pb.me, args, reply)
	} else {
		reply.Err = ErrWrongServer
	}

	return nil
}

//
// ping the viewserver periodically.
// if view changed:
//   transition to new view.
//   manage transfer of state from primary to new backup.
//
func (pb *PBServer) tick() {
	// Your code here.
	// if atomic.LoadInt32(&pb.dead) == 1 {
	// 	return
	// }

	pb.mu.Lock()
	defer pb.mu.Unlock()
	// log.Debugf("Server: %s tick, dead: %v", pb.me, pb.dead)
	// ping view server
	view, ok := pb.vs.Ping(pb.view.ViewNum)
	if ok != nil {
		// view server 核心服务，视图服务都挂了，那么跟着挂
		log.Debug("pbservice tick ping error:", pb.me, pb.view)
		atomic.StoreInt32(&pb.alive, 1)
		return
	}
	// ping 只要成功了，当前节点就可以存活
	atomic.StoreInt32(&pb.alive, 0)
	if !view.Equals(&pb.view) {
		log.Debug("update view: ", pb.me, pb.view, view)
		pb.view = view // 更新视图
		// 如果是主节点，且还有从节点，那么需要同步备份节点
		if view.Primary == pb.me && view.Backup != "" {
			// forwards data
			// 视图更新后，副本需要更新KV数据
			forwardArgs := ForwardArgs{KV: pb.kv}
			forwardReply := ForwardReply{}
			for i := 1; i <= RetryTimes; i++ {
				ok := call(pb.view.Backup, "PBServer.Forward", forwardArgs, &forwardReply)
				if forwardReply.Err != OK || !ok {
					// rpc failed
					log.Debugf("try times: %d forward rpc failed, backup: %v, reply: %v", i, pb.view.Backup, forwardReply)
				}
				if ok && forwardReply.Err == OK {
					log.Debug("forward data to backup successful ", view)
					break
				}
				time.Sleep(viewservice.PingInterval)
			}
		}
	}

}

// tell the server to shut itself down.
// please do not change this function.
func (pb *PBServer) kill() {
	log.Debugf("PBServer me: %s killed", pb.me)
	atomic.StoreInt32(&pb.dead, 1)
	pb.l.Close()
}

func StartServer(vshost string, me string) *PBServer {
	pb := new(PBServer)
	pb.me = me
	pb.vs = viewservice.MakeClerk(me, vshost)
	// Your pb.* initializations here.
	pb.view = viewservice.View{}
	pb.view.Primary = ""
	pb.view.Backup = ""
	pb.view.ViewNum = 0
	pb.kv = make(map[string]string)
	pb.oldBackup = ""
	pb.alive = 0
	pb.dead = 0
	pb.unreliable = 0

	rpcSrv := rpc.NewServer()
	rpcSrv.Register(pb)
	log.Debugf("StartServer, view: %s, me: %s", vshost, me)
	os.Remove(pb.me)
	l, e := net.Listen("unix", pb.me)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	pb.l = l

	// please do not change any of the following code,
	// or do anything to subvert it.
	go func(pb *PBServer) {
		for atomic.LoadInt32(&pb.dead) == 0 {
			// log.Debugf("Server: %s still alive, dead: %v", pb.me, pb.dead)
			conn, err := pb.l.Accept()
			if err == nil && atomic.LoadInt32(&pb.dead) == 0 {
				if atomic.LoadInt32(&pb.unreliable) == 1 && (rand.Int63()%1000) < 100 {
					// discard the request. 直接关闭请求
					conn.Close()
				} else if atomic.LoadInt32(&pb.unreliable) == 1 && (rand.Int63()%1000) < 200 {
					// process the request but force discard of reply.
					// 处理请求，但不回复
					c1 := conn.(*net.UnixConn)
					f, _ := c1.File()
					err = syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
					if err != nil {
						log.Debugf("shutdown: %v\n", err)
					}
					go rpcSrv.ServeConn(conn)
				} else {
					go rpcSrv.ServeConn(conn)
				}
			} else if err == nil {
				conn.Close()
			}
			if err != nil && atomic.LoadInt32(&pb.dead) == 0 {
				log.Debugf("PBServer(%v) accept: %v\n", me, err.Error())
				pb.kill()
			}
		}
	}(pb)

	go func(pb *PBServer) {
		for atomic.LoadInt32(&pb.dead) == 0 {
			pb.tick() // tick loop
			time.Sleep(viewservice.PingInterval)
		}
	}(pb)

	return pb
}
