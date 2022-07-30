package pbservice

import (
	"math/rand"
	"net"
	"net/rpc"
	"os"
	"sync"
	"syscall"
	"time"

	"github.com/pedrogao/log"
	"pedrogao/distributed/viewservice"
)

type PBServer struct {
	mu         sync.Mutex
	l          net.Listener
	dead       bool // for testing
	unreliable bool // for testing
	me         string
	vs         *viewservice.Clerk

	// Your declarations here.
	view      viewservice.View  // 视图
	kv        map[string]string // KV数据
	oldBackup string            // 备份
	alive     bool
}

func (pb *PBServer) Get(args *GetArgs, reply *GetReply) error {
	// Your code here.
	pb.mu.Lock()
	defer pb.mu.Unlock()

	log.Infof("server get, args: %v, reply: %v", args, reply)
	// 非主节点，或者节点已死亡，直接 err 返回
	if pb.view.Primary != pb.me || !pb.alive {
		reply.Value = ""
		reply.Err = ErrWrongServer
	} else {
		key := args.Key
		value, ok := pb.kv[key]

		if ok {
			reply.Value = value
			reply.Err = OK
		} else {
			reply.Value = ""
			reply.Err = ErrNoKey
		}

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
	if pb.view.Primary == pb.me {
		pb.kv[key] = value
		if pb.view.Backup != "" { // 如果有备份，同步到备份节点
			// update backup
			updateArgs := UpdateArgs{Key: key, Value: value}
			updateReply := UpdateReply{}
			ok := call(pb.view.Backup, "PBServer.Update", updateArgs, &updateReply)
			for updateReply.Err != OK || ok == false {
				// rpc failed
				// 如果备份失败，则重试，直到成功
				ok = call(pb.view.Backup, "PBServer.Update", updateArgs, &updateReply)
				time.Sleep(viewservice.PingInterval)
			}
		}

	} else {
		// not the primary
		// 非主节点直接err
		reply.Err = ErrWrongServer
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
		pb.kv[key] = value
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

	log.Infof("backup:%s forward from primary", pb.me)
	// 从节点直接备份，复制 KV
	if pb.view.Backup == pb.me {
		pb.kv = args.KV
		reply.Err = OK

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
	pb.mu.Lock()
	defer pb.mu.Unlock()
	// ping view server
	view, ok := pb.vs.Ping(pb.view.ViewNum)
	if ok != nil {
		// view server 核心服务，视图服务都挂了，那么跟着挂
		log.Info("pbservice tick ping error:", pb.me, pb.view)
		pb.alive = false
		return
	}
	// ping 只要成功了，当前节点就可以或者
	pb.alive = true
	if !view.Equals(&pb.view) {
		log.Info("update view: ", pb.view, view)
		pb.view = view // 更新视图
		// 如果是主节点，且还有从节点，那么需要同步备份节点
		if view.Primary == pb.me && view.Backup != "" {
			// forwards data
			// 视图更新后，副本需要更新KV数据
			forwardArgs := ForwardArgs{KV: pb.kv}
			forwardReply := ForwardReply{}
			log.Info("forward data to backup")
			ok := call(pb.view.Backup, "PBServer.Forward", forwardArgs, &forwardReply)
			for forwardReply.Err != OK || ok == false {
				// rpc failed
				log.Info("forward rpc failed", pb.view.Backup, forwardReply)
				ok = call(view.Backup, "PBServer.Forward", forwardArgs, &forwardReply)
				time.Sleep(viewservice.PingInterval) // 休眠一会儿
			}
			/*
				for k, v := range pb.kv {
					// update backup
					updateArgs := UpdateArgs{Key: k, Value: v}
					updateReply := UpdateReply{}
					ok := call(pb.view.Backup, "PBServer.Update", updateArgs, &updateReply)
					for updateReply.Err != OK || ok == false {
						// rpc failed
						ok = call(pb.view.Backup, "PBServer.Update", updateArgs, &updateReply)
						time.Sleep(viewservice.PingInterval)
					}

				}
			*/

		}
	}

}

// tell the server to shut itself down.
// please do not change this function.
func (pb *PBServer) kill() {
	pb.mu.Lock()
	defer pb.mu.Unlock()

	pb.dead = true
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
	pb.alive = true

	rpcSrv := rpc.NewServer()
	rpcSrv.Register(pb)

	os.Remove(pb.me)
	l, e := net.Listen("unix", pb.me)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	pb.l = l

	// please do not change any of the following code,
	// or do anything to subvert it.
	go func() {
		for pb.dead == false {
			conn, err := pb.l.Accept()
			if err == nil && pb.dead == false {
				if pb.unreliable && (rand.Int63()%1000) < 100 {
					// discard the request. 直接关闭请求
					conn.Close()
				} else if pb.unreliable && (rand.Int63()%1000) < 200 {
					// process the request but force discard of reply.
					// 处理请求，但不回复
					c1 := conn.(*net.UnixConn)
					f, _ := c1.File()
					err = syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
					if err != nil {
						log.Infof("shutdown: %v\n", err)
					}
					go rpcSrv.ServeConn(conn)
				} else {
					go rpcSrv.ServeConn(conn)
				}
			} else if err == nil {
				conn.Close()
			}
			if err != nil && pb.dead == false {
				log.Infof("PBServer(%v) accept: %v\n", me, err.Error())
				pb.kill()
			}
		}
	}()

	go func() {
		for pb.dead == false {
			pb.tick() // tick loop
			time.Sleep(viewservice.PingInterval)
		}
	}()

	return pb
}
