package pbservice

import (
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/rpc"
	"os"
	"sync"
	"syscall"
	"time"

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

	view      viewservice.View
	kv        map[string]string
	oldBackup string
	alive     bool
}

func (pb *PBServer) Get(args *GetArgs, reply *GetReply) error {

	// Your code here.
	//pb.mu.Lock()
	//defer pb.mu.Unlock()
	//fmt.Println("server get: ", pb.view, pb.me, pb.alive)
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

	if pb.view.Primary == pb.me {
		pb.kv[key] = value

		if pb.view.Backup != "" {
			//update backup
			updateArgs := UpdateArgs{Key: key, Value: value}
			updateReply := UpdateReply{}
			ok := call(pb.view.Backup, "PBServer.Update", updateArgs, &updateReply)
			for updateReply.Err != OK || ok == false {
				//rpc failed
				ok = call(pb.view.Backup, "PBServer.Update", updateArgs, &updateReply)
				time.Sleep(viewservice.PingInterval)
			}
		}

	} else {
		// not the primary
		reply.Err = ErrWrongServer
	}

	return nil
}

// Update function to update the put to the backup
func (pb *PBServer) Update(args *UpdateArgs, reply *UpdateReply) error {

	pb.mu.Lock()
	defer pb.mu.Unlock()

	key := args.Key
	value := args.Value

	if pb.view.Backup == pb.me {
		pb.kv[key] = value
		reply.Err = OK
	} else {
		reply.Err = ErrWrongServer
	}

	//fmt.Println("pbserver update", pb.me, pb.kv)

	return nil
}

// Forward function to forward the kv map to the new backup
func (pb *PBServer) Forward(args *ForwardArgs, reply *ForwardReply) error {

	pb.mu.Lock()
	defer pb.mu.Unlock()

	if pb.view.Backup == pb.me {
		pb.kv = args.KV
		reply.Err = OK

	} else {
		reply.Err = ErrWrongServer
	}

	//fmt.Println("pbserver forward:", pb.me, pb.kv)

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
	//pb.mu.Lock()
	//defer pb.mu.Unlock()

	view, ok := pb.vs.Ping(pb.view.Viewnum)
	if ok != nil {
		//fmt.Println("pbservice tick: ping error:", pb.me, pb.view, pb.kv)
		pb.alive = false
		return
	}

	pb.alive = true
	//pb.vs.Ping(pb.view.Viewnum)
	if view != pb.view {
		pb.view = view
		if view.Primary == pb.me && view.Backup != "" {
			//fmt.Println("server tick primary:", pb.me, pb.kv)
			//fmt.Println("server tick backup:", pb.view.Backup)

			//forwards data

			forwardArgs := ForwardArgs{KV: pb.kv}
			forwardReply := ForwardReply{}

			//fmt.Println("server tick: forword", forwardArgs.kv)
			ok := call(pb.view.Backup, "PBServer.Forward", forwardArgs, &forwardReply)
			for forwardReply.Err != OK || ok == false {
				//rpc failed
				//fmt.Println("server tick: rpc failed", pb.view.Backup, forwardReply, ok)
				ok = call(view.Backup, "PBServer.Forward", forwardArgs, &forwardReply)
				time.Sleep(viewservice.PingInterval)
			}

			/*
				for k, v := range pb.kv {
					//update backup
					updateArgs := UpdateArgs{Key: k, Value: v}
					updateReply := UpdateReply{}
					ok := call(pb.view.Backup, "PBServer.Update", updateArgs, &updateReply)
					for updateReply.Err != OK || ok == false {
						//rpc failed
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
	pb.view.Viewnum = 0
	pb.kv = make(map[string]string)
	pb.oldBackup = ""
	pb.alive = true

	rpcs := rpc.NewServer()
	rpcs.Register(pb)

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
					// discard the request.
					conn.Close()
				} else if pb.unreliable && (rand.Int63()%1000) < 200 {
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
				conn.Close()
			}
			if err != nil && pb.dead == false {
				fmt.Printf("PBServer(%v) accept: %v\n", me, err.Error())
				pb.kill()
			}
		}
	}()

	go func() {
		for pb.dead == false {
			pb.tick()
			time.Sleep(viewservice.PingInterval)
		}
	}()

	return pb
}
