package viewservice

import (
	"fmt"
	"log"
	"net"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type ViewServer struct {
	mu   sync.Mutex
	l    net.Listener
	dead bool
	me   string

	// Your declarations here.
	clients    map[string]time.Time
	view       View
	primaryAck bool
	volunteer  string
}

// Ping
// server Ping RPC handler.
//
func (vs *ViewServer) Ping(args *PingArgs, reply *PingReply) error {

	// Your code here.
	me := args.Me
	viewNum := args.Viewnum

	//set time
	vs.mu.Lock()
	defer vs.mu.Unlock()

	//fmt.Println("server ping start", me, vs.view)

	//the first ping from server
	if viewNum == 0 && vs.view.Primary == "" {

		vs.view.Primary = me
		vs.primaryAck = false
		vs.view.Viewnum++
		reply.View = vs.view

		vs.clients[me] = time.Now()
		return nil
	}

	//the first ping from backup
	if vs.view.Backup == "" && vs.primaryAck && vs.view.Primary != me {

		//primary acked
		vs.view.Backup = me
		vs.view.Viewnum++
		vs.primaryAck = false
		reply.View = vs.view

		vs.clients[me] = time.Now()
		return nil
	}

	//idle server
	if me != vs.view.Backup && me != vs.view.Primary && viewNum == 0 {

		//add new idle server
		vs.volunteer = me
		//fmt.Println("server Ping", vs.volunteers)
		vs.clients[me] = time.Now()
		return nil
	}

	//ack from primary
	if me == vs.view.Primary && !vs.primaryAck && viewNum == vs.view.Viewnum {

		vs.primaryAck = true
		reply.View = vs.view

		vs.clients[me] = time.Now()
		return nil
	}

	//primary restart
	if me == vs.view.Primary && viewNum == 0 && vs.primaryAck == true {

		vs.view.Primary = vs.view.Backup
		vs.view.Backup = me
		vs.view.Viewnum++
		vs.primaryAck = false
		reply.View = vs.view

		vs.clients[me] = time.Now()
		return nil
	}

	//default

	/*
		if me == vs.view.Primary && vs.primaryAck {
			reply.View = vs.view

			return nil
		}

		if me == vs.view.Backup && vs.primaryAck {
			reply.View = vs.view

			return nil
		}
	*/
	//fmt.Println("sever ping default:", me, vs.view)
	vs.clients[me] = time.Now()
	reply.View = vs.view

	//ping fail
	return nil
}

// Get
// server Get() RPC handler.
//
func (vs *ViewServer) Get(args *GetArgs, reply *GetReply) error {

	// Your code here.
	reply.View = vs.view

	return nil
}

//
// tick() is called once per PingInterval; it should notice
// if servers have died or recovered, and change the view
// accordingly.
//
func (vs *ViewServer) tick() {

	// Your code here.

	now := time.Now()

	vs.mu.Lock()
	defer vs.mu.Unlock()

	if vs.view.Primary == "" {
		return
	}

	//primary die
	pingTime, _ := vs.clients[vs.view.Primary]
	if now.Sub(pingTime) > (DeadPings * PingInterval) {
		//fmt.Println("server primary timeout", vs.view)

		//just wait
		if vs.primaryAck == false {
			return
		}

		// has backup
		if vs.view.Backup != "" {
			vs.view.Primary = vs.view.Backup
			vs.view.Viewnum++
			vs.primaryAck = false

			vs.view.Backup = ""
			//make a volunteer become a backup
			if vs.volunteer != "" {
				volTime, _ := vs.clients[vs.volunteer]
				if now.Sub(volTime) < PingInterval {

					vs.view.Backup = vs.volunteer
					vs.volunteer = ""
					//fmt.Println("server tick", vs.view)
				}
			}
		}

	}

	if vs.view.Backup == "" {
		return
	}

	//backup dies
	pingTime, _ = vs.clients[vs.view.Backup]
	if now.Sub(pingTime) > (DeadPings * PingInterval) {
		vs.view.Backup = ""
		vs.view.Viewnum++
		vs.primaryAck = false

		//fmt.Println("server backup timeout", vs.volunteers)
		//make a volunteer become a backup
		if vs.volunteer != "" {
			volTime, _ := vs.clients[vs.volunteer]
			if now.Sub(volTime) < PingInterval {

				vs.view.Backup = vs.volunteer
				vs.volunteer = ""
				//fmt.Println("server tick", vs.view)
			}
		}

	}
}

// Kill
// tell the server to shut itself down.
// for testing.
// please don't change this function.
//
func (vs *ViewServer) Kill() {
	vs.dead = true
	vs.l.Close()
}

func StartServer(me string) *ViewServer {
	vs := new(ViewServer)
	vs.me = me
	// Your vs.* initializations here.

	vs.clients = make(map[string]time.Time)
	vs.view = View{}
	vs.view.Viewnum = 0
	vs.view.Primary = ""
	vs.view.Backup = ""
	vs.volunteer = ""
	vs.primaryAck = false

	// tell net/rpc about our RPC server and handlers.
	rpcs := rpc.NewServer()
	rpcs.Register(vs)

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
	go func() {
		for vs.dead == false {
			conn, err := vs.l.Accept()
			if err == nil && vs.dead == false {
				go rpcs.ServeConn(conn)
			} else if err == nil {
				conn.Close()
			}
			if err != nil && vs.dead == false {
				fmt.Printf("ViewServer(%v) accept: %v\n", me, err.Error())
				vs.Kill()
			}
		}
	}()

	// create a thread to call tick() periodically.
	go func() {
		for vs.dead == false {
			vs.tick()
			time.Sleep(PingInterval)
		}
	}()

	return vs
}
