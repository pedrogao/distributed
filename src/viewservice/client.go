package viewservice

import (
	"fmt"
	"net/rpc"
)

// Clerk
// the views service Clerk lives in the client
// and maintains a little state.
type Clerk struct {
	me     string // client's name (host:port)
	server string // view service's host:port
}

func MakeClerk(me string, server string) *Clerk {
	ck := new(Clerk)
	ck.me = me
	ck.server = server
	return ck
}

func call(srv string, rpcname string,
	args any, reply any) bool {
	c, err := rpc.Dial("unix", srv)
	if err != nil {
		return false
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}
	return false
}

func (ck *Clerk) Ping(viewNum uint) (View, error) {
	// prepare the arguments.
	args := &PingArgs{
		Me:      ck.me,
		ViewNum: viewNum,
	}
	var reply PingReply

	// send an RPC request, wait for the reply.
	ok := call(ck.server, "ViewServer.Ping", args, &reply)
	if !ok {
		return View{}, fmt.Errorf("ping(%v) failed", viewNum)
	}

	return reply.View, nil
}

func (ck *Clerk) Get() (View, bool) {
	args := &GetArgs{}
	var reply GetReply
	ok := call(ck.server, "ViewServer.Get", args, &reply)
	if ok == false {
		return View{}, false
	}
	return reply.View, true
}

func (ck *Clerk) Primary() string {
	v, ok := ck.Get()
	if ok {
		return v.Primary
	}
	return ""
}
