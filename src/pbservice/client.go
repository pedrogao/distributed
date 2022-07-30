package pbservice

import (
	"net/rpc"
	"time"

	"pedrogao/distributed/viewservice"
)

// You'll probably need to uncomment this:

type Clerk struct {
	vs *viewservice.Clerk
}

func MakeClerk(vshost string, me string) *Clerk {
	ck := new(Clerk)
	ck.vs = viewservice.MakeClerk(me, vshost)
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

// Get
// fetch a key's value from the current primary;
// if they key has never been set, return "".
// Get() must keep trying until it either the
// primary replies with the value or the primary
// says the key doesn't exist (has never been Put().
func (ck *Clerk) Get(key string) string {
	// Your code here.
	args := GetArgs{Key: key}
	reply := GetReply{}

	ok := call(ck.vs.Primary(), "PBServer.Get", args, &reply)
	for (reply.Err != OK && reply.Err != ErrNoKey) || ok == false {
		// rpc failed
		ok = call(ck.vs.Primary(), "PBServer.Get", args, &reply)
		time.Sleep(viewservice.PingInterval)
	}

	return reply.Value
}

// Put
// tell the primary to update key's value.
// must keep trying until it succeeds.
func (ck *Clerk) Put(key string, value string) {
	// Your code here.
	args := PutArgs{Key: key, Value: value}
	reply := PutReply{}

	ok := call(ck.vs.Primary(), "PBServer.Put", args, &reply)
	for reply.Err != OK || ok == false {
		//rpc failed
		ok = call(ck.vs.Primary(), "PBServer.Put", args, &reply)
		time.Sleep(viewservice.PingInterval)
	}

}
