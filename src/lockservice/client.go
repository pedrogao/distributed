package lockservice

import (
	"net/rpc"

	"github.com/pedrogao/log"
)

// Clerk
// the lock service Clerk lives in the client
// and maintains a little state.
type Clerk struct {
	servers [2]string // primary port, backup port
	// Your definitions here.
	single bool
}

func MakeClerk(primary string, backup string) *Clerk {
	ck := new(Clerk)
	ck.servers[0] = primary
	ck.servers[1] = backup
	// Your initialization code here.
	// 用于主从备份
	if backup == "" {
		ck.single = true
	}
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

// Lock
// ack the lock service for a lock.
// returns true if the lock service
// granted the lock, false otherwise.
//
// you will have to modify this function.
func (ck *Clerk) Lock(lockName string) bool {
	// prepare the arguments.
	number := nrand()
	args := &LockArgs{
		LockName: lockName,
		Number:   number,
	}
	var reply LockReply

	// send an RPC request, wait for the reply.
	ok := call(ck.servers[0], "LockServer.Lock", args, &reply)
	if !ok {
		log.Debugf("Lock primary fail, try lock backup, args: %v, reply: %v", args, reply)
		// primary 未响应，请求 backup
		ok = call(ck.servers[1], "LockServer.Lock", args, &reply)
		// backup 仍未响应，返回 false
		if !ok {
			log.Debugf("Lock backup fail, args: %v, reply: %v", args, reply)
			return false
		}
	}
	if !reply.OK {
		log.Errorf("Lock failed, err: %s", reply.Err)
	}
	log.Debugf("Lock successful, args: %v, reply: %v", args, reply)
	return reply.OK
}

// Unlock ask the lock service to unlock a lock.
// returns true if the lock was previously held,
// false otherwise.
func (ck *Clerk) Unlock(lockName string) bool {
	// Your code here.
	number := nrand()
	args := &UnlockArgs{
		LockName: lockName,
		Number:   number,
	}
	reply := &UnlockReply{}

	ok := call(ck.servers[0], "LockServer.Unlock", args, &reply)
	if !ok {
		log.Debugf("Unlock primary fail, try unlock backup, args: %v, reply: %v", args, reply)
		// primary 未响应，请求 backup
		ok = call(ck.servers[1], "LockServer.Unlock", args, &reply)
		// backup 仍未响应，返回 false
		if !ok {
			log.Debugf("Unlock backup fail, args: %v, reply: %v", args, reply)
			return false
		}
	}
	if !reply.OK {
		log.Errorf("Unlock failed, err: %s", reply.Err)
	}
	log.Debugf("Unlock successful, args: %v, reply: %v", args, reply)
	return reply.OK
}

// Update primary lock to back up
func (ck *Clerk) Update(lock string, number int64, val bool) bool {
	// 无法备份
	if !ck.single {
		return false
	}

	args := &UpdateArgs{
		Lock:   lock,
		Val:    val,
		Number: number,
	}
	reply := &UpdateReply{}
	ok := call(ck.servers[0], "LockServer.Update", args, &reply)
	if ok == false {
		log.Errorf("call Update RPC err")
		return false
	}
	if reply.Err != "" {
		log.Errorf("Update RPC err: %s", reply.Err)
	}
	log.Debugf("Update successful, args: %v, reply: %v", args, reply)
	return true
}
