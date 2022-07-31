package lockservice

import (
	"fmt"
	"io"
	"net"
	"net/rpc"
	"os"
	"sync"
	"time"

	"github.com/pedrogao/log"
)

type LockServer struct {
	mu    sync.Mutex
	l     net.Listener
	dead  bool // for test_test.go
	dying bool // for test_test.go

	amPrimary bool   // am I the primary?
	backup    string // backup's port
	me        string

	// for each lock name, is it locked?
	locks      map[string]bool
	backupInfo map[string]bool // 是否备份而来

	client *Clerk
}

// Lock
// server Lock RPC handler.
//
// you will have to modify this function
// If a Lock(lockname) request arrives at the service and the named lock is not held,
// or has never been used before, the lock service should remember that the lock is held,
// and return a successful reply to the client.
func (ls *LockServer) Lock(args *LockArgs, reply *LockReply) error {
	ls.mu.Lock()
	defer ls.mu.Unlock()

	locked, _ := ls.locks[args.LockName]

	// 如果是客户端发送的同一次请求，那么返回 true
	// case1：主备之间，主库挂了，但已经同步到了从库
	// case2：客户端请求丢包，二次请求了
	if locked && ls.backupInfo[args.LockName] {
		log.Debugf("%s Handle Lock successful in backup, args: %v, reply: %v", ls.me, args, reply)
		reply.OK = true
		return nil
	}

	if locked {
		log.Debugf("%s Handle Lock fail, args: %v, reply: %v", ls.me, args, reply)
		reply.OK = false
		reply.Err = ErrLockFailed
		return nil
	}

	// 主节点需要向从节点备份
	if ls.amPrimary {
		log.Debugf("%s Update backup, args: %v", ls.me, args)
		for i := 1; i <= RetryTimes; i++ {
			if ok := ls.client.Update(args.LockName, args.Number, true); ok {
				break
			}
			if i == RetryTimes {
				log.Error("%s update backup fail, times: %d", ls.me, RetryTimes)
				reply.OK = false
				reply.Err = ErrBackup
				return nil
			}
		}
	}
	// 加锁成功
	reply.OK = true
	ls.locks[args.LockName] = true
	log.Debugf("%s Handle Lock successful, args: %v, reply: %v", ls.me, args, reply)
	return nil
}

// Unlock
// server Unlock RPC handler.
func (ls *LockServer) Unlock(args *UnlockArgs, reply *UnlockReply) error {
	// Your code here.
	ls.mu.Lock()
	defer ls.mu.Unlock()

	locked, ok := ls.locks[args.LockName]
	if !ok {
		reply.OK = false
		reply.Err = ErrNoSuckLock
		log.Debugf("%s Handle Unlock fail, no suck lock, args: %v, reply: %v", ls.me, args, reply)
		return nil
	}
	// case1：主库解锁成功，然后同步到从库，但是主库的ack被丢失了，因此客户端会再次请求从库，此时从库返回 true
	if !locked && ls.backupInfo[args.LockName] {
		reply.OK = true
		log.Debugf("%s Handle Unlock successful, unlock twice in pb, args: %v, reply: %v", ls.me, args, reply)
		return nil
	}
	// 未加锁过
	if !locked {
		reply.OK = false
		reply.Err = ErrUnlockFailed
		log.Debugf("%s Handle Unlock fail, invalid lock, args: %v, reply: %v", ls.me, args, reply)
		return nil
	}
	// 同步请求
	if ls.amPrimary {
		log.Debugf("%s Update backup, args: %v", ls.me, args)
		for i := 1; i <= RetryTimes; i++ {
			if ok := ls.client.Update(args.LockName, args.Number, false); ok {
				break
			}
			if i == RetryTimes {
				reply.OK = false
				reply.Err = ErrBackup
				log.Errorf("%s update backup fail, times: %d", ls.me, RetryTimes)
				return nil
			}
		}
	}
	// 解锁成功
	ls.locks[args.LockName] = false
	reply.OK = true
	log.Debugf("%s Handle Unlock successful, args: %v, reply: %v", ls.me, args, reply)
	return nil
}

// Update primary-backup
func (ls *LockServer) Update(args *UpdateArgs, reply *UpdateReply) error {
	// Your code here.
	ls.mu.Lock()
	defer ls.mu.Unlock()

	if ls.amPrimary {
		log.Debugf("%s Handle Update fail, invalid primary, args: %v, reply: %v", ls.me, args, reply)
		reply.Err = ErrTryUpdatePrimary
		return nil
	}

	ls.locks[args.Lock] = args.Val
	ls.backupInfo[args.Lock] = true
	log.Debugf("%s Handle Update successful, args: %v, reply: %v", ls.me, args, reply)
	return nil
}

//
// tell the server to shut itself down.
// for testing.
// please don't change this.
//
func (ls *LockServer) kill() {
	ls.mu.Lock()
	defer ls.mu.Unlock()

	ls.dead = true
	ls.l.Close()
}

// DeafConn
// hack to allow test_test.go to have primary process
// an RPC but not send a reply. can't use the shutdown()
// trick b/c that causes client to immediately get an
// error and send to back up before primary does.
// please don't change anything to do with DeafConn.
type DeafConn struct {
	c io.ReadWriteCloser
}

func (dc DeafConn) Write(p []byte) (n int, err error) {
	return len(p), nil
}
func (dc DeafConn) Close() error {
	return dc.c.Close()
}
func (dc DeafConn) Read(p []byte) (n int, err error) {
	return dc.c.Read(p)
}

func StartServer(primary string, backup string, amPrimary bool) *LockServer {
	ls := new(LockServer)
	ls.backup = backup
	ls.amPrimary = amPrimary
	ls.locks = map[string]bool{}
	ls.backupInfo = map[string]bool{}

	// Your initialization code here.

	me := ""
	if amPrimary {
		me = primary
		ls.client = MakeClerk(backup, "")
	} else {
		me = backup
	}
	ls.me = me

	// tell net/rpc about our RPC server and handlers.
	rpcSrv := rpc.NewServer()
	rpcSrv.Register(ls)

	// prepare to receive connections from clients.
	// change "unix" to "tcp" to use over a network.
	os.Remove(me) // only needed for "unix"
	l, e := net.Listen("unix", me)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	ls.l = l

	// please don't change any of the following code,
	// or do anything to subvert it.
	// create a thread to accept RPC connections from clients.
	go func() {
		for ls.dead == false {
			conn, err := ls.l.Accept()
			// 无 err，且无 dead
			if err == nil && ls.dead == false {
				if ls.dying {
					// process the request but force discard of reply.

					// without this the connection is never closed,
					// b/c ServeConn() is waiting for more requests.
					// test_test.go depends on this two seconds.
					go func() {
						time.Sleep(2 * time.Second) // 等待2s关闭
						conn.Close()
					}()
					ls.l.Close()

					// this object has the type ServeConn expects,
					// but discards writes (i.e. discards the RPC reply).
					deafConn := DeafConn{c: conn}
					rpcSrv.ServeConn(deafConn)

					ls.dead = true
				} else {
					go rpcSrv.ServeConn(conn) // 正常RPC服务
				}
			} else if err == nil { // dead
				conn.Close()
			}
			// 出现 err
			if err != nil && ls.dead == false {
				fmt.Printf("LockServer(%v) accept: %v\n", me, err.Error())
				ls.kill()
			}
		}
	}()

	return ls
}
