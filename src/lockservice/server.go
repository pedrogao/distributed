package lockservice

import (
	"fmt"
	"io"
	"net"
	"net/rpc"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pedrogao/log"
)

type version struct {
	Seqno  int64 // 序号
	Val    bool  // 值
	Backup bool  // 是否备份
}

type LockServer struct {
	mu    sync.Mutex
	l     net.Listener
	dead  int32 // for test_test.go
	dying bool  // for test_test.go

	amPrimary bool   // am I the primary?
	backup    string // backup's port
	me        string

	// for each lock name, is it locked?
	locks map[string][]*version

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
	// 如果是客户端发送的同一次请求，那么返回 true
	// case1：主备之间，主库挂了，但已经同步到了从库
	// case2：客户端请求丢包，二次请求了
	// case3: 客户端对 a 加锁、且已经备份到了从库，然后主库挂了，那么客户端对从库加锁，那么应该返回 false
	vector := ls.locks[args.LockName]
	idx := versionIndex(vector, args.Number)
	// idx >= 0 表示 value 是以前的版本，且已加锁
	if idx >= 0 && vector[idx].Val {
		log.Debugf("%s Handle Lock vector, vector: %v, idx: %d, number: %v", ls.me, vector, idx, args.Number)
		reply.OK = true
		return nil
	}
	// 旧版本，且旧版本没有加锁
	if idx >= 0 && !vector[idx].Val {
		log.Debugf("%s Handle Lock vector fail, args: %v, reply: %v", ls.me, args, reply)
		reply.OK = false
		reply.Err = ErrLockFailed
		return nil
	}
	// 当前版本，取最近的 version
	if len(vector) > 0 {
		bigger := biggerVersionIndex(vector, args.Number)
		var currentVersion *version
		if bigger >= 0 {
			currentVersion = vector[bigger]
		} else {
			currentVersion = vector[len(vector)-1]
		}
		if currentVersion.Val {
			log.Debugf("%s Handle Lock fail, args: %v, reply: %v", ls.me, args, reply)
			reply.OK = false
			reply.Err = ErrLockFailed
			return nil
		}
	}
	// 主节点需要向从节点备份
	if ls.amPrimary {
		log.Debugf("%s Update backup, args: %v", ls.me, args)
		for i := 1; i <= RetryTimes; i++ {
			if ok := ls.client.Update(args.LockName, args.Number, true); ok {
				break
			}
			if i == RetryTimes {
				log.Errorf("%s update backup fail, times: %d", ls.me, RetryTimes)
			}
		}
	}
	// 加锁成功
	reply.OK = true
	if _, ok := ls.locks[args.LockName]; !ok {
		ls.locks[args.LockName] = []*version{{
			Seqno: args.Number,
			Val:   true,
		}}
	} else {
		ls.locks[args.LockName] = append(ls.locks[args.LockName], &version{
			Seqno: args.Number,
			Val:   true,
		})
	}
	log.Debugf("%s Handle Lock successful, args: %v, reply: %v", ls.me, args, reply)
	return nil
}

// Unlock
// server Unlock RPC handler.
func (ls *LockServer) Unlock(args *UnlockArgs, reply *UnlockReply) error {
	// Your code here.
	ls.mu.Lock()
	defer ls.mu.Unlock()

	_, ok := ls.locks[args.LockName]
	if !ok {
		reply.OK = false
		reply.Err = ErrNoSuckLock
		log.Debugf("%s Handle Unlock fail, no suck lock, args: %v, reply: %v", ls.me, args, reply)
		return nil
	}
	// case2：备库，且 seqno 已经存在于旧版本，那么新的 unlock 不会更新值，但仍返回成功。 核心点：保存旧版本的 seqno
	vector, ok := ls.locks[args.LockName]
	idx := versionIndex(vector, args.Number) // 旧版本
	// idx >= 0 表示 value 是以前的版本，且已加锁
	if idx >= 0 && !vector[idx].Val {
		log.Debugf("%s Handle Unlock vector, ok: %v, vector: %v, idx: %d, number: %v", ls.me, ok, vector, idx, args.Number)
		reply.OK = true
		return nil
	}
	// 旧版本，且旧版本没有加锁
	if idx >= 0 && vector[idx].Val {
		log.Debugf("%s Handle Unlock fail in vector, args: %v, reply: %v", ls.me, args, reply)
		reply.OK = false
		reply.Err = ErrUnlockFailed
		return nil
	}
	// 当前版本，取最近的 version
	if len(vector) > 0 {
		// 如果时间戳大的新版本，那么已最近的版本为主
		bigger := biggerVersionIndex(vector, args.Number)
		var currentVersion *version
		if bigger >= 0 {
			currentVersion = vector[bigger]
		} else {
			currentVersion = vector[len(vector)-1]
		}
		if !currentVersion.Val {
			log.Debugf("%s Handle Unlock fail, current: %v, args: %v, reply: %v",
				ls.me, *currentVersion, args, reply)
			reply.OK = false
			reply.Err = ErrUnlockFailed
			return nil
		}
	}
	// 同步请求
	if ls.amPrimary {
		log.Debugf("%s Update backup, args: %v", ls.me, args)
		for i := 1; i <= RetryTimes; i++ {
			if ok := ls.client.Update(args.LockName, args.Number, false); ok {
				break
			}
			if i == RetryTimes {
				log.Errorf("%s update backup fail, times: %d", ls.me, RetryTimes)
			}
		}
	}
	// 解锁成功
	if _, ok := ls.locks[args.LockName]; !ok {
		ls.locks[args.LockName] = []*version{{
			Seqno: args.Number,
			Val:   false,
		}}
	} else {
		ls.locks[args.LockName] = append(ls.locks[args.LockName], &version{
			Seqno: args.Number,
			Val:   false,
		})
	}

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

	if _, ok := ls.locks[args.Lock]; !ok {
		ls.locks[args.Lock] = []*version{{
			Seqno:  args.Number,
			Val:    args.Val,
			Backup: true,
		}}
	} else {
		ls.locks[args.Lock] = append(ls.locks[args.Lock], &version{
			Seqno:  args.Number,
			Val:    args.Val,
			Backup: true,
		})
	}
	log.Debugf("%s Handle Update successful, args: %v, reply: %v", ls.me, args, reply)
	return nil
}

//
// tell the server to shut itself down.
// for testing.
// please don't change this.
//
func (ls *LockServer) kill() {
	atomic.StoreInt32(&ls.dead, 1)
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
	// skip write, same as discard reply
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
	ls.locks = map[string][]*version{}
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
		for atomic.LoadInt32(&ls.dead) == 0 {
			conn, err := ls.l.Accept()
			// 无 err，且无 dead
			if err == nil && atomic.LoadInt32(&ls.dead) == 0 {
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

					// ls.dead = true // 下次直接关闭
					atomic.StoreInt32(&ls.dead, 1)
				} else {
					go rpcSrv.ServeConn(conn) // 正常RPC服务
				}
			} else if err == nil { // dead
				conn.Close()
			}
			// 出现 err
			if err != nil && atomic.LoadInt32(&ls.dead) == 0 {
				fmt.Printf("LockServer(%v) accept: %v\n", me, err.Error())
				ls.kill()
			}
		}
	}()

	return ls
}
