package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isLeader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	//	"bytes"
	"sync"
	"sync/atomic"
	"time"

	//	"pedrogao/distributed/labgob"
	"pedrogao/distributed/labrpc"
)

// ApplyMsg
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      any
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type State string

func (s State) String() string {
	return string(s)
}

const (
	Follower  State = "Follower"
	Candidate State = "Candidate"
	Leader    State = "Leader"
)

// Raft
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	state       State         // 节点状态
	currentTerm int           // 当前任期
	votedFor    int           // 给谁投过票
	leaderId    int           // 集群 leader id
	applyCh     chan ApplyMsg // apply message channel
}

// GetState return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Your code here (2A).
	term := rf.currentTerm
	isLeader := rf.leaderId == rf.me
	return term, isLeader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// CondInstallSnapshot
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// Snapshot the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

type AppendEntriesArgs struct {
	Term     int // leader 任期
	LeaderId int // leader ID
}

type AppendEntriesReply struct {
	Term    int  // 回复者任期
	Success bool // 是否同步成功
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	// 发现任期小的，直接拒绝
	if args.Term < rf.currentTerm {
		reply.Success = false
		return
	}
	rf.leaderId = args.LeaderId
	reply.Success = true
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs,
	reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// RequestVoteArgs
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term        int // 请求者任期
	CandidateId int // 请求者Id
}

// RequestVoteReply
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  // 回复者任期，如果回复者任期高，那么请求者成为追随者
	VoteGranted bool // 是否投票
}

// RequestVote
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	// 处理他人的请求投票
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	// 1. 判断任期
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		return
	}
	// 如果你的任期大，那么我就成为 Follower
	if args.Term > rf.currentTerm {
		rf.becomeFollower(args.Term)
	}
	// 2. 判断任期是否可以投票，没有投过票，或者已经给请求者投过票，那么都可以投票
	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId // 更新 votedFor
	}
	// 3. TODO 引入日志后还需判断日志的完整性
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus, there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// Start
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command any) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
}

// Kill
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heart beats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		// Follower 在一定时间内未收到心跳，那么就会超时成为 Candidate，发起选举
		time.Sleep(getRandomElectTimeout())
		// 加锁
		rf.mu.Lock()
		if rf.state == Leader {
			// leader 无需发起选举
			rf.mu.Unlock()
			continue // 跳过本次超时，不能直接退出函数，因为 leader 可能下一秒就不是 leader 了
		}
		rf.becomeCandidate()
		DPrintf("peer: %d become candidate, and begin to vote", rf.me)
		var votes uint32 = 1 // 自己投给自己的一票
		for peerId := range rf.peers {
			if peerId == rf.me {
				continue
			}
			go rf.sendRequestVoteToPeer(peerId, &votes)
		}

		rf.mu.Unlock()
	}
}

// 给请求其它节点投票
func (rf *Raft) sendRequestVoteToPeer(peerId int, votes *uint32) {
	// 得到 args 得加锁
	rf.mu.Lock()
	args := &RequestVoteArgs{
		Term:        rf.currentTerm,
		CandidateId: rf.me,
	}
	rf.mu.Unlock()

	reply := &RequestVoteReply{}
	ok := rf.sendRequestVote(peerId, args, reply)
	if !ok {
		DPrintf("sendRequestVote err, args: %v, reply: %v", *args, *reply)
		return
	}

	// 处理请求结果
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("peer: %d send vote to peer: %d, args: %v, reply: %v", rf.me, peerId, *args, *reply)
	// 如果在发送选票期间，state 发生了变化，或者任期发生了变化，那么这次投票无效
	if rf.state != Candidate || args.Term != rf.currentTerm {
		return
	}

	if reply.Term > rf.currentTerm {
		// 发现任期大的，成为 Follower，然后返回
		rf.becomeFollower(reply.Term)
		return
	}
	// 没有收到投票
	if !reply.VoteGranted {
		DPrintf("peer: %d send RequestVote to %d but not received vote", rf.me, peerId)
		return
	}
	// 收到了投票
	currentVotes := atomic.AddUint32(votes, 1)
	// 获得了超过半数的投票，那么成为 leader
	// 如果 len(peers) = 3 / 2 = 1 + 1 = 2，那么至少应该得到 2 票
	if int(currentVotes) >= (len(rf.peers)/2 + 1) {
		rf.becomeLeader()
	}
}

//
// state operations
//

func (rf *Raft) becomeFollower(term int) {
	rf.currentTerm = term
	rf.state = Follower
	rf.votedFor = -1 // 追随者重置投票
}

func (rf *Raft) becomeLeader() {
	rf.state = Leader
	rf.leaderId = rf.me
	go rf.appendEntriesLoop()
}

func (rf *Raft) becomeCandidate() {
	rf.currentTerm += 1 // 任期+1
	rf.state = Candidate
	rf.votedFor = rf.me // 给自己投票
}

//
// loop
//

// leader 独有 loop，发送日志、心跳
func (rf *Raft) appendEntriesLoop() {
	for !rf.killed() {
		// 非 leader 直接退出，防止 leader 突然遇到大的任期后成为 Follower
		rf.mu.Lock()
		if rf.state != Leader {
			rf.mu.Unlock()
			return
		}

		for peerId := range rf.peers {
			if peerId == rf.me { // 跳过自己
				continue
			}
			go rf.sendAppendEntriesToPeer(peerId)
		}
		rf.mu.Unlock()
		time.Sleep(heartbeatInterval)
	}
}

// 应用日志 loop
func (rf *Raft) applyLogLoop() {
	for !rf.killed() {
		time.Sleep(applyInterval)
	}
}

func (rf *Raft) sendAppendEntriesToPeer(peerId int) {
	rf.mu.Lock()
	args := &AppendEntriesArgs{
		Term:     rf.currentTerm,
		LeaderId: rf.me,
	}
	rf.mu.Unlock()
	reply := &AppendEntriesReply{}
	ok := rf.sendAppendEntries(peerId, args, reply)
	if !ok {
		DPrintf("leader: %d send append entries to peer: %d fail, args: %v, reply: %v",
			rf.me, peerId, *args, *reply)
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("leader: %d send entries to peer: %d, args: %v, reply: %v", rf.me, peerId, *args, *reply)
	// 如果不再是 leader，或者任期变了，那么就不再处理这次同步了
	if rf.state != Leader || rf.currentTerm != args.Term {
		DPrintf("leader: %d handle AppendEntries mismatched", rf.me)
		return
	}
	// 发现任期大的，立马认怂
	if reply.Term > rf.currentTerm {
		rf.becomeFollower(reply.Term)
		return
	}
	// 同步未成功
	if !reply.Success {
		return
	}
	// TODO 同步成功后，需要更新 commitIndex、applyIndex
}

// Make
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.leaderId = -1   // -1 表示暂时没有 leader
	rf.votedFor = -1   // -1 表示没有投过票
	rf.currentTerm = 0 // 初始任期为0
	rf.applyCh = applyCh
	rf.state = Follower // 初始化为 Follower

	// initialize from state persisted before a crash
	// 读取持久化数据可能会改变任期、投票等数据
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
