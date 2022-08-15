package raft

import (
	"bytes"
	"sync"
	"sync/atomic"
	"time"

	"pedrogao/distributed/labgob"
	"pedrogao/distributed/labrpc"
)

type ApplyMsg struct {
	CommandValid bool
	Command      any
	CommandIndex int
	CommandTerm  int // 3A

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

type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	// 2A
	state       State         // 节点状态
	currentTerm int           // 当前任期
	votedFor    int           // 给谁投过票
	leaderId    int           // 集群 leader id
	applyCh     chan ApplyMsg // apply message channel
	// 2B
	log                    rLog      // 日志
	lastReceivedFromLeader time.Time // 上一次收到 leader 请求的时间
	nextIndex              []int     // 下一个待发送日志序号，leader 特有
	matchIndex             []int     // 已同步日志序号，leader 特有
	commitIndex            int       // 已提交日志序号
	lastApplied            int       // 已应用日志序号
	// 2D
	snapshot []byte
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

func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var (
		currentTerm int
		votedFor    int
		log         rLog
	)
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&log) != nil {
		DPrintf("decode persisted state err.")
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = log
	}
}

// 持久化数据，并存储快照
func (rf *Raft) persistStateAndSnapshot(snapshot []byte) {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveStateAndSnapshot(data, snapshot)
}

// InstallSnapshotArgs 快照
type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}
type InstallSnapshotReply struct {
	Term int
}

// InstallSnapshot peer 接受 leader InstallSnapshot 请求
func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		return
	}
	// Send the entire snapshot in a single InstallSnapshot RPC.
	// Don't implement Figure 13's offset mechanism for splitting up the snapshot.
	if args.Term > rf.currentTerm {
		rf.becomeFollower(args.Term)
		rf.persist()
	}
	if rf.state != Follower {
		rf.becomeFollower(args.Term)
		rf.persist()
	}
	rf.leaderId = args.LeaderId
	rf.lastReceivedFromLeader = time.Now()
	// 拒绝，如果你的小，证明我已经快照过了，无需再次快照
	if args.LastIncludedIndex <= rf.log.LastIncludedIndex {
		return
	}
	msg := ApplyMsg{
		SnapshotValid: true,
		Snapshot:      args.Data,
		SnapshotTerm:  args.LastIncludedTerm,
		SnapshotIndex: args.LastIncludedIndex,
	}
	go func() {
		// 应用快照 msg
		rf.applyCh <- msg
	}()
}

// 发送 InstallSnapshot
func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

// sendInstallSnapshotToPeer 向其它 peer 发送快照请求
func (rf *Raft) sendInstallSnapshotToPeer(peerId int) {
	rf.mu.Lock()
	args := InstallSnapshotArgs{
		Term:              rf.currentTerm,
		LeaderId:          rf.me,
		LastIncludedIndex: rf.log.LastIncludedIndex,
		LastIncludedTerm:  rf.log.LastIncludedTerm,
		Data:              rf.snapshot,
	}
	reply := InstallSnapshotReply{}
	rf.mu.Unlock()
	ok := rf.sendInstallSnapshot(peerId, &args, &reply)
	if !ok {
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// 如果当前的状态不为 leader，那么将不能接受
	if rf.state != Leader || args.Term != rf.currentTerm {
		return
	}
	if reply.Term > rf.currentTerm {
		rf.becomeFollower(reply.Term)
		// 你的任期大，我成为你的追随者
		rf.leaderId = peerId
		rf.persist()
		return
	}
	// 注意，快照和日志同步一样，需要更新 matchIndex 和 nextIndex
	// 发送完快照后，更新了 matchIndex 和 nextIndex，因此在快照期间的日志同步将需要重新来
	rf.matchIndex[peerId] = args.LastIncludedIndex
	rf.nextIndex[peerId] = args.LastIncludedIndex + 1
}

// CondInstallSnapshot
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// more recent info since it communicate the snapshot on applyCh.
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {
	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 已快照过了，拒绝
	if lastIncludedIndex <= rf.commitIndex {
		return false
	}
	// 快照后的处理工作
	defer func() {
		rf.log.LastIncludedIndex = lastIncludedIndex
		rf.log.LastIncludedTerm = lastIncludedTerm
		rf.snapshot = snapshot
		rf.commitIndex = lastIncludedIndex
		rf.lastApplied = lastIncludedIndex
		rf.persistStateAndSnapshot(snapshot) // 持久化快照
	}()
	// 删除掉 lastIncludedIndex 之前的日志记录
	if lastIncludedIndex <= rf.log.last() && rf.log.entryAt(lastIncludedIndex).Term == lastIncludedTerm {
		// [rf.log.LastIncludedIndex, lastIncludedIndex) 是当前 snapshot 中的日志数据，所以应该删除
		// 前面需要一个占位
		rf.log.Entries = append([]LogEntry{{Term: 0, Command: nil}}, rf.log.Entries[lastIncludedIndex-rf.log.LastIncludedIndex+1:]...)
		return true
	}
	// 快照，删除所有 log entries
	rf.log.Entries = []LogEntry{{Term: 0, Command: nil}}
	return true
}

func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// 拒绝快照过的，也拒绝还未提交的
	if index <= rf.log.LastIncludedIndex || index > rf.commitIndex {
		return
	}
	rf.log.Entries = append([]LogEntry{{Term: 0, Command: nil}}, rf.log.Entries[index-rf.log.LastIncludedIndex+1:]...)
	rf.log.LastIncludedIndex = index
	rf.log.LastIncludedTerm = rf.log.entryAt(index).Term
	rf.snapshot = snapshot
	rf.persistStateAndSnapshot(snapshot)
}

func (rf *Raft) Start(command any) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	index := -1
	term := rf.currentTerm
	isLeader := false
	if rf.state == Leader {
		isLeader = true
		index = rf.log.size()
		rf.log.append(LogEntry{
			Term:    term,
			Command: command,
		})
		DPrintf("peer: %d, index: %d, start command: %+v", rf.me, index, command)
		rf.persist()
		rf.nextIndex[rf.me] = index + 1
		rf.matchIndex[rf.me] = index
	}
	return index, term, isLeader
}

func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {
	for rf.killed() == false {
		rf.mu.Lock()
		before := rf.lastReceivedFromLeader
		rf.mu.Unlock()

		// Follower 在一定时间内未收到心跳，那么就会超时成为 Candidate，发起选举
		time.Sleep(getRandomElectTimeout())
		// 加锁
		rf.mu.Lock()
		after := rf.lastReceivedFromLeader
		// 如果是 leader，或者 before != after(证明lastAppendEntriesReceived更新了，有心跳，那就不发起投票)
		if rf.state == Leader || !before.Equal(after) {
			rf.mu.Unlock()
			continue
		}

		rf.becomeCandidate()
		rf.persist()
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
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.log.last(),
		LastLogTerm:  rf.log.lastTerm(),
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
		rf.persist()
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
		rf.persist()
	}
}

type RequestVoteArgs struct {
	Term        int // 请求者任期
	CandidateId int // 请求者Id
	// 2B
	LastLogTerm  int
	LastLogIndex int
}

type RequestVoteReply struct {
	Term        int  // 回复者任期，如果回复者任期高，那么请求者成为追随者
	VoteGranted bool // 是否投票
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
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
		rf.persist()
	}
	upToDate := false
	// 如果两份日志最后的条目的任期号不同，那么任期号大的日志新
	if args.LastLogTerm > rf.log.lastTerm() {
		upToDate = true
	}
	// 如果两份日志最后的条目任期号相同，那么日志比较长的那个就新
	if rf.log.lastTerm() == args.LastLogTerm && args.LastLogIndex >= rf.log.last() {
		upToDate = true
	}
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && upToDate {
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId // 投票后，记得更新 votedFor
		rf.persist()
	}
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
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

	l := len(rf.peers)
	rf.nextIndex = make([]int, l)
	rf.matchIndex = make([]int, l)
	for i := 0; i < l; i++ {
		// nextIndex[0] 表示 0 号 peer
		rf.nextIndex[i] = rf.log.last() + 1 // 初始值为领导者最后的日志序号+1
		rf.matchIndex[i] = 0                // 初始值为 0，单调递增
	}

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
			if peerId == rf.me {
				// 更新自己的 nextIndex 和 matchIndex
				rf.nextIndex[peerId] = rf.log.size()
				rf.matchIndex[peerId] = rf.nextIndex[peerId] - 1
				continue
			}
			prevLogIndex := rf.nextIndex[peerId] - 1
			if prevLogIndex < rf.log.LastIncludedIndex {
				go rf.sendInstallSnapshotToPeer(peerId)
			} else {
				go rf.sendAppendEntriesToPeer(peerId)
			}
		}
		rf.mu.Unlock()
		time.Sleep(heartbeatInterval)
	}
}

// 应用日志 loop
func (rf *Raft) applyLogLoop() {
	for !rf.killed() {
		time.Sleep(applyInterval)
		rf.mu.Lock()
		msgs := make([]ApplyMsg, 0)
		for rf.commitIndex > rf.lastApplied {
			rf.lastApplied++ // 上一个应用的++
			// 超过了则回退，并 break
			if rf.lastApplied >= rf.log.size() {
				rf.lastApplied--
				break
			}
			msg := ApplyMsg{
				CommandValid: true,
				Command:      rf.log.entryAt(rf.lastApplied).Command,
				CommandIndex: rf.lastApplied,
				CommandTerm:  rf.currentTerm, // 3A
			}
			msgs = append(msgs, msg)
		}
		rf.mu.Unlock()
		for _, msg := range msgs {
			// DPrintf("peer: %d, commit index: %d, last applied: %d, send msg: %v",
			// 	rf.me, rf.commitIndex, rf.lastApplied, msg)
			rf.applyCh <- msg
		}
	}
}

func (rf *Raft) sendAppendEntriesToPeer(peerId int) {
	rf.mu.Lock()
	nextIndex := rf.nextIndex[peerId]
	prevLogTerm := 0
	prevLogIndex := 0
	entries := make([]LogEntry, 0)
	// 可能会存在 nextIndex 超过 rf.log 的情况
	if nextIndex <= rf.log.size() {
		prevLogIndex = nextIndex - 1
	}
	// double check，检查 prevLogIndex 与 lastIncludeIndex
	if rf.log.LastIncludedIndex != 0 && prevLogIndex < rf.log.LastIncludedIndex {
		DPrintf("peer: %d sendAppendEntriesToPeer but reject, prevLogIndex: %d, lastIncludedIndex: %d",
			rf.me, prevLogIndex, rf.log.LastIncludedIndex)
		rf.mu.Unlock()
		return
	}
	prevLogTerm = rf.log.entryAt(prevLogIndex).Term
	entries = rf.log.getEntries(nextIndex - rf.log.first())
	args := AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      entries,
		LeaderCommit: rf.commitIndex,
	}
	reply := AppendEntriesReply{}
	DPrintf("leader: %d sendAppendEntries to peer: %d, args: %+v", rf.me, peerId, args)
	rf.mu.Unlock()
	// 发送 RPC 的时候不要加锁
	ok := rf.sendAppendEntries(peerId, &args, &reply)
	if !ok {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != Leader || args.Term != rf.currentTerm {
		return
	}
	if reply.Term > rf.currentTerm {
		rf.becomeFollower(reply.Term)
		rf.leaderId = peerId
		rf.persist()
		return
	}
	if reply.Success {
		// 1. 更新 matchIndex 和 nextIndex
		rf.matchIndex[peerId] = prevLogIndex + len(args.Entries)
		rf.nextIndex[peerId] = rf.matchIndex[peerId] + 1
		// 2. 计算更新 commitIndex
		newCommitIndex := getMajorIndex(rf.matchIndex)
		DPrintf("peer: %d, newCommitIndex: %d, commitIndex: %d", rf.me, newCommitIndex, rf.commitIndex)
		if newCommitIndex > rf.commitIndex && rf.log.entryAt(newCommitIndex).Term == rf.currentTerm {
			DPrintf("peer: %d update commitIndex: %d", rf.me, newCommitIndex)
			rf.commitIndex = newCommitIndex
		}
	} else {
		if reply.ConflictTerm == -1 {
			rf.nextIndex[peerId] = reply.ConflictIndex
		} else {
			// Upon receiving a conflict response, the leader should first search its log for conflictTerm.
			// If it finds an entry in its log with that term,
			// it should set nextIndex to be the one beyond the index of the last entry in that term in its log.
			lastIndexOfTerm := -1
			for i := rf.log.last(); i >= rf.log.first(); i-- {
				if rf.log.entryAt(i).Term == reply.ConflictTerm {
					lastIndexOfTerm = i
					break
				}
			}
			// If it does not find an entry with that term, it should set nextIndex = conflictIndex.
			if lastIndexOfTerm < 0 {
				rf.nextIndex[peerId] = reply.ConflictIndex
			} else {
				// 如果找到了冲突的任期，那么 +1 就是下一个需要同步的
				rf.nextIndex[peerId] = lastIndexOfTerm + 1
			}
		}
	}
}

type AppendEntriesArgs struct {
	Term         int        // leader 任期
	LeaderId     int        // leader id
	PrevLogIndex int        // leader 中上一次同步的日志索引
	PrevLogTerm  int        // leader 中上一次同步的日志任期
	Entries      []LogEntry // 同步日志
	LeaderCommit int        // 领导者的已知已提交的最高的日志条目的索引
}

type AppendEntriesReply struct {
	ConflictTerm  int  // 日志冲突任期
	ConflictIndex int  // 日志冲突序号
	Term          int  // 当前任期号，以便于候选人去更新自己的任期号
	Success       bool // 是否同步成功，true 为成功
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.Success = false
	if args.Term < rf.currentTerm {
		return
	}
	// 如果你大，那就成为 follower
	if args.Term > rf.currentTerm {
		rf.becomeFollower(args.Term)
		rf.leaderId = args.LeaderId
		rf.persist()
	}
	if rf.state != Follower {
		rf.becomeFollower(args.Term)
		rf.persist()
	}
	rf.leaderId = args.LeaderId
	rf.lastReceivedFromLeader = time.Now()
	if args.PrevLogIndex < rf.log.LastIncludedIndex {
		reply.ConflictIndex = rf.log.LastIncludedIndex
		reply.ConflictTerm = -1
		return
	}
	logSize := rf.log.size()
	// 日志、任期冲突直接返回
	if args.PrevLogIndex >= logSize {
		reply.ConflictIndex = rf.log.size()
		reply.ConflictTerm = -1
		return
	}
	if rf.log.entryAt(args.PrevLogIndex).Term != args.PrevLogTerm {
		reply.ConflictTerm = rf.log.entryAt(args.PrevLogIndex).Term
		for i := rf.log.first(); i < rf.log.size(); i++ {
			if rf.log.entryAt(i).Term == reply.ConflictTerm {
				reply.ConflictIndex = i
				break
			}
		}
		return
	}
	entriesSize := len(args.Entries)
	insertIndex := args.PrevLogIndex + 1
	entriesIndex := 0
	// 遍历日志，找到冲突日志
	for {
		// 超过了长度 break
		if insertIndex >= logSize || entriesIndex >= entriesSize {
			break
		}
		// 日志冲突，break
		if rf.log.entryAt(insertIndex).Term != args.Entries[entriesIndex].Term {
			break
		}
		insertIndex++
		entriesIndex++
	}
	// 追加日志中尚未存在的任何新条目
	if entriesIndex < entriesSize {
		// [0,insertIndex) 是之前已经同步好的日志
		rf.log.subTo(insertIndex - rf.log.first())
		rf.log.append(args.Entries[entriesIndex:]...)
		rf.persist()
	}
	// 取两者的最小值
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = minInt(args.LeaderCommit, rf.log.last())
		DPrintf("peer: %d update commitIndex: %d, leaderCommit: %d, last index: %d",
			rf.me, rf.commitIndex, args.LeaderCommit, rf.log.last())
	}
	reply.Success = true
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs,
	reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

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
	// 2B
	rf.log = defaultRLog()
	rf.lastReceivedFromLeader = time.Now()
	// initialize from state persisted before a crash
	// 读取持久化数据可能会改变任期、投票等数据
	rf.readPersist(persister.ReadRaftState())
	// 2D
	rf.snapshot = persister.ReadSnapshot()
	rf.commitIndex = rf.log.LastIncludedIndex
	rf.lastApplied = rf.log.LastIncludedIndex

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.applyLogLoop()

	return rf
}
