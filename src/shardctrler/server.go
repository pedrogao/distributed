package shardctrler

import (
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pedrogao/log"

	"pedrogao/distributed/labgob"
	"pedrogao/distributed/labrpc"
	"pedrogao/distributed/raft"
)

var (
	// Debug Debugging
	Debug  = false
	logger = log.New(log.WithSkipLevel(3))
)

func init() {
	if os.Getenv("debug") != "" || Debug {
		logger.SetOptions(log.WithLevel(log.DebugLevel))
	} else {
		logger.SetOptions(log.WithLevel(log.ErrorLevel))
	}
}

func DPrintf(format string, a ...any) {
	logger.Debugf(format, a...)
}

// ShardCtrler 基于 raft 的配置中心
// 注意：每次 shard 的更新，都必须将分组重新分配，并且尽可能均匀
type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.
	dead        int32
	configs     []Config                 // indexed by config num, config num 表示版本号
	recordMap   map[int64]*clientRecord  // 操作记录
	notifyMap   map[int]chan *clientResp // 通知记录
	lastApplied int64
}

type clientRecord struct {
	Seq   int
	Index int
	Resp  *clientResp
}

type clientResp struct {
	Config Config
	Err    Err
}

type Action = string

const (
	JoinAction  Action = "Join"
	LeaveAction Action = "Leave"
	MoveAction  Action = "Move"
	QueryAction Action = "Query"
)

// Op raft log entry
type Op struct {
	// Your data here.
	Seq      int
	ClientId int64

	Action string
	// Servers for Join
	Servers map[int][]string // new GID -> servers mappings
	// Num for Query
	Num int // desired config number
	// GIDs for leave
	GIDs []int
	// Shard & GID for move
	Shard int
	GID   int
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	// 加入
	op := Op{
		Seq:      args.Seq,
		ClientId: args.ClientId,
		Action:   JoinAction,
		Servers:  args.Servers,
	}
	index, term, isLeader := sc.rf.Start(op)
	if term == 0 { // term = 0 的时候，leader 还没选出来
		DPrintf("%d Join no leader, args: %+v, reply: %+v ", sc.Me(), args, reply)
		reply.Err = ErrNoLeader
		reply.WrongLeader = true
		return
	}
	if !isLeader {
		DPrintf("%d Join wrong leader, args: %+v, reply: %+v ", sc.Me(), args, reply)
		reply.Err = ErrWrongLeader
		reply.WrongLeader = true
		return
	}

	sc.mu.Lock()
	// 只有 leader 需要 notify 客户端
	notify := make(chan *clientResp)
	sc.notifyMap[index] = notify
	sc.mu.Unlock()

	select {
	case resp := <-notify: // 阻塞
		// 成功同步
		reply.Err = resp.Err
		DPrintf("%d Join Op ok, op: %+v ", sc.Me(), op)
	case <-time.After(defaultTimeout):
		// 超时
		DPrintf("%d Join Op timeout, op: %+v ", sc.Me(), op)
		reply.Err = ErrTimeout
	}

	// 删除无用的 notify
	go func() {
		sc.mu.Lock()
		defer sc.mu.Unlock()
		delete(sc.notifyMap, index)
	}()
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	// 离开
	op := Op{
		Seq:      args.Seq,
		ClientId: args.ClientId,
		Action:   LeaveAction,
		GIDs:     args.GIDs,
	}
	index, term, isLeader := sc.rf.Start(op)
	if term == 0 { // term = 0 的时候，leader 还没选出来
		DPrintf("%d Leave no leader, args: %+v, reply: %+v ", sc.Me(), args, reply)
		reply.Err = ErrNoLeader
		reply.WrongLeader = true
		return
	}
	if !isLeader {
		DPrintf("%d Leave wrong leader, args: %+v, reply: %+v ", sc.Me(), args, reply)
		reply.Err = ErrWrongLeader
		reply.WrongLeader = true
		return
	}

	sc.mu.Lock()
	notify := make(chan *clientResp)
	sc.notifyMap[index] = notify
	sc.mu.Unlock()

	select {
	case resp := <-notify:
		reply.Err = resp.Err
		DPrintf("%d Leave Op ok, op: %+v ", sc.Me(), op)
	case <-time.After(defaultTimeout):
		// 超时
		DPrintf("%d Leave Op timeout, op: %+v ", sc.Me(), op)
		reply.Err = ErrTimeout
	}

	go func() {
		sc.mu.Lock()
		delete(sc.notifyMap, index)
		sc.mu.Unlock()
	}()
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	// 移动
	op := Op{
		Seq:      args.Seq,
		ClientId: args.ClientId,
		Action:   MoveAction,
		Shard:    args.Shard,
		GID:      args.GID,
	}
	index, term, isLeader := sc.rf.Start(op)
	if term == 0 { // term = 0 的时候，leader 还没选出来
		DPrintf("%d Move no leader, args: %+v, reply: %+v ", sc.Me(), args, reply)
		reply.Err = ErrNoLeader
		reply.WrongLeader = true
		return
	}
	if !isLeader {
		DPrintf("%d Move wrong leader, args: %+v, reply: %+v ", sc.Me(), args, reply)
		reply.Err = ErrWrongLeader
		reply.WrongLeader = true
		return
	}

	sc.mu.Lock()
	notify := make(chan *clientResp)
	sc.notifyMap[index] = notify
	sc.mu.Unlock()

	select {
	case resp := <-notify:
		reply.Err = resp.Err
		DPrintf("%d Move Op ok, op: %+v ", sc.Me(), op)
	case <-time.After(defaultTimeout):
		// 超时
		DPrintf("%d Move Op timeout, op: %+v ", sc.Me(), op)
		reply.Err = ErrTimeout
	}

	go func() {
		sc.mu.Lock()
		delete(sc.notifyMap, index)
		sc.mu.Unlock()
	}()
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	// 查询
	op := Op{
		Seq:      args.Seq,
		ClientId: args.ClientId,
		Action:   QueryAction,
		Num:      args.Num,
	}
	index, term, isLeader := sc.rf.Start(op)
	if term == 0 { // term = 0 的时候，leader 还没选出来
		DPrintf("%d Query no leader, args: %+v, reply: %+v ", sc.Me(), args, reply)
		reply.Err = ErrNoLeader
		reply.WrongLeader = true
		return
	}
	if !isLeader {
		DPrintf("%d Query wrong leader, args: %+v, reply: %+v ", sc.Me(), args, reply)
		reply.Err = ErrWrongLeader
		reply.WrongLeader = true
		return
	}

	sc.mu.Lock()
	notify := make(chan *clientResp)
	sc.notifyMap[index] = notify
	sc.mu.Unlock()

	select {
	case resp := <-notify:
		reply.Err = resp.Err
		reply.Config = resp.Config
		DPrintf("%d Query Op ok, op: %+v, reply: %+v", sc.Me(), op, reply)
	case <-time.After(defaultTimeout):
		// 超时
		DPrintf("%d Query Op timeout, op: %+v ", sc.Me(), op)
		reply.Err = ErrTimeout
	}

	go func() {
		sc.mu.Lock()
		delete(sc.notifyMap, index)
		sc.mu.Unlock()
	}()
}

// Kill
// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	// Your code here, if desired.
	atomic.StoreInt32(&sc.dead, 1)
}

func (sc *ShardCtrler) killed() bool {
	z := atomic.LoadInt32(&sc.dead)
	return z == 1
}

// Raft needed by shard kv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

func (sc *ShardCtrler) Me() int {
	val := int32(sc.me)
	return int(atomic.LoadInt32(&val))
}

func (sc *ShardCtrler) apply() {
	for !sc.killed() {
		select {
		case message := <-sc.applyCh:
			DPrintf("%d handle apply message: %+v ", sc.Me(), message.CommandIndex)
			if message.CommandValid {
				index := message.CommandIndex
				if int64(index) <= atomic.LoadInt64(&sc.lastApplied) {
					DPrintf("%d discard apply op, message: %+v, lastApplied: %+v ", sc.Me(), message, sc.lastApplied)
					continue
				}
				command, ok := message.Command.(Op)
				if !ok {
					log.Fatalf("invalid message command: %+v", message.Command)
				}
				clientId := command.ClientId
				seq := command.Seq
				resp := &clientResp{}
				sc.mu.Lock() // 加锁
				switch command.Action {
				case QueryAction:
					// 读取配置数据
					var conf Config
					if command.Num == -1 || command.Num >= len(sc.configs) {
						_, conf = sc.getLatestConfig()
					} else {
						conf = sc.configs[command.Num]
					}
					resp.Err = OK
					resp.Config = conf
					DPrintf("%d apply op successful, action: %+v, Resp: %+v ", sc.Me(), command.Action, resp)
				case JoinAction, LeaveAction, MoveAction:
					if sc.isLatestRequest(clientId, seq) {
						// 如果是重复提交
						record, _ := sc.recordMap[clientId] // FIXME?
						resp = record.Resp
						DPrintf("%d apply op exists, action: %+v, Resp: %+v ", sc.Me(), command.Action, resp)
					} else {
						resp.Err = OK
						record := &clientRecord{
							Seq:   seq,
							Index: index,
							Resp:  resp,
						}
						sc.recordMap[clientId] = record
						// 更新 config
						switch command.Action {
						case JoinAction:
							sc.handleJoin(command.Servers)
						case MoveAction:
							sc.handleMove(command.Shard, command.GID)
						case LeaveAction:
							sc.handleLeave(command.GIDs)
						}
						DPrintf("%d apply op successful, action: %+v, Resp: %+v ", sc.Me(), command.Action, resp)
					}
				}
				term, isLeader := sc.rf.GetState()
				if term == message.CommandTerm && isLeader {
					// 注意：只有 leader 会收到请求
					notify, exist := sc.notifyMap[index]
					DPrintf("%d notify op, exist: %+v, action: %+v, Resp: %+v ", sc.Me(), exist, command.Action, resp)
					if exist {
						notify <- resp
					}
				}
				sc.mu.Unlock() // 解锁
				atomic.StoreInt64(&sc.lastApplied, int64(index))
			} else if message.SnapshotValid {
				// TODO 暂时无需快照
				log.Fatalf("invalid snapshot: %+v", message)
			} else {
				log.Fatalf("invalid message: %+v", message)
			}
		}
	}
}

func (sc *ShardCtrler) getLatestConfig() (int, Config) {
	lastVersion := len(sc.configs) - 1
	return lastVersion, sc.configs[lastVersion]
}

func (sc *ShardCtrler) appendNewConfig(conf Config) {
	sc.configs = append(sc.configs, conf)
}

func (sc *ShardCtrler) isLatestRequest(clientId int64, seq int) bool {
	record, ok := sc.recordMap[clientId]
	if !ok {
		return false
	}
	// 等于：当前请求重复提交
	// 小于：以前的请求重复提交
	return seq <= record.Seq
}

// handleJoin 新加入的Group信息，要求在每一个group平衡分布shard，即任意两个group之间的shard数目相差不能为1，
// 具体实现每一次找出含有shard数目最多的和最少的，最多的给最少的一个，循环直到满足条件为止。
// 坑为：GID = 0 是无效配置，一开始所有分片分配给GID=0，需要优先分配；map的迭代时无序的，不确定顺序的话，
// 同一个命令在不同节点上计算出来的新配置不一致，按sort排序之后遍历即可。且 map 是引用对象，需要用深拷贝做复制。
func (sc *ShardCtrler) handleJoin(groups map[int][]string) {
	lastVersion, latestConfig := sc.getLatestConfig()
	// 先拷贝已有配置，注意是深拷贝，不然新的配置更改会引起旧配置变化
	newConf := Config{
		Num:    lastVersion + 1,
		Shards: latestConfig.Shards,
		Groups: deepCopy(latestConfig.Groups),
	}
	// 将新的 groups 加入到新配置中
	for gid, servers := range groups {
		if _, ok := newConf.Groups[gid]; !ok {
			// 注意：copy有坑，如果 dst 是 nil，那么无法拷贝
			newServers := make([]string, len(servers))
			copy(newServers, servers)
			newConf.Groups[gid] = newServers
		}
	}
	// 重新平衡
	// 平衡的方法是每次循环让拥有 Shard 最多的 Group 分一个给拥有 Shard 最少的 Group，直到它们之间的差值小等于1
	g2s := groupToShards(newConf)
	for {
		s, t := getMaxNumShardByGid(g2s), getMinNumShardByGid(g2s)
		if s != 0 && len(g2s[s])-len(g2s[t]) <= 1 {
			break
		}
		g2s[t] = append(g2s[t], g2s[s][0])
		g2s[s] = g2s[s][1:]
	}

	var newShards [NShards]int
	for gid, shards := range g2s {
		for _, shardId := range shards {
			newShards[shardId] = gid
		}
	}
	newConf.Shards = newShards
	sc.appendNewConfig(newConf)
}

// handleMove 将数据库子集Shard分配给GID的Group
func (sc *ShardCtrler) handleMove(shard int, gid int) {
	lastVersion, latestConfig := sc.getLatestConfig()
	newConf := Config{
		Num:    lastVersion + 1,
		Shards: latestConfig.Shards,
		Groups: deepCopy(latestConfig.Groups),
	}
	newConf.Shards[shard] = gid
	sc.appendNewConfig(newConf)
}

// handleLeave 移除Group，同样别忘记实现均衡，将移除的Group的shard每一次分配给数目最小的Group就行，如果全部删除，别忘记将shard置为无效的0。
func (sc *ShardCtrler) handleLeave(gids []int) {
	lastVersion, latestConfig := sc.getLatestConfig()
	newConf := Config{
		Num:    lastVersion + 1,
		Shards: latestConfig.Shards,
		Groups: deepCopy(latestConfig.Groups),
	}

	g2s := groupToShards(newConf)
	noUsedShards := make([]int, 0)
	for _, gid := range gids {
		if _, ok := newConf.Groups[gid]; ok {
			delete(newConf.Groups, gid)
		}
		if shards, ok := g2s[gid]; ok {
			noUsedShards = append(noUsedShards, shards...)
			delete(g2s, gid)
		}
	}

	var newShards [NShards]int
	if len(newConf.Groups) > 0 {
		for _, shardId := range noUsedShards {
			t := getMinNumShardByGid(g2s)
			g2s[t] = append(g2s[t], shardId)
		}

		for gid, shards := range g2s {
			for _, shardId := range shards {
				newShards[shardId] = gid
			}
		}
	}
	newConf.Shards = newShards

	sc.appendNewConfig(newConf)
}

func getMinNumShardByGid(g2s map[int][]int) int {
	// 不固定顺序的话，可能会导致两次的config不同
	gids := make([]int, 0)
	for key := range g2s {
		gids = append(gids, key)
	}

	sort.Ints(gids)

	min, index := NShards+1, -1
	for _, gid := range gids {
		if gid != 0 && len(g2s[gid]) < min {
			min = len(g2s[gid])
			index = gid
		}
	}
	return index
}

func getMaxNumShardByGid(g2s map[int][]int) int {
	// GID = 0 是无效配置，一开始所有分片分配给GID=0
	if shards, ok := g2s[0]; ok && len(shards) > 0 {
		return 0
	}

	gids := make([]int, 0)
	for key := range g2s {
		gids = append(gids, key)
	}

	sort.Ints(gids)

	max, index := -1, -1
	for _, gid := range gids {
		if len(g2s[gid]) > max {
			max = len(g2s[gid])
			index = gid
		}
	}
	return index
}

func groupToShards(conf Config) map[int][]int {
	// groupId -> shardIds
	g2s := map[int][]int{}
	for gid := range conf.Groups {
		g2s[gid] = []int{}
	}

	for shardId, gid := range conf.Shards {
		g2s[gid] = append(g2s[gid], shardId)
	}

	return g2s
}

func deepCopy(groups map[int][]string) map[int][]string {
	newGroups := make(map[int][]string)
	for gid, servers := range groups {
		newServers := make([]string, len(servers))
		copy(newServers, servers)
		newGroups[gid] = newServers
	}
	return newGroups
}

// StartServer
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// param me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{} // 0 号配置作为占位

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.
	sc.recordMap = map[int64]*clientRecord{}
	sc.notifyMap = map[int]chan *clientResp{}
	go sc.apply()
	return sc
}
