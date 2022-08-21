package kvpaxos

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
	ErrNoLeader    = "ErrNoLeader" // leader还没被选出来
	ErrTimeout     = "ErrTimeout"
	ErrShutdown    = "ErrShutdown"
)

type Err string

// PutAppendArgs Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Seq      int
	ClientId int64
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	Seq      int
	ClientId int64
}

type GetReply struct {
	Err   Err
	Value string
}
