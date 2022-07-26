package pbservice

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongServer = "ErrWrongServer"
)

type Err string

type PutArgs struct {
	Key   string
	Value string
}

type PutReply struct {
	Err Err
}

type GetArgs struct {
	Key string
}

type GetReply struct {
	Err   Err
	Value string
}

//
// Your RPC definitions here.
//

type UpdateArgs struct {
	Key   string
	Value string
}

type UpdateReply struct {
	Err Err
}

type ForwardArgs struct {
	KV map[string]string
}

type ForwardReply struct {
	Err Err
}
