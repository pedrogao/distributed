package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

type TaskType = string

const (
	MapTask    TaskType = "Map"
	ReduceTask TaskType = "Reduce"
)

//
// RequestTask 请求任务
//

type RequestTaskArgs struct {
}

type RequestTaskReply struct {
	TaskType   TaskType
	Filepath   string
	InterFiles []string // reduce任务中间文件
	NReduce    int      // reduce个数
	Number     int      // 任务序号
	Done       bool     // 所有任务已经完成
}

//
// FinishTask 完成任务
//

type FinishTaskArgs struct {
	TaskType   TaskType
	Filepath   string
	Number     int // 任务序号
	InterFiles []string
}

type FinishTaskReply struct {
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
