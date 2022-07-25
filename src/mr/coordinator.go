package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Coordinator struct {
	// Your definitions here.
	nReduce    int // total reduce number
	curReduce  int // current reduce number
	inputFiles []string

	mapTasks    map[int]bool
	reduceTasks map[int]bool

	reduceFiles map[int][]string
	mapFiles    map[int]string

	// mapTaskNumber    int
	// reduceTaskNumber int

	mu sync.Mutex // 分布式任务状态信息数据必须加锁
}

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) RequestTask(args *RequestTaskArgs,
	reply *RequestTaskReply) error {
	log.Printf("handle RequestTask")
	// 派发任务
	if c.Done() {
		return fmt.Errorf("all tasks done")
	}
	dispatchMap := !c.isAllMapTasksDone()
	// 派发 Map 任务
	if dispatchMap {
		c.mu.Lock()
		for number, ok := range c.mapTasks {
			if !ok {
				reply.TaskType = MapTask
				reply.Filepath = c.mapFiles[number]
				reply.NReduce = c.nReduce
				reply.Number = number
				log.Printf("dispatch map task: %v", *reply)
				go c.monitorTask(reply)
				break
			}
		}
		c.mu.Unlock()
		return nil
	}
	// 派发 Reduce 任务
	c.mu.Lock()
	defer c.mu.Unlock()

	for number, ok := range c.reduceTasks {
		if !ok {
			reply.TaskType = ReduceTask
			reply.InterFiles = c.reduceFiles[number]
			reply.NReduce = c.nReduce
			reply.Number = number
			log.Printf("dispatch reduce task: %v", *reply)
			go c.monitorTask(reply)
			break
		}
	}
	return nil
}

func (c *Coordinator) monitorTask(reply *RequestTaskReply) {
	// 监控任务完成，如果超过10s，则重新发布任务
	var taskMap map[int]bool
	if reply.TaskType == MapTask {
		taskMap = c.mapTasks
	} else {
		taskMap = c.reduceTasks
	}
	t := time.NewTicker(time.Second * 10)
	defer t.Stop()
	for {
		select {
		case <-t.C:
			// 超时
			c.mu.Lock()
			taskMap[reply.Number] = false // 超时后再次设置为 false
			c.mu.Unlock()
		default:
			c.mu.Lock()
			if taskMap[reply.Number] { // 如果完成了，直接返回
				c.mu.Unlock()
				return
			}
			c.mu.Unlock()
		}
	}
}

func (c *Coordinator) isAllMapTasksDone() bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	for _, t := range c.mapTasks {
		if !t {
			return false
		}
	}
	return true
}

func (c *Coordinator) FinishTask(args *FinishTaskArgs,
	reply *FinishTaskReply) error {
	if c.Done() {
		return fmt.Errorf("all tasks done")
	}

	number := args.Number
	switch args.TaskType {
	case ReduceTask:
		if !c.isAllMapTasksDone() {
			return fmt.Errorf("reduce task can't be finished")
		}
		c.mu.Lock()
		defer c.mu.Unlock()
		done, exists := c.reduceTasks[number]
		if !exists {
			return fmt.Errorf("reduce task: %d is not valid", number)
		}
		if done {
			return fmt.Errorf("reduce task: %d is already done", number)
		}
		log.Printf("finish reduce task: %v", *args)
		c.reduceTasks[number] = true
	case MapTask:
		// worker 已完成 map 任务
		c.mu.Lock()
		defer c.mu.Unlock()
		done, exists := c.mapTasks[number]
		if !exists {
			return fmt.Errorf("map task: %d is not valid", number)
		}
		if done {
			return fmt.Errorf("map task: %d is already done", number)
		}
		c.mapTasks[number] = true
		log.Printf("finish map task: %v", *args)
		for _, interFile := range args.InterFiles {
			i := strings.LastIndexByte(interFile, '-')
			n, err := strconv.ParseInt(interFile[i+1:], 10, 64)
			if err != nil {
				log.Fatal("parse int error:", err)
			}
			c.reduceFiles[int(n)] = append(c.reduceFiles[int(n)], interFile)
		}
		log.Printf("reduceFiles: %v", c.reduceFiles)
	default:
		return fmt.Errorf("invalid task tyoe")
	}
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	// l, e := net.Listen("tcp", ":1234")
	sockName := coordinatorSock()
	os.Remove(sockName)
	l, e := net.Listen("unix", sockName)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// Done
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	// Your code here.
	mapDone := c.isAllMapTasksDone()
	if !mapDone {
		return false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, ok := range c.reduceTasks {
		if !ok {
			return false
		}
	}

	return true
}

// MakeCoordinator
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	// 注意：nReduce 表示 reduce 任务数不能超过 nReduce 的个数，但 map 任务无限制
	mapTasks := map[int]bool{}
	mapFiles := map[int]string{}
	// 待 map 完成后再填充 reduceJobs
	for i, file := range files {
		mapTasks[i] = false
		mapFiles[i] = file
	}

	reduceFiles := map[int][]string{}
	reduceTasks := map[int]bool{}
	for i := 0; i < nReduce; i++ {
		reduceTasks[i] = false
		reduceFiles[i] = []string{}
	}
	log.Printf("mapFiles: %v", mapFiles)
	log.Printf("reduceFiles: %v", reduceFiles)
	c := Coordinator{
		nReduce:     nReduce,
		curReduce:   0,
		inputFiles:  files,
		mapTasks:    mapTasks,
		mapFiles:    mapFiles,
		reduceTasks: reduceTasks,
		reduceFiles: reduceFiles,
		mu:          sync.Mutex{},
	}

	c.server()
	return &c
}
