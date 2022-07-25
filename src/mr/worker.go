package mr

import (
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strings"
)

// KeyValue
// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose to reduce
// task number for each KeyValue emitted by Map.
/**
在具体的实现之前，我们来思考一下，分布式 MR 实现的原理，为什么海量数据可以在多个物理机上执行，产生多个结果文件，但是结果却是正确而又有序呢？

这个问题困扰了我很久，以至于我在实现 MR 的时候，功能都实现了，但是结果却一直错误，直到我看了别人实现的 Map 操作，我一下就明白了。

这个问题的关键点在 Hash 拆分，将所有 keys 以哈希的方式拆分到不同的物理机上，由于哈希的作用，相同的 key 会被拆分到同一个物理机，
因此多个物理机做 Reduce 时，key 是不会混淆的。这种哈希拆分的思想被广泛运行，如 redis cluster。
*/
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// Worker
// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	// Your worker implementation here.
	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
	// 请求任务
	for {
		log.Printf("try call task")
		taskReply, err := CallRequestTask()
		if err != nil {
			log.Fatal("CallRequestTask err: ", err)
		}
		log.Printf("call task: %v", *taskReply)
		switch taskReply.TaskType {
		case MapTask:
			doMapTask(taskReply, mapf)
		case ReduceTask:
			doReduceTask(taskReply, reducef)
		default:
			log.Fatalf("invalid task: %s", taskReply.TaskType)
		}
	}
}

// ByKey for sorting by key.
type ByKey []KeyValue

// Len for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

func doReduceTask(reply *RequestTaskReply, reducef func(string, []string) string) {
	interFiles := reply.InterFiles
	intermediate := make([]KeyValue, 0)
	for _, interFile := range interFiles {
		file, err := os.Open(interFile)
		if err != nil {
			log.Fatalf("cannot open %v", interFile)
		}

		content, err := ioutil.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", interFile)
		}

		parts := strings.Split(string(content), "\r\n")
		for _, part := range parts {
			if strings.TrimSpace(part) == "" {
				continue
			}
			kv := line2kv(part)
			intermediate = append(intermediate, kv)
		}

		file.Close()
	}

	sort.Sort(ByKey(intermediate))

	oname := fmt.Sprintf("mr-out-%d", reply.Number)
	ofile, err := os.Create(oname)
	if err != nil {
		log.Fatalf("cannot create %s", err)
	}

	i := 0
	for i < len(intermediate) {
		j := i + 1
		// shuffle 操作，将 key 相同的单词聚合成数组
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		var values []string
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	ofile.Close()

	args := &FinishTaskArgs{
		TaskType:   ReduceTask,
		Filepath:   reply.Filepath,
		Number:     reply.Number,
		InterFiles: interFiles,
	}
	CallFinishTask(args)
}

func doMapTask(reply *RequestTaskReply, mapf func(string, string) []KeyValue) {
	filepath := reply.Filepath
	log.Printf("do map task: %s", filepath)
	file, err := os.Open(filepath)
	if err != nil {
		log.Fatalf("cannot open %v", filepath)
	}
	defer file.Close()

	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filepath)
	}
	// map 任务结果
	kva := mapf(filepath, string(content))
	fileMap := map[string]*os.File{}
	for _, kv := range kva {
		// use ihash(key) % NReduce to choose to reduce
		// task number for each KeyValue emitted by Map.
		n := ihash(kv.Key) % reply.NReduce
		intermediateFilename := fmt.Sprintf("mr-%d-%d", reply.Number, n)
		if _, ok := fileMap[intermediateFilename]; !ok {
			var intermediateFile *os.File
			if intermediateFile, err = os.Create(intermediateFilename); err != nil {
				log.Fatalf("create file err: %s", err)
			}
			fileMap[intermediateFilename] = intermediateFile
		}
		_, err = fileMap[intermediateFilename].WriteString(kv2Line(kv))
		if err != nil {
			log.Fatalf("write file err: %s", err)
		}
	}
	var interFiles []string
	for filename, item := range fileMap {
		interFiles = append(interFiles, filename)
		item.Close()
	}
	args := &FinishTaskArgs{
		TaskType:   MapTask,
		Filepath:   reply.Filepath,
		Number:     reply.Number,
		InterFiles: interFiles,
	}
	CallFinishTask(args)
}

func kv2Line(kv KeyValue) string {
	return kv.Key + "," + kv.Value + "\r\n"
}

func line2kv(line string) KeyValue {
	parts := strings.Split(line, ",")
	return KeyValue{
		Key:   parts[0],
		Value: parts[1],
	}
}

// CallRequestTask 请求任务
func CallRequestTask() (*RequestTaskReply, error) {
	args := RequestTaskArgs{}
	reply := RequestTaskReply{}
	ok := call("Coordinator.RequestTask", &args, &reply)
	if ok {
		log.Printf("%v\n", reply)
		return &reply, nil
	}
	log.Printf("call RequestTask failed!\n")
	return nil, fmt.Errorf("call RequestTask failed")
}

// CallFinishTask 完成任务
func CallFinishTask(args *FinishTaskArgs) {
	reply := FinishTaskReply{}
	ok := call("Coordinator.FinishTask", args, &reply)
	if ok {
		log.Printf("%v\n", reply)
	} else {
		log.Printf("call FinishTask failed!\n")
	}
}

// CallExample
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		log.Printf("reply.Y %v\n", reply.Y)
	} else {
		log.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns tr§ue.
// returns false if something goes wrong.
func call(rpcName string, args any, reply any) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockName := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockName)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcName, args, reply)
	if err == nil {
		return true
	}

	log.Println(err)
	return false
}
