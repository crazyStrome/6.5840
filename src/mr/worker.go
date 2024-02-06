package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"os"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	for {
		task, err := getTask()
		if err != nil {
			log.Printf("get task err:%v\n", err)
			continue
		}
		if task.Valid == ValidTaskDone {
			return
		}
		if task.Valid == ValidTaskWait {
			time.Sleep(time.Second)
			continue
		}
		var names []string
		if task.TaskType == TaskTypeMap {
			names, err = processMapTask(task, mapf)
		} else {
			names, err = processReduceTask(task, reducef)
		}
		setResult(&SetResultRequest{
			TaskType:  task.TaskType,
			TaskIdx:   task.TaskIdx,
			FileNames: names,
			IsError:   err != nil,
		})
	}
}

func processMapTask(task *GetTaskResponse, mapf func(string, string) []KeyValue) (names []string, err error) {
	fileData, _ := os.ReadFile(task.MapFileName)
	kvs := mapf(task.MapFileName, string(fileData))
	ouputs := make([]*json.Encoder, task.NReduce)
	for i := range ouputs {
		name := fmt.Sprintf("%d-mr-%d-%d", os.Getpid(), task.TaskIdx, i)
		names = append(names, name)
		file, _ := os.Create(name)
		defer file.Close()
		enc := json.NewEncoder(file)
		ouputs[i] = enc
	}
	for _, kv := range kvs {
		idx := ihash(kv.Key) % task.NReduce
		ouputs[idx].Encode(&kv)
	}
	return names, nil
}

func processReduceTask(task *GetTaskResponse,
	reducef func(string, []string) string) (names []string, err error) {
	kvs := make(map[string][]string)
	for i := 0; i < task.MMap; i++ {
		name := fmt.Sprintf("mr-%d-%d", i, task.TaskIdx)
		file, _ := os.Open(name)
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kvs[kv.Key] = append(kvs[kv.Key], kv.Value)
		}
		file.Close()
	}
	name := fmt.Sprintf("%d-mr-out-%d", os.Getpid(), task.TaskIdx)
	file, _ := os.Create(name)
	for k, vs := range kvs {
		result := reducef(k, vs)
		fmt.Fprintf(file, "%v %v\n", k, result)
	}
	file.Close()
	return []string{name}, nil
}

func getTask() (*GetTaskResponse, error) {
	req := &GetTaskRequest{}
	rsp := &GetTaskResponse{}
	ok := call("Coordinator.GetTask", req, rsp)
	if !ok {
		return nil, fmt.Errorf("get task err")
	}
	return rsp, nil
}

func setResult(req *SetResultRequest) error {
	rsp := &SetResultResponse{}
	ok := call("Coordinator.SetResult", req, rsp)
	if !ok {
		return fmt.Errorf("set err")
	}
	return nil
}

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
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
