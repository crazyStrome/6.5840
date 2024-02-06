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

// Add your RPC definitions here.

// GetTaskRequest 获取任务的请求
type GetTaskRequest struct {
	WorkerID int
}

const (
	ValidTaskTodo = 1
	ValidTaskWait = 2
	ValidTaskDone = 3
)

// GetTaskResponse 获取任务的响应
type GetTaskResponse struct {
	Valid       int // 1: 有任务待执行，2：等待分配任务，3：所有任务执行完成
	TaskType    int
	MapFileName string
	TaskIdx     int // 任务的索引
	NReduce     int // reduce 任务的数量
	MMap        int // map 任务的数量
}

// SetResultRequest 设置任务结果
type SetResultRequest struct {
	TaskType  int
	TaskIdx   int
	IsError   bool
	FileNames []string
}

// SetResultResponse 设置任务结果
type SetResultResponse struct {
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
