package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

const (
	TaskStatusIdle       = 1
	TaskStatusProcessing = 2
	TaskStatusCompleted  = 3
)

const (
	TaskTypeMap    = 1
	TaskTypeReduce = 2
)

// Task 任务数据
type Task struct {
	Status      int       // 任务状态
	Ttype       int       // 任务类型，map：1，reduce：2
	MapFileName string    // map 任务的输入文件名
	AssignTime  time.Time // 分配任务的时间
}

type Coordinator struct {
	// Your definitions here.
	nReduce     int        // reduce worker 的数量
	mMap        int        // map worker 的数量
	taskLock    sync.Mutex // 设置任务时加锁
	mapTasks    []Task     // map 任务的状态
	reduceTasks []Task     // reduce 任务的状态

	// TODO 机器节点的标识，用于心跳
}

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) GetTask(req *GetTaskRequest, rsp *GetTaskResponse) error {
	c.taskLock.Lock()
	defer c.taskLock.Unlock()
	// 首先遍历 mapTasks，取出来第一个是 TaskStatusIdle 的任务
	// 如果所有的 mapTasks 任务都不是 TaskStatusIdle 但是还有任务未完成，就返回空任务
	for i, task := range c.mapTasks {
		if canAssignTask(task) {
			c.mapTasks[i].Status = TaskStatusProcessing
			c.mapTasks[i].AssignTime = time.Now()
			rsp.Valid = ValidTaskTodo
			rsp.TaskType = TaskTypeMap
			rsp.TaskIdx = i
			rsp.MapFileName = c.mapTasks[i].MapFileName
			rsp.NReduce = c.nReduce
			rsp.MMap = c.mMap
			return nil
		}
	}
	if !allTaskDone(c.mapTasks) {
		// map 任务还未完成，还需等待
		rsp.Valid = ValidTaskWait
		return nil
	}
	// 此时所有 map 任务执行完成，查找 reduce 任务
	for i, task := range c.reduceTasks {
		if canAssignTask(task) {
			c.reduceTasks[i].Status = TaskStatusProcessing
			c.reduceTasks[i].AssignTime = time.Now()
			rsp.Valid = ValidTaskTodo
			rsp.TaskType = TaskTypeReduce
			rsp.TaskIdx = i
			rsp.MMap = c.mMap
			rsp.NReduce = c.nReduce
			return nil
		}
	}
	if !allTaskDone(c.reduceTasks) {
		rsp.Valid = ValidTaskWait
		return nil
	}
	// 此时所有任务完成
	rsp.Valid = ValidTaskDone
	return nil
}

func canAssignTask(task Task) bool {
	if task.Status == TaskStatusIdle {
		return true
	}
	if task.Status == TaskStatusCompleted {
		return false
	}
	return time.Since(task.AssignTime) > 6*time.Second
}

// allTaskDone 否所有任务都执行完成
func allTaskDone(tasks []Task) bool {
	for _, task := range tasks {
		if task.Status != TaskStatusCompleted {
			return false
		}
	}
	return true
}

func (c *Coordinator) SetResult(req *SetResultRequest, rsp *SetResultResponse) error {
	c.taskLock.Lock()
	defer c.taskLock.Unlock()
	if req.TaskType == TaskTypeMap {
		// map 任务处理
		if req.IsError {
			// 任务执行失败，重置任务状态
			c.mapTasks[req.TaskIdx].Status = TaskStatusIdle
			rmFiles(req.FileNames)
		} else {
			if c.mapTasks[req.TaskIdx].Status == TaskStatusCompleted {
				rmFiles(req.FileNames)
			} else {
				c.mapTasks[req.TaskIdx].Status = TaskStatusCompleted
				for i, name := range req.FileNames {
					nname := fmt.Sprintf("mr-%d-%d", req.TaskIdx, i)
					os.Rename(name, nname)
				}
			}
		}
		return nil
	}
	// reduce 任务处理
	if req.IsError {
		// 任务执行失败，重置任务状态
		c.reduceTasks[req.TaskIdx].Status = TaskStatusIdle
		rmFiles(req.FileNames)
	} else {
		if c.reduceTasks[req.TaskIdx].Status == TaskStatusCompleted {
			rmFiles(req.FileNames)
		} else {
			c.reduceTasks[req.TaskIdx].Status = TaskStatusCompleted
			nname := fmt.Sprintf("mr-out-%d", req.TaskIdx)
			os.Rename(req.FileNames[0], nname)
		}
	}

	return nil
}

func rmFiles(names []string) {
	for _, name := range names {
		os.Remove(name)
	}
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	c.taskLock.Lock()
	defer c.taskLock.Unlock()
	return allTaskDone(c.mapTasks) && allTaskDone(c.reduceTasks)
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.mMap = len(files)
	c.nReduce = nReduce
	c.mapTasks = make([]Task, len(files))
	for i := range c.mapTasks {
		c.mapTasks[i].Ttype = TaskTypeMap
		c.mapTasks[i].Status = TaskStatusIdle
		c.mapTasks[i].MapFileName = files[i]
	}
	c.reduceTasks = make([]Task, nReduce)
	for i := range c.reduceTasks {
		c.reduceTasks[i].Ttype = TaskTypeReduce
		c.reduceTasks[i].Status = TaskStatusIdle
	}

	c.server()
	return &c
}

