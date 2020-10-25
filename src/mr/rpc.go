package mr

import (
	"time"
	"sync"
	"fmt"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
)


type RequestTaskArgs struct {
	// 
}

type RequestTaskReply struct {
	// NMaps ??
}


// 针对　taskQueue 的吞吐数据，锁死解锁等操作

// 关于 rpc 服务开启，停止，关闭的问题
// 暂停
// Shutdown is an RPC method that shuts down the Master's RPC server.
// func (mr *Master) Shutdown(_, _ *struct{}) error {
// 	debug("Shutdown: registration server\n")
// 	close(mr.shutdown)
// 	mr.l.Close() // causes the Accept to fail
// 	return nil
// }

// 开始
// startRPCServer starts the Master's RPC server. It continues accepting RPC
// calls (Register in particular) for as long as the worker is alive.
// func (mr *Master) startRPCServer() {
// 	rpcs := rpc.NewServer()
// 	rpcs.Register(mr)
// 	os.Remove(mr.address) // only needed for "unix"
// 	l, e := net.Listen("unix", mr.address)
// 	if e != nil {
// 		log.Fatal("RegstrationServer", mr.address, " error: ", e)
// 	}
// 	mr.l = l

// 	// now that we are listening on the master address, can fork off
// 	// accepting connections to another thread.
// 	go func() {
// 	loop:
// 		for {
// 			select {
// 			case <-mr.shutdown:
// 				break loop
// 			default:
// 			}
// 			conn, err := mr.l.Accept()
// 			if err == nil {
// 				go func() {
// 					rpcs.ServeConn(conn)
// 					conn.Close()
// 				}()
// 			} else {
// 				debug("RegistrationServer: accept error", err)
// 				break
// 			}
// 		}
// 		debug("RegistrationServer: done\n")
// 	}()
// }

// 关闭
// This must be done through an RPC to avoid race conditions between the RPC
// server thread and the current thread.
// func (mr *Master) stopRPCServer() {
// 	var reply ShutdownReply
// 	ok := call(mr.address, "Master.Shutdown", new(struct{}), &reply)
// 	if ok == false {
// 		fmt.Printf("Cleanup: RPC %s error\n", mr.address)
// 	}
// 	debug("cleanupRegistration: done\n")
// }



//  开始

package mr

import (
	"os"
	"strconv"
	"time"
)

// type RequestTaskArgs struct {
// 	// 
// }

const (
	MAP_TASK = "map"
	REDUCE_TASK = "reduce"
	TASK_WAIT = "wait"
	TASK_RUNNING = "running"
	TASK_END = "end"
)


type TaskInfo struct {
	TaskType        string  // "map" or "reduce"
	TaskState       string  // "wait", "running", "end"，根据任务状态决定任务在 TaskQueue 中的转移方式
	StartTime       time.time   // 判断任务是否超时
	FileName        string  // 仅用于 MapTask
	FileIndex       int
	PartIndex       int     // 仅用于 ReduceTask,指定该 task 要处理哪一块 files
	NReduces        int   
	NInputFiles     int  // 等于 MapTasksNum
}

type TaskQueue struct {
	TaskArray  []TaskInfo
	mutex     sync.Mutex
}

// 针对 TaskQueue 的一系列操作，如锁，吞，吐，删，测
func (tq *TaskQueue) lock() {
	tq.mutex.Lock()
}

func (tq *TaskQueue) unlock() {
	tq.mutex.Unlock()
}

func (tq *TaskQueue) getLength() int {
	return len(tq.TaskArray)
}

func (tq *TaskQueue) Push(taskInfo TaskInfo) {
	tq.lock()
	tq.TaskArray = append(tq.TaskArray, taskInfo)
	tq.unlock()
}

func (tq *TaskQueue) Pop() TaskInfo {
	tq.lock()

	length = tq.getLength()
	if length == 0 {
		tq.unlock()
		taskInfo := TaskInfo{}
		taskInfo.TaskState == TASK_END
		return taskInfo
	}

	taskInfo := tq.TaskArray[length - 1]
	tq.TaskArray = tq.TaskArray[: length - 1]
	tq.unlock()
	return taskInfo
}

func (tq *TaskQueue) Remove(fileIndex int, partIndex int) {
	tq.lock()

	length = tq.getLength()
	for i := 0; i < length; {
		taskInfo = tq.TaskArray[i]
		if (taskInfo.FileIndex == fileIndex) && (taskInfo.PartIndex == partIndex){
			tq.TaskArray = append(tq.TaskArray[: i], tq.TaskArray[i + 1 :]...)
		} else {
			i++
		}
	}

	tq.unlock()
}


// 获取 server 地址
func getMasterSockAddr() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}

// 关于任务超时的一系列针对 TaskInfo 的操作，包括超时时间判断，收集超时的任务队列
func (ti *TaskInfo) getStartTime() {
	ti.StartTime = time.Now()
}

// 超时判断，超时时间定为 10s 
func (ti *TaskInfo) isOutOfTime() {
	return time.Now().Sub(ti.StartTime) > time.Duration(time.Second*10)
}

func (tq *TaskQueue) getTimeOutQueue() []TaskInfo {
	taskQueue := make([]TaskInfo, 0)
	tq.lock()

	length = tq.getLength()
	for i := 0; i < length; {
		taskInfo = tq.TaskArray[i]
		if taskInfo.isOutOfTime() {
			taskQueue = taskQueue.Push(taskInfo)
			tq.TaskArray = append(tq.TaskArray[: i], tq.TaskArray[i + 1 :]...)
		} else {
			i++
		}
	}
	tq.unlock()
	return taskQueue
}

