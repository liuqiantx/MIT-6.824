package mr

// 定义　Master 的结构
// type Master struct {
// 	reduceTaskQueuing  []mr.TaskQueue
// 	reduceTaskRunning  []mr.TaskQueue
// 	mapTaskQueuing     []mr.TaskQueue
// 	mapTaskRunning     []mr.TaskQueue
// 	filesName          []string
// 	isDone             bool
// }

// 发送任务的方法
// 简单版步骤（无文件读取错误，task 真假死的问题）
// 1.
// func (m *Master) SendTask()

// type Master struct
// func sendTask
// func makeMaster 
// TimeOut {getTimeoutTask collectTimeoutTask}

// 
import (
	"fmt"
	"time"
	"sync"
)

const (
	TIMEOUT = 10 * time.Second
)

type Master struct {
	// mutex sync.Mutex
	isDone bool

}

func (m *Master) schedule(args *RequestTaskArgs, reply *RequestTaskReply) error {
	// 完整任务分配流程（必须等所有 mapTask 完成后再开始 reduceTask）
	// master 要锁？仅　queue　上锁即可
	m.mutex.Lock()
	defer m.mutex.Unlock()

	// 需考虑超时任务的回炉重造
	// 分配任务的逻辑，先分配 map ，
	// 待 map 全部正确完成后（包括超时重分），
	// 才开始 reduce 任务的分配
}


func (m *Master) NotifyTaskDone() error {
	// 通知任务完成的流程
}


//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
// 	rpc.Register(m)
// 	rpc.HandleHTTP()
// 	//l, e := net.Listen("tcp", ":1234")
// 	sockaddr := masterSock()
// 	os.Remove(sockaddr)
// 	l, e := net.Listen("unix", sockaddr)
// 	if e != nil {
// 		log.Fatal("listen error:", e)
// 	}
// 	go http.Serve(l, nil)
}

func (m *Master) isDone() bool {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	return m.isDone
}

// 初始化 Master 
func makeMaster(files []string, nReduces int) *Master {
	m := Master{
		\\ 初始化 Master 结构体
	}

	for idx, file := range files {
		// 分配 mapTask
	}

	for i := 0; i < nReduces; i++ {
		// 初始化 reduceTask
	}

	m.Server() // 调用 rpc
	return &m
}

func (m *Master) initMapTask() {
	// 
}

func (m *Master) initReduceTask() {

}


// func (m *Master) mergeResultFiles(){
// 	// 将所有输出的结果合并为一个文件
// }



func (m *Master) sendMapTask(args *RequestTaskArgs, reply *RequestTaskReply) error {
	// 仅考虑 map 任务的发送逻辑，包含回炉重造
}

func (tq *TaskQueue) reQueueTask(taskinfo *TaskInfo) {
	// 超时任务的回炉重造
}

//  开始

package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"

const (
	MAP_TASK = "map"
	REDUCE_TASK = "reduce"
	TASK_WAIT = "wait"
	TASK_RUNNING = "running"
	TASK_END = "end"
)

type Master struct {
	mapTaskRunning          []mr.TaskQueue
	mapTaskQueuing          []mr.TaskQueue
	reduceTaskRunning       []mr.TaskQueue
	reduceTaskQueuing       []mr.TaskQueue
	mutex                   sync.Mutex  // 针对 master 的处理，需锁定后执行
	isDone                  bool
	// fileNames               []string
}

func (m *Master) lock() {
	m.mutex.Lock()
}

func (m *Master) unlock() {
	m.mutex.Unlock()
}

func (m *Master) SendTask(args *TaskRequestInfo, reply *TaskInfo) error{
	// 任务发送逻辑
	// 待判断任务全部执行完毕后，m.isDone == True
}

//
// 注册 Master server 
//
func (m *Master) startServer() {
	rpc.Register(m)
	rpc.HandleHTTP()
	sockaddr := getMasterSockAddr()
	// 解绑（删除 sock 文件），以避免端点占用报错
	if err := os.RemoveAll(sockaddr); err != nil {
		log.Fatal("remove sock file error:", err)
	}
	os.Remove(sockaddr)
	l, err := net.Listen("unix", sockaddr)
	if err != nil {
		log.Fatal("listen error:", err)
	}
	fmt.Println("Serving ...")
	go http.Serve(l, nil)
}

func (m *Master) stopServer() {

}

//
// main/mrmaster.go 需要调用此方法判断 master 中任务是否全部结束
//
func (m *Master) Done() bool {
	ret := false

	m.lock()
	if m.isDone {
		ret = true
		fmt.Println("All task is done, stop master server")
	}
	m.unlock()

	return ret
}

//
// main/mrmaster.go calls this function.
// 读取命令行输入，
// 将输入的文件传给 MakeMaster() 函数创建 master server 
// 当 m.Done 为 False 时，每次任务都休息 time.Second, 当为 True 时，休息一次后结束 main 函数
// 创建并初始化 Master 对象，然后启用 Master server
//
func MakeMaster(files []string, nReduce int) *Master {
	// 初始化 Master 结构
	m := Master{}

	// 根据输出文件初始化 mapTaskQueuing



	// 启动 Master server
	m.startServer()
	return &m
}

func (m *Master) initMapTask(files []string, nReduce int) error {
	// 构造 m 个 map task(m files)
	// 量长，初始化，迭代赋值TASK_WAIT
}

// 注意：mapTask 和 reduceTask 的初始化生成场景不同，输入的参数也不同

type TaskInfo struct {
	TaskType        string  // "map" or "reduce"
	TaskState       string  // "wait", "running", "end"，根据任务状态决定任务在 TaskQueue 中的转移方式
	StartTime       time.time   // 判断任务是否超时
	FileName        string  // 仅用于 MapTask
	FileIndex       int
	PartIndex       int     // 仅用于 ReduceTask,指定该 task 要处理哪一块 files
	NReduces        int   
	NInputFiles     int   
}

type Master struct {
	mapTaskRunning          []mr.TaskQueue
	mapTaskQueuing          []mr.TaskQueue
	reduceTaskRunning       []mr.TaskQueue
	reduceTaskQueuing       []mr.TaskQueue
	mutex                   sync.Mutex  // 针对 master 的处理，需锁定后执行
	isDone                  bool
	// fileNames               []string
}

func (m *Master) initReduceTask(taskInfo *TaskInfo) error {
	for i:= 0; i < taskInfo.NReduces; i++ {
		newReduceTaskInfo := TaskInfo{}
		newReduceTaskInfo.TaskType  = REDUCE_TASK
		newReduceTaskInfo.TaskState = TASK_WAIT  // 在判断任务是否结束时可用
		newReduceTaskInfo.PartIndex = i 
		newReduceTaskInfo.NInputFiles = m.NFiles

	}
}


type Master struct {
	mapTaskRunning          []mr.TaskQueue
	mapTaskQueuing          []mr.TaskQueue
	reduceTaskRunning       []mr.TaskQueue
	reduceTaskQueuing       []mr.TaskQueue
	mutex                   sync.Mutex  // 针对 master 的处理，需锁定后执行
	isDone                  bool
	NFiles                  int     // 创建　reduce 任务时，需要传给 taskInfo
}

// 将超时的任务队列重新添加至相应的等候任务队列中
func (m *Master) AppendTimeOutQueue() {
	for {
		time.Sleep(time.Duration(time.Second * 10))

		mapTimeoutQueue = m.mapTaskRunning.getTimeOutQueue()
		if mapTimeoutQueue.getLength() > 0 {
			m.mapTaskQueuing.lock()
			m.mapTaskQueuing.TaskArray = append(m.mapTaskQueuing.TaskArray, mapTimeoutQueue...)
			m.mapTaskQueuing.unlock()
		}

		reduceTimeoutQueue = m.reduceTaskRunning.getTimeOutQueue()
		if reduceTimeoutQueue.getLength() > 0 {
			m.reduceTaskQueuing.lock()
			m.reduceTaskQueuing.TaskArray = append(m.reduceTaskQueuing.TaskArray, reduceTimeoutQueue...)
		}
	}
}