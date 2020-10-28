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
	MapTask     = "map",
	ReduceTask  = "reduce",
	TaskIdle    = "wait",
	TaskRunning = "running",
	TaskEnd     = "end"
)

type Master struct {
	mapTaskRunning    []TaskQueue
	mapTaskQueuing    []TaskQueue
	reduceTaskRunning []TaskQueue
	reduceTaskQueuing []TaskQueue
	mutex             sync.Mutex // 针对 master 的处理，需锁定后执行
	isDone            bool
	NFiles            int // 创建　reduce 任务时，需要传给 taskInfo
}

func (m *Master) lock() {
	m.mutex.Lock()
}

func (m *Master) unlock() {
	m.mutex.Unlock()
}

func (m *Master) SendTask(args *TaskRequestInfo, taskInfo *TaskInfo) error {
	// 完整任务分配流程（必须等所有 mapTask 完成后再开始 reduceTask）
	m.lock()
	defer m.unlock()
	// 任务发送逻辑
	if m.mapTaskQueuing.getLength() > 0 {
		taskInfo := m.mapTaskQueuing.Pop()
		taskInfo.getStartTime()
		m.mapTaskRunning.Push(taskInfo)
		*reply = taskInfo
		fmt.Println("sent map task of %v", taskInfo.FileName)
		return nil
	}

	if m.reduceTaskQueuing.getLength() > 0 {
		taskInfo := m.reduceTaskQueuing.Pop()
		taskInfo.getStartTime()
		m.reduceTaskRunning.Push(taskInfo)
		*reply = taskInfo
		fmt.Println("sent reduce task of %v", taskInfo.FileName)
		return nil
	}

	if m.mapTaskRunning.getLength() == 0 && m.reduceTaskRunning.getLength() == 0 {
		reply.TaskState = TASK_END
		m.isDone = true
		return nil
	} else {
		reply.TaskState = TASK_RUNNING
		return nil
	}
}

func (m *Master) TaskDone(taskInfo *TaskInfo) error {
	switch taskInfo.TaskType {
	case MAP_TASK:
		m.mapTaskRunning.Remove(taskInfo.FileIndex, taskInfo.PartIndex)
		fmt.Println("map task on %v file is complete", taskInfo.FileName)
		// 当所有 map 任务完成后，才能开始 reduce 任务
		if m.mapTaskRunning.getLength() == 0 && m.mapTaskQueuing.getLength() == 0 {
			err := m.initReduceTask(taskInfo)
			if err == nil {
				fmt.Println("init reduce task done ... ")
			}
		}
		break
	case REDUCE_TASK:
		m.reduceTaskRunning.Remove(taskInfo.FileIndex, taskInfo.PartIndex)
		fmt.Println("reduce task on %v part is complete", taskInfo.PartIndex)
		break
	default:
		panic("wrong task done")
	}
	return nil
}

//
// 注意：mapTask 和 reduceTask 的初始化生成场景不同，输入的参数也不同
//
func (m *Master) initMapTask(files []string, nReduce int) error {
	for idx, file := range files {
		taskInfo := TaskInfo{
			TaskType:    MAP_TASK,
			TaskState:   TASK_WAIT,
			FileName:    file,
			FileIndex:   idx,
			NReduces:    nReduce,
			NInputFiles: len(files),
		}
		m.mapTaskQueuing.Push(taskInfo)
	}
	return nil
}

func (m *Master) initReduceTask(taskInfo *TaskInfo) error {
	for i := 0; i < taskInfo.NReduces; i++ {
		newReduceTaskInfo := TaskInfo{}
		newReduceTaskInfo.TaskType = REDUCE_TASK
		newReduceTaskInfo.TaskState = TASK_WAIT // 在判断任务是否结束时可用
		newReduceTaskInfo.PartIndex = i
		newReduceTaskInfo.NInputFiles = m.NFiles
		m.reduceTaskQueuing.Push(newReduceTaskInfo)
	}
	return nil
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

// 注册 Master server
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

// main/mrmaster.go 需要调用此方法判断 master 中任务是否全部结束
func (m *Master) Done() bool {
	m.lock()
	defer m.unlock()

	if m.isDone() {
		fmt.Println("All task is done, stop master server")
		return true
	}

	return false
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
	m.NFiles = len(files)

	err := m.initMapTask(files, nReduce)
	if err == nil {
		fmt.Println("init map task done ... ")
	}

	go m.AppendTimeOutQueue()
	// 启动 Master server
	m.startServer()
	return &m
}
