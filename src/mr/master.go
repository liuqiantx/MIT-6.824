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

//
// 常量
//

// 任务类型和任务状态的标记符
const (
	MapTask      = "map"
	ReduceTask   = "reduce"
	EmptyTask    = "empty"
	TaskIdle     = "wait"
	TaskRunning  = "running"
	TaskFinished = "finished"
)

//
// 基本数据结构
//

// Master 做任务调度需要用到的信息结构
type Master struct {
	mapIdleQueue        TaskQueue
	mapRunningQueue     TaskQueue
	mapFinishedQueue    TaskQueue
	reduceIdleQueue     TaskQueue
	reduceRunningQueue  TaskQueue
	reduceFinishedQueue TaskQueue

	mutex  sync.Mutex // 针对 master 的处理，需锁定后执行
	isDone bool

	// 判断刚完成的 task 是否是新的
	finishedFileIndexes []int
	finishedPartIndexes []int

	NFiles   int
	NReduces int
}

//
// 常规方法
//

func (m *Master) lock() {
	m.mutex.Lock()
}

func (m *Master) unlock() {
	m.mutex.Unlock()
}

//
// 任务调度相关的方法
//

// 注意：mapTask 和 reduceTask 的初始化生成场景不同，输入的参数也不同
// initMapTask -> master 初始化 map task 的方法
func (m *Master) initMapTask(files []string, nReduce int) error {
	for idx, file := range files {
		newMapIdleTask := TaskInfo{
			TaskType:    MapTask,
			TaskState:   TaskIdle,
			NInputFiles: len(files),
			NReduces:    nReduce,
			FileName:    file,
			FileIndex:   idx,
		}
		m.mapIdleQueue.Push(newMapIdleTask)
	}
	return nil
}

// initReduceTask -> master 初始化 reduce task 的方法
func (m *Master) initReduceTask(taskInfo *TaskInfo) error {
	for i := 0; i < taskInfo.NReduces; i++ {
		newReduceIdleTask := TaskInfo{
			TaskType:    ReduceTask,
			TaskState:   TaskIdle,
			NInputFiles: taskInfo.NInputFiles,
			PartIndex:   i,
		}
		m.reduceIdleQueue.Push(newReduceIdleTask)
	}
	return nil
}

// SendTask -> Master做任务调度的流程
// ->先发 map,待 map 全部完成后才开始 reduce,当 reduce 全部完成时,发送状态为已完成的任务
func (m *Master) SendTask(args *RequestTaskArgs, reply *TaskInfo) error {
	m.lock()
	defer m.unlock()

	if len(m.finishedFileIndexes) < m.NFiles {
		taskInfo := m.mapIdleQueue.Pop()
		taskInfo.getStartTime()
		m.mapRunningQueue.Push(taskInfo)
		*reply = taskInfo
		fmt.Println("sent map task of %v", taskInfo.FileName)
		return nil
	}

	if len(m.finishedPartIndexes) < m.NReduces {
		taskInfo := m.reduceIdleQueue.Pop()
		taskInfo.getStartTime()
		m.reduceRunningQueue.Push(taskInfo)
		*reply = taskInfo
		fmt.Println("sent reduce task of %v", taskInfo.PartIndex)
		return nil
	}

	if len(m.finishedFileIndexes) == m.NFiles && len(m.finishedPartIndexes) == m.NReduces {
		taskInfo := TaskInfo{
			TaskType:  EmptyTask,
			TaskState: TaskFinished,
		}
		m.isDone = true
		*reply = taskInfo
		return nil
	} else {
		taskInfo := TaskInfo{
			TaskType:  EmptyTask,
			TaskState: TaskRunning,
		}
		*reply = taskInfo
		return nil
	}
}

// TaskDone -> worker 发送任务完成后 master 的操作逻辑
func (m *Master) TaskDone(args *TaskDoneArgs, taskInfo TaskInfo) error {
	m.lock()
	defer m.unlock()

	if args.TaskType == MapTask {
		if isElementInSLice(args.FileIndex, m.finishedFileIndexes) == true {
			return nil
		}

		// 更改任务所处队列，将文件索引添加至已完成队列
		m.finishedFileIndexes = append(m.finishedFileIndexes, args.FileIndex)
		m.mapFinishedQueue.Push(taskInfo)
		m.mapRunningQueue.Remove(taskInfo.FileIndex, taskInfo.PartIndex)

		// 文件重命名
		for i := 0; i < args.NReduces; i++ {
			name := makeMapOutFileName(args.FileIndex, i)
			os.Rename(args.TmpFiles[i].Name(), name)
		}

		fmt.Println("map task of %v is done", args.FileIndex)
	} else {
		if isElementInSLice(args.PartIndex, m.finishedPartIndexes) {
			return nil
		}

		m.finishedPartIndexes = append(m.finishedPartIndexes, args.PartIndex)
		m.reduceFinishedQueue.Push(taskInfo)
		m.reduceRunningQueue.Remove(taskInfo.FileIndex, taskInfo.PartIndex)

		name := makeReduceOutFileName(args.PartIndex)
		os.Rename(args.TmpOutputFile, name)

		fmt.Println("reduce task of %v is done", args.PartIndex)
	}
	return nil
}

// AppendTimeOutQueue -> master 将超时任务重新添加回待执行的任务队列中
func (m *Master) AppendTimeOutQueue() {
	for {
		time.Sleep(time.Duration(time.Second * 10))

		mapTimeoutQueue := m.mapRunningQueue.getTimeOutQueue()
		if len(mapTimeoutQueue) > 0 {

			m.mapIdleQueue.lock()
			m.mapIdleQueue.TaskArray = append(m.mapIdleQueue.TaskArray, mapTimeoutQueue...)
			m.mapIdleQueue.unlock()

			m.mapRunningQueue.lock()
			for _, taskInfo := range mapTimeoutQueue {
				m.mapRunningQueue.Remove(taskInfo.FileIndex, taskInfo.PartIndex)
			}
			m.mapRunningQueue.unlock()

		}

		reduceTimeoutQueue := m.reduceRunningQueue.getTimeOutQueue()
		if len(reduceTimeoutQueue) > 0 {

			m.reduceIdleQueue.lock()
			m.reduceIdleQueue.TaskArray = append(m.reduceIdleQueue.TaskArray, reduceTimeoutQueue...)
			m.reduceIdleQueue.unlock()

			m.reduceRunningQueue.lock()
			for _, taskInfo := range reduceTimeoutQueue {
				m.reduceRunningQueue.Remove(taskInfo.FileIndex, taskInfo.PartIndex)
			}
			m.reduceRunningQueue.unlock()
		}
	}
}

// Done -> master 任务完成时,主流程需要调用的方法
func (m *Master) Done() bool {
	m.lock()
	defer m.unlock()

	if m.isDone {
		fmt.Println("All task is done, stop master server")
		return true
	}

	return false
}

//
// master 初始化
//

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

// MakeMaster -> 创建并初始化 Master 对象，然后启用 Master server
func MakeMaster(files []string, nReduce int) *Master {
	// 初始化 Master 结构
	m := Master{}
	m.NFiles = len(files)
	m.NReduces = nReduce

	err := m.initMapTask(files, nReduce)
	if err == nil {
		fmt.Println("init map task done ... ")
	}

	// 同时收集超时任务
	go m.AppendTimeOutQueue()
	// 启动 Master server
	m.startServer()
	return &m
}
