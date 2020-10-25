package mr

import (
	"os"
	"strconv"
	"time"
)

const (
	MAP_TASK = "map"
	REDUCE_TASK = "reduce"
	TASK_WAIT = "wait"
	TASK_RUNNING = "running"
	TASK_END = "end"
)

type RequestTaskArgs struct {
	// 
}

type RequestTaskReply struct {
	reply  TaskInfo
}

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

