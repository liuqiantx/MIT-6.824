package mr

import (
	"os"
	"strconv"
	"sync"
	"time"
)

//
// 关于 rpc 通信的信息结构
//

// TaskInfo -> 任务中包含的信息
type TaskInfo struct {
	// common
	TaskType    string    // "map" or "reduce"
	TaskState   string    // "idle", "running", "end"，根据任务状态决定任务在 TaskQueue 中的转移方式
	StartTime   time.Time // 判断任务是否超时
	NInputFiles int       // 等于 MapTasksNum
	NReduces    int

	// map task
	FileName  string // 仅用于 MapTask
	FileIndex int

	// reduce task
	PartIndex int // 仅用于 ReduceTask,指定该 task 要处理哪一块 files
}

// TaskDoneArgs 发送任务完成请求时需要用到的信息结构
type TaskDoneArgs struct {
	// common
	TaskType string
	NReduces int

	// map task done
	FileIndex int
	TmpFiles  []*os.File // 对于 map task，[]中多个值，对于 reduce task，[]中一个值

	// reduce task done
	PartIndex     int
	TmpOutputFile string
}

// RequestTaskArgs -> 请求任务时的参数结构
type RequestTaskArgs struct {
}

// TaskQueue -> 存储任务队列
type TaskQueue struct {
	TaskArray []TaskInfo
	mutex     sync.Mutex
}

//
// 关于任务队列
//
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

// Push ->　添加任务
func (tq *TaskQueue) Push(taskInfo TaskInfo) {
	tq.lock()
	tq.TaskArray = append(tq.TaskArray, taskInfo)
	tq.unlock()
}

// Pop -> 从任务队列中发出任务
func (tq *TaskQueue) Pop() TaskInfo {
	tq.lock()

	length := tq.getLength()
	if length == 0 {
		tq.unlock()
		taskInfo := TaskInfo{}
		taskInfo.TaskState = TaskRunning
		return taskInfo
	}

	taskInfo := tq.TaskArray[length-1]
	tq.TaskArray = tq.TaskArray[:length-1]
	tq.unlock()
	return taskInfo
}

// Remove -> 移除任务队列中的指定任务
func (tq *TaskQueue) Remove(fileIndex int, partIndex int) {
	tq.lock()

	length := tq.getLength()
	for i := 0; i < length; {
		taskInfo := tq.TaskArray[i]
		if (taskInfo.FileIndex == fileIndex) && (taskInfo.PartIndex == partIndex) {
			tq.TaskArray = append(tq.TaskArray[:i], tq.TaskArray[i+1:]...)
		} else {
			i++
		}
	}

	tq.unlock()
}

//
// 任务超时
//

// 针对 TaskInfo 的一系列操作，包括获取开始时间，超时时间判断，收集超时的任务队列
func (ti *TaskInfo) getStartTime() {
	ti.StartTime = time.Now()
}

// 超时判断，超时时间定为 10s
func (ti *TaskInfo) isOutOfTime() bool {
	return time.Now().Sub(ti.StartTime) > time.Duration(time.Second*10)
}

func (tq *TaskQueue) getTimeOutQueue() []TaskInfo {
	taskQueue := make([]TaskInfo, 0)
	tq.lock()

	length := tq.getLength()
	for i := 0; i < length; {
		taskInfo := tq.TaskArray[i]
		if taskInfo.isOutOfTime() {
			taskQueue = append(taskQueue, taskInfo)
			tq.TaskArray = append(tq.TaskArray[:i], tq.TaskArray[i+1:]...)
		} else {
			i++
		}
	}
	tq.unlock()
	return taskQueue
}

//
// 获取 server 地址
//
func getMasterSockAddr() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
