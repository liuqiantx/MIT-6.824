package mr

// 请求 taskinfo 的程序

// func worker(mapf reducef)

// func mapWorker
// func reduceWorker
// func requestForTask

type KeyValue struct {
	Key string
	Value string
}

type KeyValueArray []KeyValue

// 文件中已有部分

func worker() {
	// 步骤一：请求任务
	// 步骤二：解析任务
	// 步骤三：切换任务分支
	// 步骤四： 执行任务
	// 步骤五：
	for {
		// 根据　replyinfo 决定走哪一类 worker
	}
}

func requestForTask() TaskInfo {
	// 步骤一：请求 task 任务，-> 与 master rpc 通信 -> 得到 taskinfo 
}


func handleMapTask() {
	// 对　map task 的处理，打开文件读文件执行 map 保存临时文件最终文件等一系列操作
}

func handleReduceTask() {
	// 
}

// func notifyMapTaskDone() {
// 	// 
// }

// func notifyReduceTaskDone() {

// }



// 调用 rpc 模块，请求任务
// func call(rpcname string, args interface{}, reply interface{}) bool {
// 	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
// 	sockaddr := masterSock()
// 	c, err := rpc.DialHTTP("unix", sockaddr)
// 	if err != nil {
// 		log.Fatal("dialing:", err)
// 	}
// 	defer c.Close()

// 	err = c.Call(rpcname, args, reply)
// 	if err == nil {
// 		return true
// 	}

// 	fmt.Println(err)
// 	return false
// }

func makeMapOutFileName() string {
	// 构造 map 输出文件的名
}

func makeReduceOutFileName() string {

}

type worker struct {
	id int  // 报告 worker 问题时用到
	mapf  func(string, string) []KeyValue
	reducef func(string, []string) string

}
func (w *Worker) run() {
	// 仅管 task　获取, （死活判断？？？）执行 task
}

func (w *Worker) requestForTask() TaskInfo {
	// 获取任务
}

func (w *Worker) doTask(taskinfo TaskInfo) {
	// switch
}

func (w *Worker) reportTaskStatus(taskinfo TaskInfo, done bool, err error) {
	// 获取 task 执行状况后报告状况
}

//  开始开始

package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"


//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// 通过 hash 的方式为 key 从 range [0: NReduce] 中选 PartIndex,
// 从而保证不同文件中的同一个 key 所在文件的 PartIndex 部分相同,
// 即被同一个 ReduceTask 处理（一个 ReduceTask 处理一组同一 PartIndex 的文件) 
// 
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}


//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the master.
	// 包含跟 master 请求任务的部分（详细部分，可在下方）
	// 第一次请求任务CallSendTask()
	// 执行任务(根据reply 结果，解析并 switch 到特定任务)
	// 判断任务执行完成
	// 再次请求任务 CallSendTask()
	// 直到返回任务全部执行完毕，则退出

}

func CallSendTask(args *TaskRequestInfo, reply *TaskInfo) {

	call("Master.SendTask", &args, &reply)

}
//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// 获取 server 地址 (Master server 的注册地址，可调用 Master 拥有的各种方法)
	// 连接该地址
	// 调用 Master 指定的方法（如 SendTask）
	// （该方法执行完毕后，会将结果传入给定的 reply 存储位置，通过获取指针值即可获得 server 返回的结果）
	sockaddr := getMasterSockAddr()
	c, err := rpc.DialHTTP("unix", sockaddr)
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
