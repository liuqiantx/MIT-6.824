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
// copy from main/mrsequential
//
// reduce_func 中所有相关文件中的数据存储进 intermediate_data 之后需要用到
type ByKey []mr.KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

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

	// 步骤一：请求任务
	// 步骤二：解析任务
	// 步骤三：切换任务分支
	// 步骤四： 执行任务

	// uncomment to send the Example RPC to the master.
	// 包含跟 master 请求任务的部分（详细部分，可在下方）
	// 第一次请求任务CallSendTask()
	// 执行任务(根据reply 结果，解析并 switch 到特定任务)
	// 判断任务执行完成
	// 再次请求任务 CallSendTask()
	// 直到返回任务全部执行完毕，则退出

}

func doTask(taskinfo TaskInfo) {
	// switch
}

// ??? 需要再思考
func CallSendTask() *TaskInfo {
	// args := ExampleArgs{}  -> ???
	reply := TaskInfo
	call("Master.SendTask", &args, &reply)
	return &reply
}

func CallTaskDone(taskInfo *TaskInfo){
	call("Master.TaskDone", &taskInfo)
	return &taskInfo
}

func mapWorker(map func(string string) []KeyValue, taskInfo *TaskInfo) {

	// 从打开文件读文件到执行 mapf 再到分文件存中间结果中间文件，保存最终结果
	// 调用任务结束
}

func reduceWorker(taskInfo TaskInfo) {
	// 从解析任务到读文件到排序数据到依次执行 reduce_func 再到保存中间文件，再到保存最终
	// 再到调用任务结束
}
func makeMapOutFileName() string {
	// 构造 map 输出文件的名
}

func makeReduceOutFileName() string {

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
