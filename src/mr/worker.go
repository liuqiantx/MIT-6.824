package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"


type KeyValue struct {
	Key   string
	Value string
}

// 
// 数据排序
// 用于 reduce_func 前同 key 数据的聚堆
// 
type ByKey []mr.KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }


// 
// 给不同的 key 分配区块
// 保证不同文件中同一个 key 所在文件的 PartIndex 部分相同
// （每个 reduceWorker 处理一块相同 PartIndex 的文件) 
// 
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}


//
// main/mrworker.go calls this function.
// 完整的 worker 工作流程
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
		for {
			taskInfo := CallSendTask()
			switch taskInfo.TaskType {
			case MAP_TASK:
				mapWorker(mapf, taskInfo)
			case REDUCE_TASK:
				reducef(reducef, taskInfo)
			case TASK_END:
				fmt.Println("All task done")
				return 
			default:
				panic("Invalid Task type")
			}
		}

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

func mapWorker(mapf func(string string) []KeyValue, taskInfo *TaskInfo) {

	// 从打开文件读文件到执行 mapf 再到分文件存中间结果中间文件，保存最终结果
	// 调用任务结束
}

func reduceWorker(reducef func(string, []string) string, taskInfo *TaskInfo) {
	// 从解析任务到读文件到排序数据到依次执行 reduce_func 再到保存中间文件，再到保存最终
	// 再到调用任务结束
	// sort.Sort(ByKey(intermediate))
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
