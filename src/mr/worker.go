package mr

import (
	"strconv"
	"fmt"
	"log"
	"net/rpc"
	"hash/fnv"
)


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
// 1. 请求任务
// 2. 解析任务,得到任务类型
// 3. 根据指定的任务类型,切换任务分支
// 4. 在特定分支执行特定任务
// 5. 迭代 1~4, 直到任务类型为"全部完成"时,输出任务完成,退出 Worker 
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
}

func CallSendTask() *TaskInfo {
	args := RequestTaskArgs{}  // TODO 是否需要请求体结构,需要待定
	reply := TaskInfo{}
	call("Master.SendTask", &args, &reply)
	return &reply
}

// TODO 
func CallTaskDone(taskInfo *TaskInfo){
	call("Master.TaskDone", &taskInfo)
	return &taskInfo
}


func mapWorker(mapf func(string string) []KeyValue, taskInfo *TaskInfo) {
	// 1. 解析任务信息,获取要处理的文件名,文件索引(fi)
	// 2. 打开文件,读取文件内容
	// 3. 对文件内容执行 map_function
	// 4. 获取任务信息中 NReduces 数量
	// 5. 生成 N 个临时文件(临时保存 KeyValue), N 个编码文件(保存 KeyValue 的编码形式), N 个保存最终 KeyValue 的数据文件.(当 NReduce 为 10 时,文件名为 mr-fi-0 ~ mr-fi-9)
	// 6. 迭代对 KeyValue 中的 Key 进行 hash % NReduce,所得结果即该 Key 所属的 PartIndex.(如,当 {"happy", 1} hash 后的结果为 8,则 {"happy", 1} 保存至文件名为 mr-fi-8 的文件中)
	// 7. 将 KeyValue 保存至对应索引的临时文件中
	// 8. 待所有临时文件都保存完毕后,再将文件结果保存至编码文件和最终文件
	// 9. 报告任务结束
}

func reduceWorker(reducef func(string, []string) string, taskInfo *TaskInfo) {
	// 1. 解析任务信息,获取处理的数据的 PartIndex,假设为 1
	// 2. 初始化中间数据的存放 slice, 假设为 intermediate
	// 3. 依次迭代 mapWorker 结果文件夹中的结果文件,当文件名的 PartIndex 部分为 1 时,进行下一步处理
	// 4. 打开文件,读取文件内容
	// 5. 将文件内容添加至 intermediate 中
	// 6. 对 intermediate 中的数据进行排序
	// 7. 按顺序读取 intermediate 中的 KeyValue，对于每一组同 Key 数据，执行依次 reduce function．
	// 8. 将结果依次写入 mr-out-1 文件中
	// 9. 待所有结果存储完毕，报告任务结束
}
func makeMapOutFileName(fileIndex int, partIndex int) string {
	return "mr-" + strconv.Itoa(fileIndex) + "-" + strconv.Itoa(partIndex)
}

func makeReduceOutFileName(partIndex int) string {
	return "mr-out-" + strconv.Itoa(partIndex)
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
