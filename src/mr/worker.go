package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
)

//
// 基本数据结构
//

// KeyValue -> 数据格式
type KeyValue struct {
	Key   string
	Value string
}

// ByKey -> 数据排序, 用于 reduce_func 前同 key 数据的聚堆
type ByKey []mr.KeyValue
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// 任务执行相关方法
//
// ihash -> 给不同的 key 分配区块, 保证不同文件中同一个 key 所在文件的 PartIndex 部分相同
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// CallTaskDone -> 任务完成时，向 master 汇报
func CallTaskDone(args *TaskDoneArgs, taskInfo *TaskInfo) {
	err := call("Master.TaskDone", &args, &taskInfo)
	if err != nil {
		log.Fatal("call task done err : %v", err)
	}
}

// mapWorker 执行 mapworker 的工作流程
func mapWorker(mapf func(string string) []KeyValue, taskInfo *TaskInfo) {
	fileName := taskInfo.FileName
	fileIndex := taskInfo.FileIndex
	fmt.Println("start map task on %s", fileName)

	// 打开并读文件
	file, err := os.Open(fileName)
	if err != nil {
		log.Fatal("open file err : %v", err)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatal("read file err : %v", err)
	}
	file.Close()

    // 执行并存储结果（存为临时文件）
	kva := mapf(fileName, string(content))
	nReduces := taskInfo.NReduces
	tmpFiles := make([]*os.File, nReduces)
	encoderFiles := make([]*json.Encoder, nReduces)
	for _, kv := range kva {
		storeKeyValue(kv, tmpFiles, encoderFiles)
	}

	for i, file := range tmpFiles {
		file.Close()
	}

	// 发送任务结束信息
	taskDoneArgs := TaskDoneArgs{
		TaskType:  MapTask,
		NReduces:  nReduces,
		FileIndex: fileIndex,
		TmpFiles:  tmpFiles,
	}
	callErr := call("Master.TaskDone", &taskDoneArgs, &taskInfo)
	if callErr != nil {
		log.Fatal("call task done err : %v", callErr)
	}
}

func reduceWorker(reducef func(string, []string) string, taskInfo *TaskInfo) {
	partIndex := taskInfo.PartIndex
	nFiles := taskInfo.NInputFiles
	fmt.Println("start reduce work on %v part", partIndex)

	intermediate := []KeyValue{}

	for i := 0; i < nFiles; i++ {
		fileName := makeMapOutFileName(i, partIndex)
		file, err := os.Open(fileName)
		if err != nil {
			log.Fatal("open file err : %v", err)
		}
		decoder := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := decoder.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
		file.Close()
	}

	// 执行并存储结果（临时文件）
	sort.Sort(ByKey(intermediate))

	tmpFile, err := ioutil.TemFile("", "tmp")
	if err != nil {
		log.Fatal("create temp file err : %v", err)
	}

	for i := 0; i < len(intermediate); {
		key := intermediate[i].Key
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == Key {
			j++
		}

		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(key, values)

		fmt.Fprintf(tmpFile, "%v %v\n", key, output)
	}
	tmpFile.Close()

	// 发送任务结束信息
	taskDoneArgs := TaskDoneArgs{
		TaskType:  ReduceTask,
		NReduces:  nReduces,
		PartIndex: partIndex,
		TmpFiles:  [tmpFile],
	}
	callErr := call("Master.TaskDone", &taskDoneArgs, &taskInfo)
	if callErr != nil {
		log.Fatal("call task done err : %v", callErr)
	}
}

func storeKeyValue(kv KeyValue, tmpFiles []*os.File, encoders []*json.Encoder) {
	partIndex := ihash(kv.Key) % len(tmpFiles)
	encoder := encoders[partIndex]

	if encoder == nil {
		tmpFile, err := ioutil.TemFile("", "tmp")
		if err != nil {
			log.Fatal("create temp file err : %v", err)
		}
		tmpFiles[partIndex] = tmpFile
		encoder = json.NewEncoder(tmpFile)
		encoders[partIndex] = encoder
	}

	err := encoder.Encode(kv)
	if err != nil {
		log.Fatal("json encode err : %v", err)
	}
}

//
// 主工作流程
// 
// Worker -> worker 的主工作流程，包括任务请求，任务执行
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	for {
		args := RequestTaskArgs{}
		reply := RequestTaskReply{}
		taskinfo := call("Master.SendTask", &args, &reply)
		switch taskInfo.TaskType {
		case MapTask:
			mapWorker(mapf, taskInfo)
		case ReduceTask:
			reducef(reducef, taskInfo)
		case EmptyTask:
			if taskInfo.TaskState == TaskFinished {
				fmt.Println("All task done")
				return
			} else {
				fmt.Println("All task is running")
			}
		default:
			panic("Invalid Task type")
		}
	}
}

//
//  rpc
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