package main

//
// start the master process, which is implemented
// in ../mr/master.go
//
// cmd : go run mrmaster.go pg*.txt
//
// 流程：读文件 -> 利用文件创任务 -> (执行任务) -> 任务全部完成即停止
//

import "../mr"
import "time"
import "os"
import "fmt"

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "Usage: mrmaster inputfiles...\n")
		os.Exit(1)
	}

	// 接收的参数全是待处理的文件, nReduce 为 10
	m := mr.MakeMaster(os.Args[1:], 10)  // os.Args 第一个值为操作路径
	for m.Done() == false {
		time.Sleep(time.Second) 
	}

	time.Sleep(time.Second) // 发任务的过程会有休息，以应对需要等待的情况
}
