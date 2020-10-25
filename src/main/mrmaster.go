package main

//
// start the master process, which is implemented
// in ../mr/master.go，职责，发送任务
//
// go run mrmaster.go pg*.txt
//
// Please do not change this file.
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
	m := mr.MakeMaster(os.Args[1:], 10) // os.Args 第一个值为操作路径
	for m.Done() == false {
		time.Sleep(time.Second) // master 需要有 Done 的判断，以增加休息时间
	}

	time.Sleep(time.Second) // 发任务的过程会有休息，以应对需要等到的情况
}
