package main

//
// a MapReduce pseudo-application to test that workers
// execute reduce tasks in parallel.　
//
// go build -buildmode=plugin rtiming.go
//

import "../mr"
import "fmt"
import "os"
import "syscall"
import "time"
import "io/ioutil"

func nparallel(phase string) int {
	// create a file so that other workers will see that
	// we're running at the same time as them.　
	// 记录阶段和线程 id
	pid := os.Getpid()
	myfilename := fmt.Sprintf("mr-worker-%s-%d", phase, pid) // "mr-worker-reduce-1024"
	err := ioutil.WriteFile(myfilename, []byte("x"), 0666)
	if err != nil {
		panic(err)
	}

	// are any other workers running?
	// find their PIDs by scanning directory for mr-worker-XXX files.
	// 打开 current dir，获取 dir 下所有文件的文件名（最多100万个）
	dd, err := os.Open(".")
	if err != nil {
		panic(err)
	}
	names, err := dd.Readdirnames(1000000)
	if err != nil {
		panic(err)
	}
	ret := 0
	for _, name := range names {
		var xpid int
		pat := fmt.Sprintf("mr-worker-%s-%%d", phase) // "mr-workder-reduce-%d"
		n, err := fmt.Sscanf(name, pat, &xpid) // name -> "mr-worker-reduce-1024"
		// 如果存在一个重合
		if n == 1 && err == nil { 
			err := syscall.Kill(xpid, 0) //　杀死进程组
			if err == nil {
				// if err == nil, xpid is alive.
				ret += 1
			}
		}
	}
	dd.Close()

	time.Sleep(1 * time.Second)

	err = os.Remove(myfilename)
	if err != nil {
		panic(err)
	}

	return ret
}

func Map(filename string, contents string) []mr.KeyValue {

	kva := []mr.KeyValue{}
	// 将新的元素组件添加至 kva slice 中 
	kva = append(kva, mr.KeyValue{"a", "1"})
	kva = append(kva, mr.KeyValue{"b", "1"})
	kva = append(kva, mr.KeyValue{"c", "1"})
	kva = append(kva, mr.KeyValue{"d", "1"})
	kva = append(kva, mr.KeyValue{"e", "1"})
	kva = append(kva, mr.KeyValue{"f", "1"})
	kva = append(kva, mr.KeyValue{"g", "1"})
	kva = append(kva, mr.KeyValue{"h", "1"})
	kva = append(kva, mr.KeyValue{"i", "1"})
	kva = append(kva, mr.KeyValue{"j", "1"})
	return kva
}

func Reduce(key string, values []string) string {
	n := nparallel("reduce")

	val := fmt.Sprintf("%d", n)

	return val
}
