package main

//
// simple sequential MapReduce.
//
// go run mrsequential.go wc.so pg*.txt
//

import "fmt"
import "../mr"
import "plugin"
import "os"
import "log"
import "io/ioutil"
import "sort"

// for sorting by key.
type ByKey []mr.KeyValue  // [{key, value}, {key, value},...]

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

func main() {
	if len(os.Args) < 3 {
		fmt.Fprintf(os.Stderr, "Usage: mrsequential xxx.so inputfiles...\n")
		os.Exit(1)
	}

	mapf, reducef := loadPlugin(os.Args[1])

	//
	// read each input file,
	// pass it to Map,
	// accumulate the intermediate Map output.
	//
	intermediate := []mr.KeyValue{}
	for _, filename := range os.Args[2:] {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		content, err := ioutil.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", filename)
		}
		file.Close()
		kva := mapf(filename, string(content))
		intermediate = append(intermediate, kva...)
	}

	//
	// a big difference from real MapReduce is that all the
	// intermediate data is in one place, intermediate[],
	// rather than being partitioned into NxM buckets.
	//

	sort.Sort(ByKey(intermediate))

	oname := "mr-out-0"
	ofile, _ := os.Create(oname)

	//
	// call Reduce on each distinct key in intermediate[],
	// and print the result to mr-out-0.
	//
	i := 0
	for i < len(intermediate) {
		j := i + 1#!/bin/sh

		#
		# basic map-reduce test
		#
		
		RACE=
		
		# uncomment this to run the tests with the Go race detector.
		#RACE=-race
		
		# run the test in a fresh sub-directory.
		rm -rf mr-tmp
		mkdir mr-tmp || exit 1
		cd mr-tmp || exit 1
		rm -f mr-*
		
		# make sure software is freshly built.
		(cd ../../mrapps && go build $RACE -buildmode=plugin wc.go) || exit 1
		(cd ../../mrapps && go build $RACE -buildmode=plugin indexer.go) || exit 1
		(cd ../../mrapps && go build $RACE -buildmode=plugin mtiming.go) || exit 1
		(cd ../../mrapps && go build $RACE -buildmode=plugin rtiming.go) || exit 1
		(cd ../../mrapps && go build $RACE -buildmode=plugin crash.go) || exit 1
		(cd ../../mrapps && go build $RACE -buildmode=plugin nocrash.go) || exit 1
		(cd .. && go build $RACE mrmaster.go) || exit 1
		(cd .. && go build $RACE mrworker.go) || exit 1
		(cd .. && go build $RACE mrsequential.go) || exit 1
		
		failed_any=0
		
		# first word-count
		
		# generate the correct output
		../mrsequential ../../mrapps/wc.so ../pg*txt || exit 1
		sort mr-out-0 > mr-correct-wc.txt
		rm -f mr-out*
		
		echo '***' Starting wc test.
		
		timeout -k 2s 180s ../mrmaster ../pg*txt &
		
		# give the master time to create the sockets.
		sleep 1
		
		# start multiple workers.
		timeout -k 2s 180s ../mrworker ../../mrapps/wc.so &
		timeout -k 2s 180s ../mrworker ../../mrapps/wc.so &
		timeout -k 2s 180s ../mrworker ../../mrapps/wc.so &
		
		# wait for one of the processes to exit.
		# under bash, this waits for all processes,
		# including the master.
		wait
		
		# the master or a worker has exited. since workers are required
		# to exit when a job is completely finished, and not before,
		# that means the job has finished.
		
		sort mr-out* | grep . > mr-wc-all
		if cmp mr-wc-all mr-correct-wc.txt
		then
		  echo '---' wc test: PASS
		else
		  echo '---' wc output is not the same as mr-correct-wc.txt
		  echo '---' wc test: FAIL
		  failed_any=1
		fi
		
		# wait for remaining workers and master to exit.
		wait ; wait ; wait
		
		# now indexer
		rm -f mr-*
		
		# generate the correct output
		../mrsequential ../../mrapps/indexer.so ../pg*txt || exit 1
		sort mr-out-0 > mr-correct-indexer.txt
		rm -f mr-out*
		
		echo '***' Starting indexer test.
		
		timeout -k 2s 180s ../mrmaster ../pg*txt &
		sleep 1
		
		# start multiple workers
		timeout -k 2s 180s ../mrworker ../../mrapps/indexer.so &
		timeout -k 2s 180s ../mrworker ../../mrapps/indexer.so
		
		sort mr-out* | grep . > mr-indexer-all
		if cmp mr-indexer-all mr-correct-indexer.txt
		then
		  echo '---' indexer test: PASS
		else
		  echo '---' indexer output is not the same as mr-correct-indexer.txt
		  echo '---' indexer test: FAIL
		  failed_any=1
		fi
		
		wait ; wait
		
		
		echo '***' Starting map parallelism test.
		
		rm -f mr-out* mr-worker*
		
		timeout -k 2s 180s ../mrmaster ../pg*txt &
		sleep 1
		
		timeout -k 2s 180s ../mrworker ../../mrapps/mtiming.so &
		timeout -k 2s 180s ../mrworker ../../mrapps/mtiming.so
		
		NT=`cat mr-out* | grep '^times-' | wc -l | sed 's/ //g'`
		if [ "$NT" != "2" ]
		then
		  echo '---' saw "$NT" workers rather than 2
		  echo '---' map parallelism test: FAIL
		  failed_any=1
		fi
		
		if cat mr-out* | grep '^parallel.* 2' > /dev/null
		then
		  echo '---' map parallelism test: PASS
		else
		  echo '---' map workers did not run in parallel
		  echo '---' map parallelism test: FAIL
		  failed_any=1
		fi
		
		wait ; wait
		
		
		echo '***' Starting reduce parallelism test.
		
		rm -f mr-out* mr-worker*
		
		timeout -k 2s 180s ../mrmaster ../pg*txt &
		sleep 1
		
		timeout -k 2s 180s ../mrworker ../../mrapps/rtiming.so &
		timeout -k 2s 180s ../mrworker ../../mrapps/rtiming.so
		
		NT=`cat mr-out* | grep '^[a-z] 2' | wc -l | sed 's/ //g'`
		if [ "$NT" -lt "2" ]
		then
		  echo '---' too few parallel reduces.
		  echo '---' reduce parallelism test: FAIL
		  failed_any=1
		else
		  echo '---' reduce parallelism test: PASS
		fi
		
		wait ; wait
		
		
		# generate the correct output
		../mrsequential ../../mrapps/nocrash.so ../pg*txt || exit 1
		sort mr-out-0 > mr-correct-crash.txt
		rm -f mr-out*
		
		echo '***' Starting crash test.
		
		rm -f mr-done
		(timeout -k 2s 180s ../mrmaster ../pg*txt ; touch mr-done ) &
		sleep 1
		
		# start multiple workers
		timeout -k 2s 180s ../mrworker ../../mrapps/crash.so &
		
		# mimic rpc.go's masterSock()
		SOCKNAME=/var/tmp/824-mr-`id -u`
		
		( while [ -e $SOCKNAME -a ! -f mr-done ]
		  do
			timeout -k 2s 180s ../mrworker ../../mrapps/crash.so
			sleep 1
		  done ) &
		
		( while [ -e $SOCKNAME -a ! -f mr-done ]
		  do
			timeout -k 2s 180s ../mrworker ../../mrapps/crash.so
			sleep 1
		  done ) &
		
		while [ -e $SOCKNAME -a ! -f mr-done ]
		do
		  timeout -k 2s 180s ../mrworker ../../mrapps/crash.so
		  sleep 1
		done
		
		wait
		wait
		wait
		
		rm $SOCKNAME
		sort mr-out* | grep . > mr-crash-all
		if cmp mr-crash-all mr-correct-crash.txt
		then
		  echo '---' crash test: PASS
		else
		  echo '---' crash output is not the same as mr-correct-crash.txt
		  echo '---' crash test: FAIL
		  failed_any=1
		fi
		
		if [ $failed_any -eq 0 ]; then
			echo '***' PASSED ALL TESTS
		else
			echo '***' FAILED SOME TESTS
			exit 1
		fi
		
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	ofile.Close()
}

//
// load the application Map and Reduce functions
// from a plugin file, e.g. ../mrapps/wc.so
//
func loadPlugin(filename string) (func(string, string) []mr.KeyValue, func(string, []string) string) {
	p, err := plugin.Open(filename)
	if err != nil {
		log.Fatalf("cannot load plugin %v", filename)
	}
	xmapf, err := p.Lookup("Map")
	if err != nil {
		log.Fatalf("cannot find Map in %v", filename)
	}
	mapf := xmapf.(func(string, string) []mr.KeyValue)
	xreducef, err := p.Lookup("Reduce")
	if err != nil {
		log.Fatalf("cannot find Reduce in %v", filename)
	}
	reducef := xreducef.(func(string, []string) string)

	return mapf, reducef
}
