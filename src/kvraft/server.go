package kvraft

import (
	"../labgob"
	"../labrpc"
	"log"
	"../raft"
	"sync"
	"sync/atomic"
	"bytes"
	"time"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}


const (
	GET  = "Get"
	PUT  = "Put"
	APPEND = "Append"
)

// 具体操作内容
type Op struct {
	Method  string
	Key     string
	Value    string
	ClientId  int64
	MsgId     int64
	RequestSeq   int64
}

// KVServer: 对数据应用操作；感知重复的 client 请求
type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big
	data  map[string]string
	persister  *raft.Persister

	lastApplied map[int64]int64  // 最近一次的应用记录
	notifyData    map[int64]chan NotifyMsg
}

// 发送任务给 raft
func (kv *KVServer) SendOpToRaft(op Op) (res NotifyMsg) {
	waitTimer := time.NewTimer(WaitTimeout)
	defer waitTimer.Stop()

	_, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		DPrintf("%v isn't leader, fail start ...\n", kv.me)
		res.Err = ErrWrongLeader
		return 
	}

	kv.mu.Lock()
	ch := make(chan NotifyMsg)
	kv.notifyData[op.RequestSeq] = ch
	kv.mu.Unlock()

	select {
	case res =<- ch:
		DPrintf("%v finish apply %v ...\n", kv.me, op)
		kv.mu.Lock()
		delete(kv.notifyData, op.RequestSeq)
		kv.mu.Unlock()
		return
	case <- waitTimer.C:
		DPrintf("%v wait timeout...\n", kv.me)
		kv.mu.Lock()
		delete(kv.notifyData, op.RequestSeq)
		kv.mu.Unlock()
		res.Err = ErrTimeout
		return
	}
}

// 更新 clerks 提交的日志
// 若请求不连续，则不更新
func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	DPrintf("%v receive Get : %v from %v \n", kv.me, args.Key, args.ClerkId)
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
		reply.IsLeader = false
		return
	}

	operation := Op{
		Method: GET,
		Key: args.Key,
		ClientId: args.ClerkId,
		MsgId: args.MsgId,
		RequestSeq: nrand(),
	}

	res := kv.SendOpToRaft(operation)
	reply.Err = res.Err
	if res.Err != ErrWrongLeader {
		reply.IsLeader = false
	}
	// todo: 当 kvserver 不属于多数服务器，则不应该完成 Get()
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	DPrintf("%v receive PutAppend : %v from %v \n", kv.me, args.Key, args.ClerkId)
	// 仅读取操作必须是 leader

	operation := Op{
		Method: args.Op,
		Key: args.Key,
		Value: args.Value,
		ClientId: args.ClerkId,
		MsgId: args.MsgId,
		RequestSeq: nrand(),
	}
	
	res := kv.SendOpToRaft(operation)
	reply.Err = res.Err
}

func (kv *KVServer) WaitApplyCh() {
	DPrintf("%v is waiting for applyCh ... \n", kv.me)
	for msg := range kv.applyCh {
		if !msg.CommandValid {
			kv.mu.Lock()
			kv.ApplySnapshot(kv.persister.ReadSnapshot())
			kv.mu.Unlock()
			continue
		}

		operation := msg.Command.(Op)
		var repeated bool
		if lastMsgId, ok := kv.lastApplied[operation.ClientId]; ok {
			if lastMsgId == operation.MsgId {
				DPrintf("%v get receive repeated msg ... \n", kv.me)
				repeated = true
			}
		} else {
			repeated = false
		}

		kv.mu.Lock()
		if !repeated {
			if operation.Method == PUT {
				kv.data[operation.Key] = operation.Value
			} else if operation.Method == APPEND {
				kv.data[operation.Key] += operation.Value
			}
		}

		kv.SaveSnapshot(msg.CommandIndex)
		if ch, ok := kv.notifyData[operation.RequestSeq]; ok {
			ch <- NotifyMsg{
				Err: OK,
				Value: kv.data[operation.Key],
			}
		}
		kv.mu.Unlock()
	}
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
// 关于快照 Lab 3b
// 
func (kv *KVServer) SaveSnapshot(idx int) {
	if kv.persister.RaftStateSize() < kv.maxraftstate || kv.maxraftstate == -1 {
		return
	}
	DPrintf("%v start saving snapshot, raft size is %v", kv.me, kv.persister.RaftStateSize())
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	if err := e.Encode(kv.data); err != nil {
		panic(err)
	}
	if err := e.Encode(kv.lastApplied); err != nil {
		panic(err)
	}
	kv.rf.DiscardPreviousLog(idx, w.Bytes())
}

func (kv *KVServer) ApplySnapshot(data []byte) {
	if len(data) < 1 || data == nil {
		return
	}
	DPrintf("%v start reading snapshot ...", kv.me)
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var lastApplied map[int64]int64
	var kvdata map[string]string
	if d.Decode(&kvdata) != nil || d.Decode(&lastApplied) != nil {
		panic("read snapshot err")
	} else {
		kv.lastApplied = lastApplied
		kv.data = kvdata
	}
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.dead = 0
	kv.data = make(map[string]string)
	kv.persister = persister
	kv.lastApplied = map[int64]int64{}
	kv.notifyData = map[int64]chan NotifyMsg{}

	DPrintf("reading snapshot ...")
	kv.ApplySnapshot(kv.persister.ReadSnapshot())
	go kv.WaitApplyCh()
	return kv
}