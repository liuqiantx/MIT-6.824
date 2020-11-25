package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import "sync"
import "sync/atomic"
import "../labrpc"

// import "bytes"
// import "../labgob"

// １．单个 raft 需要维护哪些属性？
// 2. raft 哪些属性需要被持久化？
// 2.1 如何持久化，从 raft 读取哪些数据？只是读取，然后保存就行了吗？是否还需要其他操作？
// 2.2 如何恢复 raft 中持久化的数据，要恢复哪些？ -> 从 persist 中读取，然后填充到 raft 对应属性下
// 3. log entry 结构如何？是否需要考虑
// 4. 关于投票
// 4.1 申请投票需要准备哪些信息给投票人看？
// 4.2 投票人会给申请人回复什么信息？ -> 我是谁，我的结果是，我是哪一期的？
// 4.3 申请投票的详细流程如何？
// 4.4 流程中网络不稳定可能导致哪些问题？
// 5. test 后如何杀死 raft server
// 5.1 如何算杀死？ -> raft dead 属性的值属于 dead
// 5.2 杀死后　raft server 有哪些属性会发生改变
// 5.3 测试代码中哪些时候用到了 杀死 raft
// 6. 创建新的 raft server
// 6.1 创建流程如何？
// 6.2 若是基于给定的缓存创建，怎么做？
// 6.3 对于需要较长时间的步骤，需要用到 goroutine

//
// 常量
//
const (
	Candidate = 0
	Follower = 1
	Leader = 2
	HeartBeatInterval = 100
	ElectionTimeout = 150
	ElectionRandomTimeRange = 150

)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//


//
// raft 常规操作，如状态持久化，获取状态等
// 
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).


	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

//
// 关于 leader election 
//
//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
}

type RequestVoteReply struct {
	// Your data here (2A).
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func isOutOfDate(args *RequestVoteArgs, rf *Raft) bool {
	// 5.4.1 选举限制的判断
	return false
}

func (rf *Raft) changeRaftState(state int) {
	// 状态切换 switch 
}

func (rf *Raft) isElectionTimeout() bool {
	// 
	return false
}

func (rf *Raft) startLeaderElection() {
	// 长流程
}

func (rf *Raft) runLeader() {
	// election 时 leader 要做的事
}

func (rf *Raft) runCandidate() {
	// 注意都需要有休息时间
}

func (rf *Raft) runFollower() {
	//
}

func (rf *Raft) Run(){
	// 
}


// 
// 关于 log replication
// 
type LogEntry struct {
	// 
}

type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

type AppendLogEntriesArgs struct {
	// 
}

type AppendLogEntriesReply struct {
	// 
}

func (rf *Raft) AppendLogEntries(args *AppendLogEntriesArgs, reply *AppendLogEntriesReply) {
	// 超长的添加流程
}

func (rf *Raft) sendAppendLogEntries(server int, args *AppendLogEntriesArgs, reply *AppendLogEntriesReply) bool {
	//
	return false
}

func (rf *Raft) SendApplyMsg() {
	// 发送 log 中的未应用的 msgs
}


//
// 共有流程
//
func (rf *Raft) sendHeartbeat() {
	// 超长流程
}


//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())


	return rf
}