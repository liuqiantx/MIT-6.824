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

import (
	"sync"
	"sync/atomic"

	"../labrpc"
)

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
	Candidate               = 0
	Follower                = 1
	Leader                  = 2
	HeartBeatInterval       = 100
	ElectionTimeout         = 150
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

	state     int         // 服务所处的状态，领导者，候选人或是下属

	// 需持久化的
	currentTerm int         //
	votedFor    int         //
	log         []*LogEntry //

	// leader 容易丢失的
	matchIndex []int // 领导者所维护的对各个下属服务的下一个待匹配索引 (init lastLogIndex + 1)
	nextIndex  []int // 领导者所维护的与各个下属服务匹配上的最高索引 (init 0),各个 follower 间如何区分？按在 peers 中的顺序？

	// 所有 server 都容易丢失的
	commitIndex int // 最近的一次已提交的日志的索引
	lastApplied int // 最近一次已应用的日志的索引
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
	term         int // 候选者所处的任期
	candidateId  int // 候选者的 id
	lastLogIndex int // 候选者的最新日志的索引,用于资格核验
	lastLogTerm  int // 候选者的最新日志的所处任期,用于资格核验
}

type RequestVoteReply struct {
	term        int  // follower 所处的任期,用于让候选者更新自己的任期及状态
	voteGranted bool // follower 是否投票给 candidate
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// 当候选者的 term < 我的 currentTerm 时，拒绝
	// 当我还未投票给任何人时 (votedFor is null)，或者我已经投给了该候选者时，当候选者的日志未落后于我的，我会给他投一票
	// 当候选者的 term > 我的 currentTerm 时, 会将 currentTerm 转换为候选者的 term
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
	// 我会先开启新一轮任期
	// 给自己投票
	// 开启新一轮选举阶段，重置选举结束倒计时(倒计时随机 150 ~ 300)
	// 给 cluster 中所有团队成员发送 requestVote rpc
	// 当收到超过一半团队成员关于我担任指定任期领导者的投票时,我的状态改为 leader.
	// 当出现网络分区时,同一时期可能出现几个领导,但各个领导的任期不一样
	// 每个 server 同一任期最多投一次票,以谁先来,先投谁的原则
}

func (rf *Raft) runLeader() {
	// election 时 leader 要做的事
	// 一旦当上领导人，马上发送 appendEntries rpc 给团队中的所有成员(同一区域内的),且会定期发送,避免任期过期,定期如何设定,需要注意???
	// 当从客户那里收到命令时,先将命令日志添加到本地,只有在命令应用到状态机后,才给客户回复(并不是日志提交就给回复)
	// 当 last log index (到底是哪个?) >= nextIndex (为 followers 记录的),给该 follower 发送 sendAppendEntries rpc,由 nextIndex 开始,即 AppendLogEntriesArgs 中的 preLogIndex 为 nextIndex -1
	// 若成功了,则更新 leader raft 中为该 follower 维护的 nextIndex & matchIndex
	// 若失败了,则降低 nextIndex,然后再次 sendAppendEntries rpc,至于 nextIndex 如何降低,是有捷径可走的. --> ?
	// 需要对 commitIndex 做核对,看是否是对的值,核对方法:当存在一个 log 的 Index > 我记录的 commitIndex,然后该 index 所处的任期为我当前的任期,并且大部分下属们匹配上的 index >= N,基于日志一定单调递增的原则, 此时需要将 commitIndex = N -> 什么情况下会是这样的
	// 当 leader 发现存在 term 比自己大的情况时,会将自己的状态改成 follower
}

func (rf *Raft) runCandidate() {
	// 注意都需要有休息时间
	// 一旦状态转换为 candidate，立马开始选举
	// 一旦收到超 1/2 的团队投票,则将自己的状态转变为领导人.
	// 当收到新的 安好信息，且该信息的发送者的任期不低于自己,立马将自己的状态转变为 follower.(在等待投票的过程中)
	// 若安好信息发送者的任期低于自己的,他会拒绝信息,并且继续投票.
	// 当选举倒计时结束时，开启新一轮选举
}

func (rf *Raft) runFollower() {
	// raft 状态由 follower 开始,
	// 除非从 candidate / leader 处收到非法的信息,或是超时都没收到信息,否则它会一直保持 follower 的状态
	//　仅会给 candidate & leader 回复
	// 每次投票完会重置选举倒计时，当选举倒计时结束，未收到领导的安好信息，或是其他人的投票信息，我会将自己的状态转换为 candidate，然后开始新一轮选举
	// 当 heartBeatInternal 结束或者 electionTimeout 到来, follower 会将自己的状态改为 candidate
}

func (rf *Raft) Run() {
	//
}

//
// 关于 log replication
//
type LogEntry struct {
	term  int // log 发布时所处的时期
	Command  interface{}  // 任务详情
	CommandIndex  int  // 任务索引
}

type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

type AppendLogEntriesArgs struct {
	term         int         // 领导者的任期
	leaderId     int         // 当客户发错消息给下属时,下属可以告诉客户领导是谁
	preLogIndex  int         // 前一日志的索引,用于资格核验
	preLogTerm   int         // 前一日志所处的任期,用于资格核验
	entries      []*LogEntry // 要添加的日志信息,为了效率,可能一次添加多条
	leaderCommit int         // 领导通知提交的日志索引
}

type AppendLogEntriesReply struct {
	term    int  // 告知领导我目前所处的日志,以便领导更新自己,然后领导转变状态
	success bool // 当下属前一日志与领导相符时,回复成功
}

func (rf *Raft) AppendLogEntries(args *AppendLogEntriesArgs, reply *AppendLogEntriesReply) {
	// 超长的添加流程
	// 当领导 term < 我的 currentTerm 时,拒绝
	// 当前一日志所处的任期及索引与领导不匹配时,拒绝
	// 当存在日志冲突时,删除该日志以及其后的所有日志.(索引相同,但所处的任期不同)
	// 添加所有领导有,但我没有的日志(保证顺序一致)
	// 若 leaderCommit > 我的 commitIndex, 则将 commitIndex = min(leaderCommit, 最新日志的索引) ,之后会随着我的日志更新, commitIndex 增大,直到跟上领导的进程
	// 当领导的 term > 我的 currentTerm 时, 会将 currentTerm 转换为领导的 term

func (rf *Raft) sendAppendLogEntries(server int, args *AppendLogEntriesArgs, reply *AppendLogEntriesReply) bool {
	//
	return false
}

func (rf *Raft) SendApplyMsg() {
	// 发送 log 中的未应用的 msgs
	// 当我 commitIndex > lastApplied, 增加 lastApplied，然后在状态机上应用该条日志.
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
