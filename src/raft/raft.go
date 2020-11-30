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
	"../labgob"
	"time"
	"math/rand"
	"bytes"
)

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
	mu              sync.Mutex          // Lock to protect shared access to this peer's state
	peers           []*labrpc.ClientEnd // RPC end points of all peers
	persister       *Persister          // Object to hold this peer's persisted state
	me              int                 // this peer's index into peers[]
	dead            int32               // set by Kill()

	state           int                 // 服务所处的状态，领导者，候选人或是下属
	leaderId        int                 // 方便 follower 在接收到 client rpc 时重定向到 leader
	voteCount       int                 // 记录获得的票数，过半即成为领导者
	timestamp       time.Time           // 计算超时时间
	electionTimeout time.Duration       // 记录超时时间

	// 需持久化的
	currentTerm     int                 //　目前的状态
	votedFor        int                 // 该 term 中投给了谁
	logs            []LogEntry          // all log entries，只能读取，不能更改

	applyCh         chan ApplyMsg       // raft 从中读取要应用的日志信息，并应用到状态机

	// leader 容易丢失的
	matchIndexes    []int               // 领导者所维护的对各个下属服务的下一个待匹配索引 (init lastLogIndex + 1)
	nextIndexes     []int               // 领导者所维护的与各个下属服务匹配上的最高索引 (init 0),各个 follower 间如何区分？按在 peers 中的顺序？

	// 所有 server 都容易丢失的
	commitIndex     int                 // 最近的一次已提交的日志的索引
	lastApplied     int                 // 最近一次已应用的日志的索引
}

type LogEntry struct {
	Term  int // log 发布时所处的时期
	Command  interface{}  // 任务详情
	CommandIndex  int  // 任务索引
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.logs)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

// 每次重新加入 raft cluster 前都需要检测该 persistence 的属性是否持久化了，若没有
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 {
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var logs []LogEntry
	var currentTerm int
	var votedFor int
	if d.Decode(&logs) != nil ||
	   d.Decode(&currentTerm) != nil ||
	   d.Decode(&votedFor) != nil {
		   panic("Decode error")
	} else {
	  rf.logs = logs
	  rf.currentTerm = currentTerm
	  rf.votedFor = votedFor
	}
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
// //
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//

func (rf *Raft) getLastLogTermAndIndex() (int, int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term := rf.logs[len(rf.logs) - 1].Term
	index := len(rf.logs) - 1
	return term, index
}

func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	rf.mu.Lock()
	term = rf.currentTerm
	isleader = rf.state == Leader
	rf.mu.Unlock()
	return term, isleader
}


func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	// 判断是否是领导，只有领导才能存储从客户处收到的命令
	term, isLeader := rf.GetState()
	if isLeader {
		// 构建包含命令的日志
		_, lastLogIndex := rf.getLastLogTermAndIndex()
		rf.mu.Lock()
		term := rf.currentTerm
		index := lastLogIndex + 1
		newEntry := LogEntry{
			Term : term,
			Command : command,
			CommandIndex: index,
		}
		rf.logs = append(rf.logs, newEntry)

		// 更改匹配列表中与自身相关的信息
		rf.nextIndexes[rf.me] = index + 1
		rf.matchIndexes[rf.me] = index
		rf.persist()
		rf.mu.Unlock()
	}
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
	Term         int // 候选者所处的任期
	CandidateId  int // 候选者的 id
	LastLogIndex int // 候选者的最新日志的索引,用于资格核验
	LastLogTerm  int // 候选者的最新日志的所处任期,用于资格核验
}

type RequestVoteReply struct {
	Term        int  // follower 所处的任期,用于让候选者更新自己的任期及状态
	VoteGranted bool // follower 是否投票给 candidate
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// 1. 初始化回复框架
	rf.mu.Lock()
	defer rf.persist()
	defer rf.mu.Unlock()
	reply.VoteGranted = false

	// 2. 候选人资格审查，即候选人所含的日志信息是不是没有落后于我
	// 2.1 若没有落后，则进入下一步，若落后了，则拒绝，并告知我所处的朝代
	if !isCandidateUpToDate(args, rf){
		reply.Term = rf.currentTerm
	} else {
		// 3. 检查我是否还有投票资格，若还有，则通过候选人的投票申请
		if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
			reply.VoteGranted = true
			rf.votedFor = args.CandidateId
			rf.resetElectionTimeout()
		}
	}

	// 4. 更新我的朝代信息及状态信息，将我的朝代信息与候选人保持一致，我的状态改为 follower（我可能为落后的 candidate）
	// 5. 更新回复中的我的朝代信息
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.changeState(Follower)
		reply.Term = rf.currentTerm
	}
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func isCandidateUpToDate(args *RequestVoteArgs, rf *Raft) bool {
	// 候选人的 LastLogTerm 是否 < 我目前所处的 term，若是，则直接返回 false，否则进行下一步判断
	if args.LastLogTerm < rf.logs[len(rf.logs) - 1].Term {
		return false
	}
	// 候选人的 lastLogIndex 是否 >= 我的 lastLogIndex，若是，则返回 true，否则返回 false
	if args.LastLogIndex >= rf.logs[len(rf.logs) - 1].CommandIndex {
		return true
	}
	return false
}

func (rf *Raft) changeState(state int) {
	rf.mu.Lock()
	rf.state = state
	rf.mu.Unlock()
}

func (rf *Raft) isElectionTimeout() bool {
	rf.mu.Lock()
	defer rf.mu.Unlock() 
	return time.Now().Sub(rf.timestamp) > rf.electionTimeout
}

func (rf *Raft) resetElectionTimeout() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.electionTimeout = time.Duration(rand.Intn(ElectionRandomTimeRange) + ElectionTimeout) * time.Millisecond
	rf.timestamp = time.Now()
}

func (rf *Raft) runLeader() {
	// 1. 发送心跳信息
	rf.sendHeartbeat()
	// 2. 等待固定的心跳信息间隔时间
	time.Sleep(time.Duration(HeartBeatInterval) * time.Millisecond)
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
	// 1. 开始领导选举
	rf.startLeaderElection()
	// 2. 选举超时，即重选
	if rf.isElectionTimeout(){
		rf.startLeaderElection()
	}
	// 3. 当所得票数超过总票数的一半时，将自己的状态变为 leader
	rf.mu.Lock()
	if rf.voteCount > len(rf.peers) / 2 {
		rf.changeState(Leader)
		rf.persist()
	}
	rf.mu.Unlock()
	// 4. 休息一下
	time.Sleep(10 * time.Millisecond)
}

func (rf *Raft) runFollower() {
	// 1. 若选举超时，则转换自己的状态
	if rf.isElectionTimeout(){
		rf.changeState(Candidate)
	}
	// 2. 休息一下
	time.Sleep(10 * time.Millisecond)
}

func (rf *Raft) Run() {
	for !rf.killed() {
		rf.mu.Lock()
		state := rf.state
		rf.mu.Unlock()
		switch state {
		case Candidate:
			rf.runCandidate()
		case Follower:
			rf.runFollower()
		case Leader:
			rf.runFollower()
		default:
			panic("wrong state")
		}
	}
}

func (rf *Raft) startLeaderElection() {
	// 1. 重置选举超时时间
	rf.resetElectionTimeout()

	DPrintf("server %v: start election", rf.me)
	// 2. 改变自己的基本状态
	rf.mu.Lock()
	rf.voteCount = 1
	rf.currentTerm += 1
	rf.votedFor = rf.me
	rf.mu.Unlock()

	// 3. 准备给每个团队成员发送投票申请（除了自己）
	for i, _ := range rf.peers {
		if i == rf.me{
			continue
		}

	// 4. 每次发送前都需要核对一下自己是否还具备资格
		rf.mu.Lock()
		state := rf.state
		rf.mu.Unlock()
		if state != Leader {
			return
		}

		go func(index int){
			// 5. 构造要发送的信息和回复接收框
			rf.mu.Lock()
			requestArgs := RequestVoteArgs{
				Term : rf.currentTerm,
				CandidateId: rf.me,
				LastLogIndex: len(rf.logs) - 1,
				LastLogTerm: rf.logs[len(rf.logs) - 1].Term,
			}
			rf.mu.Unlock()
			reply := RequestVoteReply{}

			// 6. 发送投票申请
			if rf.sendRequestVote(index, &requestArgs, &reply){
				// 7. 检查回复
				rf.mu.Lock()
				if reply.VoteGranted {
				// 7.1 若得票，则将自己的已得票数 + 1
					rf.voteCount += 1
				} else {
				// 7.2 若没得票，则需要检查原因
				// 7.2.1 若具备资格，则对该条回复直接略过（投了同期的其他人而已）
				// 7.2.2 若不具备资格，则改变自己的所处朝代及状态，并持久化状态
					if reply.Term > rf.currentTerm {
						rf.currentTerm = reply.Term
						rf.changeState(Follower)
						rf.persist()
					}
				}
				rf.mu.Unlock()
			}
		}(i)

	} 
}


//
// 关于 log replication
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}
// 所有被 committed 的日志都会转换成 applyMsg，然后塞进 applyCh,等待被应用，因此所有命令进来就需要被注册，
// 之后可以通过注册时所得到的信息，找到每一条命令，然后执行，然后返回给 client 该消息已经被执行，因为所有的消息会存放在堆栈中，
// 只有确定执行了，server 可以通过 msg 中包含的信息，找到对应的 client，并给其回复

type AppendLogEntriesArgs struct {
	Term            int         // 领导者所处的朝代
	LeaderId        int         // 当客户发错消息给下属时,下属可以告诉客户领导是谁
	PreLogIndex     int         // 前一日志的索引,用于资格核验
	PreLogTerm      int         // 前一日志所处的任期,用于资格核验
	Entries         []LogEntry  // 要添加的日志信息,为了效率,可能一次添加多条
	LeaderCommit    int         // 领导通知提交的日志索引
}

type AppendLogEntriesReply struct {
	Term            int         // 告知领导我目前所处的日志,以便领导更新自己,然后领导转变状态
	XTerm           int         // 冲突日志的朝代
	XIndex          int         // 与冲突日志同朝代的第一条日志的日志索引
	LastLogIndex    int         // 最后一条日志的 index
	Success         bool        // 当下属前一日志与领导相符时,回复成功
}

func (rf *Raft) AppendLogEntries(args *AppendLogEntriesArgs, reply *AppendLogEntriesReply) {
	// 1. 初始化回复结构
	reply.XTerm = -1
	reply.XIndex = -1
	reply.LastLogIndex = -1
	reply.Success = false

	// 2. 检查是否具备添加日志的资格，当被添加人（follower / candidate，即此raft）的朝代超前于申请添加人（疑似 leader）时，添加申请被拒绝
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist() // 存在日志更新都需要做持久化
	if rf.currentTerm > args.Term {
		reply.Term = rf.currentTerm
		return
	}

	// 3. 检查日志一致性
	// 3.1 若不一致，且不一致的点在于 follower 中的日志落后于 leader，则根据落后情况，返回相应的错误信息
	// 这类错误可大致分为两种情况，存在冲突日志 & 不存在冲突日志
	// 存在冲突日志：同索引，朝代不一致
	// 不存在冲突日志: raft logs < PreLog
	if args.PreLogIndex >= len(rf.logs) {
		reply.LastLogIndex = len(rf.logs)
	} else if args.PreLogTerm != rf.logs[args.PreLogIndex].Term {
		reply.XTerm = rf.logs[args.PreLogIndex].Term
		xIndex := args.PreLogIndex
		for rf.logs[xIndex-1].Term == reply.XTerm {
			xIndex -= 1
		}
		reply.XIndex = xIndex
	} else {
		// 3.2 若不一致，但不一致的点在于被添加人（follower）含有 leader 的 PreLog，只是 follower 中的日志比 leader 多，此时应移除 PreLog 之后的所有日志，然后逐一添加新日志.
		// 3.3 若一致，则逐一添加新日志
		reply.Success = true
		if len(args.Entries) > 0 {
			for _, entry := range args.Entries {
				// 先补上欠下的
				if entry.CommandIndex < len(rf.logs) {
					// 删除不一致的
					if rf.logs[entry.CommandIndex].Term != entry.Term {
						rf.logs = rf.logs[: entry.CommandIndex]
						rf.logs = append(rf.logs, entry)
					}
				} else {
					rf.logs = append(rf.logs, entry)
				}
			}
			rf.nextIndexes[rf.me] = len(rf.logs)
			rf.matchIndexes[rf.me] = len(rf.logs) - 1
		}

		// 4. 更新 commitIndex 
		// 若 leaderCommit > 我的 commitIndex, 则将 commitIndex = min(leaderCommit, 最新日志的索引) ,之后会随着我的日志更新, commitIndex 增大,直到跟上领导的进程
		if args.LeaderCommit > rf.commitIndex {
			if args.LeaderCommit < len(rf.logs) - 1{
				rf.commitIndex = args.LeaderCommit
			} else {
				rf.commitIndex = len(rf.logs) - 1
			}
		}
	}

	// 5. 更新 raft 状态和朝代（raft 可能为 candidate）
	// 当领导的 term > 我的 currentTerm 时, 会将 currentTerm 转换为领导的 term
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.changeState(Follower)
	}

	// 6. 补全 reply 信息
	reply.Term = rf.currentTerm

	// 7. 检查是否有日志已提交但未应用，若存在，则发送这部分日志到应用管道
	// 对于 leader 中已经 commit 的日志，会执行应用
	if rf.commitIndex > rf.lastApplied {
		rf.sendApplyMsg()
	}

	// 8. 每次接收一次日志更新的 rpc，都要重置一次选举倒计时
	rf.resetElectionTimeout()
}

func (rf *Raft) sendAppendLogEntries(server int, args *AppendLogEntriesArgs, reply *AppendLogEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendLogEntries", args, reply)
	return ok
}

func (rf *Raft) sendApplyMsg() {
	// 当 commitIndex > lastApplied, 需要发送未执行的命令到应用管道
	if rf.commitIndex > rf.lastApplied {
		// 1. 未执行的命令为已提交但未被应用的
		unusedEntries := rf.logs[rf.lastApplied + 1: rf.commitIndex + 1]

		go func(entries []LogEntry) {
			for _, entry := range entries {
				// 2. 对各日志进行格式转换
				msg := ApplyMsg{
					CommandValid: true,
					Command: entry.Command, 
					CommandIndex: entry.CommandIndex,
				}
				// 3. 将格式转换后的日志依次塞进应用管道
				rf.applyCh <- msg
				
				// 4. 更改 lastApplied
				rf.mu.Lock()
				rf.lastApplied = msg.CommandIndex
				rf.mu.Unlock()
			}
		}(unusedEntries)
	}
}

//
// 共有流程
//
func (rf *Raft) sendHeartbeat() {
	// 1.　准备给每个 peer 发送心跳信（除了自己）
	for i, _ := range rf.peers {
		if rf.me == i{
			continue
		}

		go func(index int){
			// 2. 正式发送前需检查自己是否还是 leader，若不是，则不能继续发送
			rf.mu.Lock()
			if rf.state != Leader {
				rf.mu.Unlock()
				return 
			}

			// 3. 构造心跳信息
			preLogIndex := rf.nextIndexes[index] - 1
			entries := make([]LogEntry, len(rf.logs[preLogIndex + 1 :]))
			copy(entries, rf.logs[preLogIndex + 1 :])
			args := AppendLogEntriesArgs{
				Term : rf.currentTerm,
				LeaderId: rf.me,
				PreLogIndex: preLogIndex,
				PreLogTerm: rf.logs[preLogIndex].Term,
				Entries: entries,
				LeaderCommit: rf.commitIndex,
			}
			rf.mu.Unlock()

			reply := AppendLogEntriesReply{}
			// 4. 发送心跳信息
			if rf.sendAppendLogEntries(index, &args, &reply) {
				rf.mu.Lock()

				// 5. 检查是否发送成功
				// 5.1 若成功，则更新该 peer 的matchIndex & nextIndex
				if reply.Success {
					rf.matchIndexes[index] = args.PreLogIndex + len(args.Entries)
					rf.nextIndexes[index] = rf.matchIndexes[index] + 1
				} else {
					// 5.2 若不成功，则检查自己是不是没资格
					// 5.2.1 若是没资格，则转变自己的状态（term & state）,并做持久化
					if reply.Term > rf.currentTerm {
						rf.currentTerm = reply.Term
						rf.changeState(Follower)
						rf.persist()
					} else {
						// 5.2.2 若不是没资格，则根据 XTerm & XIndex & LastLogIndex 更新该 peer 的 nextIndex.
						// 当存在冲突日志，若 leader 根本没有 XTerm，nextIndex 可直接回到该 term 下的第一条 log 的索引，即 XIndex -> a
						// 当存在冲突日志，但 leader 含有 XTerm，nextIndex 为该 Term 的最后一条 log 的索引 -> b
						// 当不存在冲突日志，则 nextIndex 为 peer 最后一条 log 的索引 -> c
						if reply.LastLogIndex != -1 {
							// c
							rf.nextIndexes[index] = reply.LastLogIndex
						} else {
							nextIndex := reply.XIndex
							for j := args.PreLogIndex; j >= 1; j-- {
								if rf.logs[j-1].Term <= reply.XTerm {
									break
								}
							if rf.logs[j-1].Term == reply.XTerm {
								// b
								rf.nextIndexes[index] =j
							} else {
								// a
								rf.nextIndexes[index] = nextIndex
							}
							}

						}
					}
				} 
				rf.mu.Unlock()
			}
		}(i)
	}

	// 6. 检查是否有新日志要被应用（即统计是否存在与超半数 peer 匹配的日志未被提交）
	rf.mu.Lock()
	if rf.commitIndex < rf.matchIndexes[rf.me] {
		c := 0
		minIndex := rf.matchIndexes[rf.me]
		for i, _ := range rf.peers {
			if rf.matchIndexes[i] > rf.commitIndex {
				c += 1
				if rf.matchIndexes[i] < minIndex {
					minIndex = rf.matchIndexes[i]
				}
			}
		}
		// 只允许提交本朝代的日志
		if c > len(rf.peers)/2 && rf.logs[minIndex].Term == rf.currentTerm {
			rf.commitIndex = minIndex
			rf.sendApplyMsg()
		}

	}
	rf.mu.Unlock()
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

	rf.state = Follower
	rf.resetElectionTimeout()

	rf.currentTerm = 0
	rf.votedFor = -1
	rf.logs = append(rf.logs, LogEntry{})
	rf.applyCh = applyCh
	rf.matchIndexes = make([]int, len(peers))
	rf.nextIndexes = make([]int, len(peers))
	rf.commitIndex = 0
	rf.lastApplied = 0

	// Your initialization code here (2A, 2B, 2C).
	
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go rf.Run()

	return rf
}
