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
	//	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

type State int

// 服务器三种状态
const (
	Leader    State = 0
	Follower  State = 1
	Candidate State = 2
)

type Log struct {
	// 日志条目的命令
	Command interface{}
	// 领导人接收到该条目时的任期
	Term int
}

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type PersisterState struct {
	currentTerm int
	votedFor    int
	log         []interface{}
}

type Timer struct {
	// 上一次重置计时器的时间
	time_    time.Time
	timeout_ int64
	mu       sync.Mutex
}

func RandomElectionTimeout() int {
	rand.Seed(int64(time.Now().Nanosecond()))
	return rand.Intn(200) + 300
}

func StableHeartBeatTimeout() int {
	return 100
}

// 重置计时器
func (timer *Timer) reset(ms int) {
	timer.mu.Lock()
	timer.time_ = time.Now()
	// 生成300-500的随机数
	timer.timeout_ = int64(ms)
	timer.mu.Unlock()
}

// 判断是否超时
func (timer *Timer) timeout() bool {
	timer.mu.Lock()
	res := time.Now().Sub(timer.time_).Milliseconds() > timer.timeout_
	timer.mu.Unlock()
	return res
}

//
// A Go object implementing a single Raft peer.
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

	// 持久性状态，如何将这些状态添加到persister中
	// 服务器已知最新的任期（在服务器首次启动时初始化为0，单调递增）
	CurrentTerm int
	// 当前任期内收到选票的 candidateId，如果没有投给任何候选人 则为空
	// 因为是当前任期投的票，所以修改任期该值需要重置
	VotedFor int
	// 日志条目；每个条目包含了用于状态机的命令，以及领导人接收到该条目时的任期（初始索引为1）
	LogEntries []Log
	// 易失性状态
	// 已知已提交的最高的日志条目的索引（初始值为0，单调递增）
	commitIndex int
	// 已经被应用到状态机的最高的日志条目的索引（初始值为0，单调递增）
	lastApplied int
	// 领导上的易失性状态
	// 对于每一台服务器，发送到该服务器的下一个日志条目的索引（初始值为领导人最后的日志条目的索引+1）
	nextIndex []int
	// 对于每一台服务器，已知的已经复制到该服务器的最高日志条目的索引（初始值为0，单调递增）
	matchIndex []int
	// 选举超时计时器
	electionTimer *Timer
	// 心跳超时计时器
	heartHeatTimer *Timer
	// 服务器的状态：leader, fllower, candidate
	state State
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	term = rf.CurrentTerm
	if rf.state == Leader {
		isleader = true
	}
	rf.mu.Unlock()
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

//
// restore previously persisted state.
//
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
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term        int
	CandidateId int
	// 候选人的最后日志条目的索引值
	LastLogIndex int
	// 候选人最后日志条目的任期号
	LastLogTerm int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	// 当前任期号，以便于候选人去更新自己的任期号
	Term int
	// 候选人赢得了此张选票时为真
	VoteGranted bool
}

type AppendEntriesArgs struct {
	// 领导人的任期
	Term     int
	LeaderId int
	// 紧邻新日志条目之前的那个日志条目的索引
	PrevLogIndex int
	// 紧邻新日志条目之前的那个日志条目的任期
	PrevLogTerm int
	// 需要被保存的日志条目（被当做心跳使用时，则日志条目内容为空；为了提高效率可能一次性发送多个）
	Entries []Log
	// 领导人的已知已提交的最高的日志条目的索引
	LeaderCommit int
}

type AppendEntriesReply struct {
	// 当前任期，对于领导人而言 它会更新自己的任期
	Term int
	// 如果跟随者所含有的条目和 prevLogIndex 以及 prevLogTerm 匹配上了，则为 true
	Success bool
}

//
// 如果term > rf.CurrentTerm，则修改该服务器的当前任期号，并重置VotedFor参数
//
func (rf *Raft) ModifyCurrentTerm(term int) bool {
	if rf.CurrentTerm < term {
		// rf.mu.Lock()
		rf.CurrentTerm = term
		rf.VotedFor = -1
		rf.state = Follower
		// rf.mu.Unlock()
		return true
	}
	return false
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	// DPrintf("request vote start: %d --> %d", args.CandidateId, rf.me)
	// 候选者的任期号过小直接返回
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.CurrentTerm > args.Term {
		reply.VoteGranted = false
		reply.Term = rf.CurrentTerm
		return
	}
	// 修改任期号
	rf.ModifyCurrentTerm(args.Term)
	reply.VoteGranted = false
	reply.Term = rf.CurrentTerm
	if rf.state == Follower && (rf.VotedFor == -1 || rf.VotedFor == args.CandidateId) {
		// 候选人的日志至少和自己一样新
		index := len(rf.LogEntries) - 1
		// 如果两份日志最后的条目的任期号不同，那么任期号大的日志更加新。
		// 如果两份日志最后的条目任期号相同，那么日志比较长的那个就更加新。
		if rf.LogEntries[index].Term < args.LastLogTerm ||
			(rf.LogEntries[index].Term == args.LastLogTerm) && index <= args.LastLogIndex {
			rf.electionTimer.reset(RandomElectionTimeout())
			// DPrintf("{Peer: %d}, {term: %d}, reset election timeout: %d", rf.me, rf.CurrentTerm, rf.electionTimer.timeout_)
			// 给候选者投票
			rf.VotedFor = args.CandidateId
			reply.VoteGranted = true
			reply.Term = rf.CurrentTerm
			DPrintf("peer: %d vote for %d, term: %d", rf.me, rf.VotedFor, rf.CurrentTerm)
			return
		}
	}

	// DPrintf("request vote finished: %d --> %d", args.CandidateId, rf.me)
}

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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// 直接返回
	if args.Term < rf.CurrentTerm {
		reply.Success = false
		reply.Term = rf.CurrentTerm
		return
	}
	// 修改任期号
	rf.ModifyCurrentTerm(args.Term)
	rf.electionTimer.reset(RandomElectionTimeout())
	// DPrintf("{Peer: %d}, {term: %d}, reset election timeout: %d", rf.me, rf.CurrentTerm, rf.electionTimer.timeout_)

	reply.Term = rf.CurrentTerm

	// 处理心跳包
	if len(args.Entries) == 0 {
		// DPrintf("peer %d get heatbeat from %d", rf.me, args.LeaderId)
		reply.Success = true
		return
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
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
// 开始选举
//
func (rf *Raft) startElection() {
	rf.mu.Lock()
	DPrintf("{peer: %d} request vote, term: %d", rf.me, rf.CurrentTerm)
	// 重置超时时间
	// rf.electionTimer.reset(RandomElectionTimeout())
	// 任期号递增
	rf.CurrentTerm = rf.CurrentTerm + 1
	// 为自己投票
	rf.VotedFor = rf.me
	// 收到的投票数
	numVotes := 1
	// 最后一条日志索引
	index := len(rf.LogEntries) - 1
	lastTerm := rf.LogEntries[index].Term
	args := &RequestVoteArgs{rf.CurrentTerm, rf.me, index, lastTerm}
	rf.mu.Unlock()
	// 向所有服务器发送投票请求
	for i := range rf.peers {
		if i != rf.me {
			go func(server int) {
				reply := &RequestVoteReply{}
				DPrintf("server: %d", server)
				//
				// 收到正常回复
				if rf.sendRequestVote(server, args, reply) {
					DPrintf("锁")
					rf.mu.Lock()
					// 检查是否为过期的回复
					if rf.CurrentTerm == args.Term && rf.state == Candidate {
						// 获得投票
						if reply.VoteGranted {
							// 增加获得的选票数
							numVotes += 1
							// 选出leader立即发送心跳包，而不是收到所有选举结果后再发送
							if numVotes > len(rf.peers)/2 {
								DPrintf("{peer: %d} is leader, term: %d", rf.me, rf.CurrentTerm)
								rf.state = Leader
								// 发送心跳包
								rf.sendHeartBeat()
							}
							// DPrintf("peer %d get vote from %d", rf.me, server)
						} else {
							rf.ModifyCurrentTerm(reply.Term)
							DPrintf("{peer: %d} (term: %d) refuse voting to %d(term: %d)", server, reply.Term, rf.me, rf.CurrentTerm)
						}
					} else {
						DPrintf("{peer: %d} (term: %d) 收到过期的回复", rf.me, rf.CurrentTerm)
					}
					rf.mu.Unlock()
				} else {
					DPrintf("{peer: %d}(term: %d) cant connect to %d", rf.me, rf.CurrentTerm, server)
				}
			}(i)
		}
	}

	// DPrintf("peer: %d, request vote finished, term: %d", rf.me, rf.CurrentTerm)
	// 选出领导人
	// rf.mu.Lock()
	// if rf.numVotes > len(rf.peers)/2 {
	// 	DPrintf("peer: %d is leader, term: %d", rf.me, rf.CurrentTerm)
	// 	rf.state = Leader
	// } else {
	// 	DPrintf("peer: %d is not a leader, term: %d", rf.me, rf.CurrentTerm)
	// }
	// rf.mu.Unlock()
}

//
// 发送心跳包
//
func (rf *Raft) sendHeartBeat() {
	DPrintf("leader %d (term: %d) broadcast heart beat", rf.me, rf.CurrentTerm)
	// 收到正常心跳回复的数量
	// numHearts := 0
	rf.heartHeatTimer.reset(StableHeartBeatTimeout())
	for i := range rf.peers {
		if i != rf.me {
			go func(server int) {
				rf.mu.Lock()
				args := &AppendEntriesArgs{}
				args.Term = rf.CurrentTerm
				reply := &AppendEntriesReply{}
				rf.mu.Unlock()
				if rf.sendAppendEntries(server, args, reply) {
					rf.mu.Lock()
					// 处理心跳信息
					if rf.CurrentTerm == args.Term {

					}
					_ = rf.ModifyCurrentTerm(reply.Term)
					// if rf.ModifyCurrentTerm(reply.Term) {
					// 	DPrintf("{leader: %d}(term: %d) is not a leader any more.", rf.me, rf.CurrentTerm)
					// }
					rf.mu.Unlock()
				} else {
					// rf.mu.Lock()
					// numHearts++
					// // 有一半的服务器无法与leader通信，leader变为follower
					// // 问题：变成follower之后一直请求投票导致任期非常大，那么再选举其必成为leader
					// if numHearts > len(rf.peers)/2 {
					// 	DPrintf("{peer: %d} (term: %d) cant connect to other servers.", rf.me, rf.CurrentTerm)
					// 	rf.state = Follower
					// 	rf.VotedFor = -1
					// }
					// rf.mu.Unlock()
				}
			}(i)
		}
	}
	// wg.Wait()
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	// 周期性的调用，用于计时
	for rf.killed() == false {
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		time.Sleep(time.Millisecond * 10)
		// 检查是否超时，如果超时，则发起投票，并在选举完成之后重置计时器
		if rf.electionTimer.timeout() {
			rf.mu.Lock()
			if rf.state != Leader {
				rf.state = Candidate
				rf.mu.Unlock()
				rf.startElection()
				// 重置选举超时时间
				rf.electionTimer.reset(RandomElectionTimeout())
			} else {
				rf.mu.Unlock()
			}
		}
		// 心跳超时
		if rf.heartHeatTimer.timeout() {
			rf.mu.Lock()
			if rf.state == Leader {
				rf.mu.Unlock()
				// leader给所有服务器发送心跳包
				rf.sendHeartBeat()
			} else {
				rf.mu.Unlock()
			}

		}
	}
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
	// 初始化计时器和状态
	rf.CurrentTerm = 0
	rf.VotedFor = -1
	rf.electionTimer = &Timer{}
	// 计时器重置
	rf.electionTimer.reset(RandomElectionTimeout())
	rf.heartHeatTimer = &Timer{}
	rf.heartHeatTimer.reset(StableHeartBeatTimeout())
	DPrintf("peers: %d, random timer: %d ms", rf.me, rf.electionTimer.timeout_)
	rf.state = Follower
	log0 := Log{nil, rf.CurrentTerm}
	rf.LogEntries = append(rf.LogEntries, log0)
	// 初始化持久性状态
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	// 开始选票
	// start ticker goroutine to start elections
	go rf.ticker()
	return rf
}
