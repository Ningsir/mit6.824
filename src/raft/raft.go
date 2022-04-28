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
	"bytes"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
)

type State int

// 服务器三种状态
const (
	Leader    State = 0
	Follower  State = 1
	Candidate State = 2
)

// 发送的快照块的最大大小
const MaxSnapshotChunk int = 1024

type HeartType int

const (
	HeartsBeat    HeartType = 0
	AppendEntries HeartType = 1
)

type Log struct {
	// 日志条目的命令
	Command interface{}
	// 领导人接收到该条目时的任期
	Term int
}

//
// 当peer意识到有日志被提交了，其需要将日志应用到状态机，也就是将命令发送给service并执行
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

	// 传递快照时，CommandValid设置为false
	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	// 快照中包含的最后日志条目的索引
	SnapshotIndex int
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
	state   State
	applyCh chan ApplyMsg

	// 持久化，用于快照
	// 最后一个被包含在快照中的日志索引
	LastIncludedIndex int
	LastIncludedTerm  int
	// FirstIndex        int

	// 条件变量
	conds []*sync.Cond
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
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	// rf.mu.Lock()
	e.Encode(rf.CurrentTerm)
	e.Encode(rf.VotedFor)
	e.Encode(rf.LogEntries)
	e.Encode(rf.LastIncludedIndex)
	e.Encode(rf.LastIncludedTerm)
	// rf.mu.Unlock()
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
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
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var term int
	var votedFor int
	var logs []Log
	var index int
	var lastTerm int
	if d.Decode(&term) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&logs) != nil ||
		d.Decode(&index) != nil ||
		d.Decode(&lastTerm) != nil {
		DPrintf("Decode error")
	} else {
		rf.mu.Lock()
		rf.CurrentTerm = term
		rf.VotedFor = votedFor
		rf.LogEntries = logs
		rf.LastIncludedIndex = index
		rf.LastIncludedTerm = lastTerm
		rf.mu.Unlock()
		DPrintf("{Peer: %d(term: %d)} reboot and read persist VotedFor: %d, LastIncludedIndex: %d, Logs: %+v", rf.me, term, votedFor, index, logs)
	}
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
// Raft将快照发送到applyCh, 服务端从中读取快照并调用CondInstallSnapshot
// 如果是旧的快照，则返回false，阻止服务端切换快照
// 如果返回true，Raft删掉已经保存到快照中的日志，服务端切换到该快照
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).
	// TODO：如何判断快照是否为老的快照？
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.commitIndex >= lastIncludedIndex {
		return false
	}
	firstIndex := rf.FirstIndex()
	rf.LastIncludedIndex = lastIncludedIndex
	rf.LastIncludedTerm = lastIncludedTerm
	// 截断日志，那么nextIndex, matchIndex, commitIndex, lastApplied, prevLogIndex都需要做出相应调整
	if lastIncludedIndex >= firstIndex+len(rf.LogEntries) {
		rf.LogEntries = rf.LogEntries[:0]
	} else {
		rf.LogEntries = rf.LogEntries[lastIncludedIndex+1-firstIndex:]
	}
	rf.lastApplied = lastIncludedIndex
	rf.commitIndex = lastIncludedIndex
	rf.persist()
	rf.persister.SaveStateAndSnapshot(rf.persister.ReadRaftState(), snapshot)
	DPrintf("{Peer: %d(Term: %d)} CondInstallSnapshot:{lastIncludeIndex: %d, lastIncludeTerm: %d}, the length of the rest of logs: %d",
		rf.me, rf.CurrentTerm, rf.LastIncludedIndex, rf.LastIncludedTerm, len(rf.LogEntries))
	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
// 服务端创建了一个快照，Raft删除不再需要的日志条目
// snapshot包含索引在index之前(包括index)的所有日志
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	firstIndex := rf.FirstIndex()
	if index < firstIndex {
		return
	}
	if index < len(rf.LogEntries)+firstIndex {
		rf.LastIncludedIndex = index
		rf.LastIncludedTerm = rf.LogEntries[index-firstIndex].Term
		// 截断日志，那么nextIndex, matchIndex, commitIndex, lastApplied, prevLogIndex都需要做出相应调整
		rf.LogEntries = rf.LogEntries[index+1-firstIndex:]
		rf.persist()
		rf.persister.SaveStateAndSnapshot(rf.persister.ReadRaftState(), snapshot)
		DPrintf("{Peer: %d(Term: %d)} create snapshot:{lastIncludeIndex: %d, lastIncludeTerm: %d}, the length of the rest of logs: %d",
			rf.me, rf.CurrentTerm, rf.LastIncludedIndex, rf.LastIncludedTerm, len(rf.LogEntries))
	}
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	// follower安装快照，并将快照传递给服务端
	// 还有可能leader两次append，那么就出现两次调用两轮InstallSnapshot
	DPrintf("{Server: %d(Term: %d)} receive snapshot from {leader %d}, lastIncludeIndex: %d, lastIncludedTerm: %d",
		rf.me, rf.CurrentTerm, args.LeaderId, args.LastIncludedIndex, args.LastIncludedTerm)
	rf.mu.Lock()
	rf.electionTimer.reset(RandomElectionTimeout())
	if args.Term < rf.CurrentTerm {
		reply.Term = rf.CurrentTerm
		rf.mu.Unlock()
		return
	}
	rf.ModifyCurrentTerm(args.Term)
	rf.mu.Unlock()
	// 1. 保存快照文件，丢弃具有较小索引的任何现有或部分快照
	// 2. 如果现存的日志条目与快照中最后包含的日志条目具有相同的索引值和任期号，则保留其后的日志条目并进行回复
	// 将快照发送给服务端
	msg := ApplyMsg{
		CommandValid:  false,
		SnapshotValid: true,
		Snapshot:      args.Data,
		SnapshotTerm:  args.LastIncludedTerm,
		SnapshotIndex: args.LastIncludedIndex,
	}
	rf.applyCh <- msg
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

func (rf *Raft) handleInstallSnapshot(server int) {
	// rf.heartHeatTimer.reset(StableHeartBeatTimeout())
	// 读取快照，然后将快照发送给follower
	rf.mu.Lock()
	args := &InstallSnapshotArgs{}
	args.Term = rf.CurrentTerm
	args.LeaderId = rf.me
	args.LastIncludedIndex = rf.LastIncludedIndex
	args.LastIncludedTerm = rf.LastIncludedTerm
	args.Data = rf.persister.ReadSnapshot()
	reply := &InstallSnapshotReply{}
	rf.mu.Unlock()
	DPrintf("{Leader: %d(Term: %d)} send snapshot to {server: %d}, lastIncludeIndex: %d, lastIncludedTerm: %d",
		rf.me, rf.CurrentTerm, server, args.LastIncludedIndex, args.LastIncludedTerm)
	if rf.sendInstallSnapshot(server, args, reply) {
		rf.mu.Lock()
		if args.Term == rf.CurrentTerm && rf.state == Leader {
			if !rf.ModifyCurrentTerm(reply.Term) {
				// TODO：如何处理nextIndex和matchIndex
				// 应该真正安装快照的时候再修改nextIndex?
				rf.nextIndex[server] = args.LastIncludedIndex + 1
				rf.matchIndex[server] = args.LastIncludedIndex
			}
		}
		rf.mu.Unlock()
	}
}

type InstallSnapshotArgs struct {
	// 领导人的任期
	Term int
	// 领导人ID
	LeaderId int
	// 快照中包含的最后日志条目的索引值
	LastIncludedIndex int
	// 快照中包含的最后日志条目的任期号
	LastIncludedTerm int
	// 分块在快照中的字节偏移量
	// Offset int
	// 从偏移量开始的快照分块的原始字节
	Data []byte
	// // 如果这是最后一个分块则为 true
	// Done bool
	// // 快照大小
	// SnapshotSize int
}

type InstallSnapshotReply struct {
	Term int
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
	// 发生冲突的任期号
	ConflictTerm int
	// ConflictTerm对应的日志的最小索引值
	MinIndex   int
	InSnapshot bool
}

//
// 如果term > rf.CurrentTerm，则修改该服务器的当前任期号，并重置VotedFor参数
//
func (rf *Raft) ModifyCurrentTerm(term int) bool {
	if rf.CurrentTerm < term {
		rf.CurrentTerm = term
		rf.VotedFor = -1
		rf.state = Follower
		rf.persist()
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
		var lastTerm int
		if index < 0 {
			lastTerm = rf.LastIncludedTerm
		} else {
			lastTerm = rf.LogEntries[index].Term
		}
		lastIndex := rf.FirstIndex() + index
		// 如果两份日志最后的条目的任期号不同，那么任期号大的日志更加新。
		// 如果两份日志最后的条目任期号相同，那么日志比较长的那个就更加新。
		if lastTerm < args.LastLogTerm ||
			(lastTerm == args.LastLogTerm) && lastIndex <= args.LastLogIndex {
			rf.electionTimer.reset(RandomElectionTimeout())
			// DPrintf("{Peer: %d}, {term: %d}, reset election timeout: %d", rf.me, rf.CurrentTerm, rf.electionTimer.timeout_)
			// 给候选者投票
			rf.VotedFor = args.CandidateId
			reply.VoteGranted = true
			reply.Term = rf.CurrentTerm
			DPrintf("peer: %d(lastTerm: %d, lastLogIndex: %d) vote for %d(lastTerm: %d, lastLogIndex: %d), term: %d",
				rf.me, lastTerm, lastIndex, rf.VotedFor, args.LastLogTerm, args.LastLogIndex, rf.CurrentTerm)
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

func Min(a int, b int) int {
	if a < b {
		return a
	} else {
		return b
	}
}

func Max(a int, b int) int {
	if a < b {
		return b
	} else {
		return a
	}
}

func (rf *Raft) FirstIndex() int {
	return rf.LastIncludedIndex + 1
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// 重置选举超时计时器
	rf.electionTimer.reset(RandomElectionTimeout())
	// leader任期过小
	if args.Term < rf.CurrentTerm {
		reply.Success, reply.Term = false, rf.CurrentTerm
		return
	}
	rf.ModifyCurrentTerm(args.Term)
	firstIndex := rf.FirstIndex()
	// PrevLogIndex+1越界
	if args.PrevLogIndex >= len(rf.LogEntries)+firstIndex {
		reply.Success = false
		reply.Term = rf.CurrentTerm
		reply.ConflictTerm = -1
		reply.MinIndex = len(rf.LogEntries) + firstIndex
		return
	}
	// 日志不匹配
	var preLogTerm int
	// 需要写入的日志可能已经写入快照中，返回false，那么发送端如何处理？
	if args.PrevLogIndex-firstIndex < -1 {
		reply.Success = false
		reply.Term = rf.CurrentTerm
		// 已经写入快照
		reply.InSnapshot = true
		return
	} else if args.PrevLogIndex-firstIndex == -1 {
		preLogTerm = rf.LastIncludedTerm
	} else {
		preLogTerm = rf.LogEntries[args.PrevLogIndex-firstIndex].Term
	}
	if preLogTerm != args.PrevLogTerm {
		reply.Success = false
		reply.Term = rf.CurrentTerm
		reply.ConflictTerm = preLogTerm
		reply.MinIndex = args.PrevLogIndex
		for i := args.PrevLogIndex - 1; i >= firstIndex; i-- {
			if preLogTerm != rf.LogEntries[i-firstIndex].Term {
				reply.MinIndex = i + 1
				break
			}
		}
		DPrintf("{Follower %d(term: %d), ConflictTerm: %d, ConflictIndex: %d} mismatch {Leader %d(term:%d), PrevLogIndex: %d, PrevLogTerm: %d}",
			rf.me, rf.CurrentTerm, rf.LogEntries[args.PrevLogIndex-firstIndex].Term, reply.MinIndex, args.LeaderId, args.Term, args.PrevLogIndex, args.PrevLogTerm)
		return
	}
	// 修改任期号
	rf.ModifyCurrentTerm(args.Term)

	// 匹配上且不是心跳
	if len(args.Entries) != 0 {
		// 1. **如果**一个已经存在的条目和新条目（译者注：即刚刚接收到的日志条目）发生
		// 了冲突（因为索引相同，任期不同），那么就删除这个已经存在的条目以及它之后的所有条目
		index := -1
		for i, log := range args.Entries {
			if i+args.PrevLogIndex+1 < len(rf.LogEntries)+firstIndex &&
				log.Term != rf.LogEntries[i+args.PrevLogIndex+1-firstIndex].Term {
				// 记录冲突的索引
				index = i + args.PrevLogIndex + 1 - firstIndex
				break
			}
		}
		// 发生冲突
		if index != -1 {
			rf.LogEntries = append(rf.LogEntries[:index], args.Entries[index-1-args.PrevLogIndex+firstIndex:]...)
		} else if len(args.Entries)+args.PrevLogIndex+1 > len(rf.LogEntries)+firstIndex {
			//  2. 追加日志中尚未存在的任何新条目
			first := len(rf.LogEntries) - args.PrevLogIndex - 1 + firstIndex
			rf.LogEntries = append(rf.LogEntries, args.Entries[first:]...)
		}
		rf.persist()
		DPrintf("{Follower %d(term: %d, commitIndex: %d)} append entries, get last entry{%+v, index: %d}, leaderCommit: %d",
			rf.me, rf.CurrentTerm, rf.commitIndex, rf.LogEntries[len(rf.LogEntries)-1], len(rf.LogEntries)-1, args.LeaderCommit)
	}
	// 根据参数leaderCommit更新commitIndex
	if args.LeaderCommit > rf.commitIndex {
		// 是与最后一个新条目的索引取最小值
		// 因为发送心跳时，日志条目为空，那么LeaderCommit可能大于最大索引
		rf.commitIndex = Min(args.LeaderCommit, args.PrevLogIndex+len(args.Entries))
		// DPrintf("{Follower: %d (term: %d)} commit log {commitIndex: %d, term: %d}", rf.me, rf.CurrentTerm, rf.commitIndex, rf.LogEntries[rf.commitIndex-firstIndex].Term)
		DPrintf("{Follower: %d (term: %d)} commit log {commitIndex: %d}, lastIncudedIndex: %d", rf.me, rf.CurrentTerm, rf.commitIndex, rf.LastIncludedIndex)
	}
	reply.Term = rf.CurrentTerm
	reply.Success = true
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
// 如果server不是leader，返回false。不能保证命令一定会提交到Raft日志中
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := false

	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	firstIndex := rf.FirstIndex()
	if rf.state != Leader {
		index, term, isLeader = len(rf.LogEntries)-1+firstIndex, rf.CurrentTerm, false
		return index, term, isLeader
	}
	log := Log{command, rf.CurrentTerm}
	rf.LogEntries = append(rf.LogEntries, log)
	rf.persist()
	// 利用日志的长度更新自身的nextIndex和matchIndex
	rf.nextIndex[rf.me] = len(rf.LogEntries) + firstIndex
	rf.matchIndex[rf.me] = len(rf.LogEntries) - 1 + firstIndex
	index, term, isLeader = len(rf.LogEntries)-1+firstIndex, rf.CurrentTerm, true
	DPrintf("{Leader: %d (term: %d)} append entries {command: %v, index: %d, term: %d} to self", rf.me, rf.CurrentTerm, command, index, rf.LogEntries[index-firstIndex].Term)
	// 调用AppendEntries
	rf.sendHeartBeat(AppendEntries)
	return index, term, isLeader
}

func (rf *Raft) startAppend() {
	DPrintf("Server: %d(term: %d) start append", rf.me, rf.CurrentTerm)
	for i := range rf.peers {
		if i != rf.me {
			go rf.append(i)
		}
	}

}
func (rf *Raft) needAppend(server int) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.nextIndex[server] < rf.FirstIndex()+len(rf.LogEntries) && rf.state == Leader
}

// 会启动多个append协程
// 为什么要append多次才能append到重连的server上？
func (rf *Raft) append(server int) {
	rf.conds[server].L.Lock()
	defer rf.conds[server].L.Unlock()
	for rf.killed() == false {
		DPrintf("Server %d prepare to append", server)
		// 没有新的log需要append, 则一直阻塞
		for !rf.needAppend(server) {
			DPrintf("Server %d append blocking", server)
			rf.conds[server].Wait()
		}
		DPrintf("Server %d start to append", server)
		rf.mu.Lock()
		index := rf.nextIndex[server] - 1
		rf.mu.Unlock()
		// 发送快照
		if index+1 < rf.FirstIndex() {
			go rf.handleInstallSnapshot(server)
		} else {
			// 参数
			rf.mu.Lock()
			args := &AppendEntriesArgs{}
			args.Term = rf.CurrentTerm
			args.LeaderId = rf.me
			args.LeaderCommit = rf.commitIndex
			// start和handleAppendEntries应该是一个整体
			args.PrevLogIndex = index
			if args.PrevLogIndex < rf.FirstIndex() {
				args.PrevLogTerm = rf.LastIncludedTerm
			} else {
				args.PrevLogTerm = rf.LogEntries[args.PrevLogIndex-rf.FirstIndex()].Term
			}
			args.Entries = append(args.Entries, rf.LogEntries[index+1-rf.FirstIndex():]...)
			rf.mu.Unlock()
			DPrintf("{Leader: %d (term: %d)} append entries {index: %d -- %d} to follower %d, args: {prevLogIndex: %d, preLogTerm: %d, leaderCommit: %d}",
				rf.me, rf.CurrentTerm, args.PrevLogIndex+1, args.PrevLogIndex+len(args.Entries), server, args.PrevLogIndex, args.PrevLogTerm, args.LeaderCommit)
			go rf.handleAppendEntries1(server, args)
		}
		time.Sleep(time.Millisecond * 20)
	}
	DPrintf("Follower %d stop append routine", server)
}
func (rf *Raft) handleAppendEntries1(server int, args *AppendEntriesArgs) {
	// rf.heartHeatTimer.reset(StableHeartBeatTimeout())
	reply := &AppendEntriesReply{}
	defer DPrintf("Server %d stop append entries, reply: %+v", server, reply)
	// 重新连接后RPC仍然不可达
	if rf.sendAppendEntries(server, args, reply) {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		// 任期相同
		if rf.CurrentTerm == args.Term && rf.state == Leader {
			if reply.Term > rf.CurrentTerm {
				if rf.ModifyCurrentTerm(reply.Term) {
					DPrintf("Leader %d convert to Follower", rf.me)
				}
				return
			}
			// 日志不匹配，修改参数重新执行
			if reply.Success == false {
				if reply.InSnapshot {
					return
				}
				// 修改nextIndex重试
				index := rf.searchConflictLogIndex(reply)
				rf.nextIndex[server] = index
				return
			} else { // 日志条目匹配上
				rf.nextIndex[server] = Max(args.PrevLogIndex+len(args.Entries)+1, rf.nextIndex[server])
				rf.matchIndex[server] = Max(args.PrevLogIndex+len(args.Entries), rf.matchIndex[server])
				// 遍历matchIndex查看是否需要提交
				rf.commitLog()
				DPrintf("Finished: {Leader: %d (term: %d)} append entries {index: %d -- %d} to follower %d, matchIndex: %v, nextIndex: %v",
					rf.me, rf.CurrentTerm, args.PrevLogIndex+1, args.PrevLogIndex+len(args.Entries), server, rf.matchIndex, rf.nextIndex)
				return
			}
		}
	}
}

//
// 发送心跳包
// TODO：将心跳和AppendEntries合并，心跳超时时重试，如果nextIndex小于len(rf.LogEntries)-1，则AppendEntries，否者发送心跳
//
func (rf *Raft) sendHeartBeat(heartType HeartType) {
	rf.heartHeatTimer.reset(StableHeartBeatTimeout())
	// 心跳包
	if heartType == HeartsBeat {
		heartLossCount := 0
		for i := range rf.peers {
			if i != rf.me {
				args := &AppendEntriesArgs{}
				args.Term = rf.CurrentTerm
				args.LeaderId = rf.me
				args.LeaderCommit = rf.commitIndex
				index := rf.nextIndex[i] - 1
				args.PrevLogIndex = index
				if args.PrevLogIndex+1 < rf.FirstIndex() {
					// go rf.handleInstallSnapshot(i)
					continue
				}
				if args.PrevLogIndex < rf.FirstIndex() {
					args.PrevLogTerm = rf.LastIncludedTerm
				} else {
					args.PrevLogTerm = rf.LogEntries[args.PrevLogIndex-rf.FirstIndex()].Term
				}
				go func(server int, appendArgs *AppendEntriesArgs) {
					ok := rf.handleHeartBeat(server, appendArgs)
					rf.mu.Lock()
					// 发送心跳失败
					if !ok && appendArgs.Term == rf.CurrentTerm {
						heartLossCount++
						// 收不到大多数心跳，转为Follower
						if heartLossCount > len(rf.peers)/2 {
							DPrintf("{Leader %d(term: %d)-->Follower} cant communicate to other servers, logs length: %d.", rf.me, rf.CurrentTerm, len(rf.LogEntries)+rf.FirstIndex())
							rf.state = Follower
						}
					}
					rf.mu.Unlock()
				}(i, args)
			}
		}
	} else if heartType == AppendEntries { // 添加新的日志
		// 唤醒append(i)协程
		for i := range rf.peers {
			if i != rf.me {
				rf.conds[i].Signal()
			}
		}
		// for i := range rf.peers {
		// 	if i != rf.me {
		// 		// 参数
		// 		args := &AppendEntriesArgs{}
		// 		args.Term = rf.CurrentTerm
		// 		args.LeaderId = rf.me
		// 		args.LeaderCommit = rf.commitIndex
		// 		// start和handleAppendEntries应该是一个整体
		// 		index := rf.nextIndex[i] - 1
		// 		args.PrevLogIndex = index
		// 		if args.PrevLogIndex+1 < rf.FirstIndex() {
		// 			go rf.handleInstallSnapshot(i)
		// 			continue
		// 		}
		// 		if args.PrevLogIndex < rf.FirstIndex() {
		// 			args.PrevLogTerm = rf.LastIncludedTerm
		// 		} else {
		// 			args.PrevLogTerm = rf.LogEntries[args.PrevLogIndex-rf.FirstIndex()].Term
		// 		}
		// 		args.Entries = append(args.Entries, rf.LogEntries[index+1-rf.FirstIndex():]...)
		// 		DPrintf("{Leader: %d (term: %d)} append entries {index: %d -- %d} to follower %d, args: {prevLogIndex: %d, preLogTerm: %d, leaderCommit: %d}",
		// 			rf.me, rf.CurrentTerm, args.PrevLogIndex+1, args.PrevLogIndex+len(args.Entries), i, args.PrevLogIndex, args.PrevLogTerm, args.LeaderCommit)
		// 		go func(server int, appendArgs *AppendEntriesArgs) {
		// 			rf.handleAppendEntries(server, appendArgs)
		// 		}(i, args)
		// 	}
		// }
	}
}
func (rf *Raft) handleHeartBeat(server int, args *AppendEntriesArgs) bool {
	reply := &AppendEntriesReply{}
	// TODO：可否在收不到一半的回复时将leader转换为follower？
	// TODO: 发送心跳时没有匹配如何处理呢？是否需要和日志复制一样处理呢？
	if rf.sendAppendEntries(server, args, reply) {
		rf.mu.Lock()
		// 处理日志不匹配的心跳信息
		if rf.CurrentTerm == args.Term && rf.state == Leader && reply.Success == false {
			// 为什么不能在心跳中修改nextIndex，
			// index := rf.searchConflictLogIndex(reply)
			// rf.nextIndex[server] = index
		}
		rf.ModifyCurrentTerm(reply.Term)
		rf.mu.Unlock()
		return true
	}
	return false
}
func (rf *Raft) handleAppendEntries(server int, args *AppendEntriesArgs) {
	for rf.killed() == false {
		reply := &AppendEntriesReply{}
		if rf.sendAppendEntries(server, args, reply) {
			rf.mu.Lock()
			// 任期相同
			if rf.CurrentTerm == args.Term && rf.state == Leader {
				if reply.Term > rf.CurrentTerm {
					rf.ModifyCurrentTerm(reply.Term)
					rf.mu.Unlock()
					break
				}
				// 日志不匹配，修改参数重新执行
				if reply.Success == false {
					if reply.InSnapshot {
						rf.mu.Unlock()
						break
					}
					// 修改nextIndex重试
					// nextIndex可能已经发生变化了
					index := rf.searchConflictLogIndex(reply)
					rf.nextIndex[server] = index
					args.PrevLogIndex = index - 1
					// 发送快照
					// TODO: 快照之后的日志如何处理？
					// leader发送下一条日志的时候就复制过来了
					if args.PrevLogIndex+1 < rf.FirstIndex() {
						go rf.handleInstallSnapshot(server)
						rf.mu.Unlock()
						break
					}
					if args.PrevLogIndex < rf.FirstIndex() {
						args.PrevLogTerm = rf.LastIncludedTerm
					} else {
						args.PrevLogTerm = rf.LogEntries[args.PrevLogIndex-rf.FirstIndex()].Term
					}
					args.Term = rf.CurrentTerm
					args.LeaderId = rf.me
					args.LeaderCommit = rf.commitIndex
					// start和handleAppendEntries应该是一个整体
					// Entries清空
					args.Entries = append(args.Entries[:0], rf.LogEntries[args.PrevLogIndex+1-rf.FirstIndex():]...)
					DPrintf("{Leader: %d (term: %d)} retry appending entries {index: %d -- %d} to follower %d, args: {prevLogIndex: %d, preLogTerm: %d, leaderCommit: %d}",
						rf.me, rf.CurrentTerm, args.PrevLogIndex+1, args.PrevLogIndex+len(args.Entries), server, args.PrevLogIndex, args.PrevLogTerm, args.LeaderCommit)
					rf.mu.Unlock()
					continue
				} else { // 日志条目匹配上
					// 为什么不利用len(rf.LogEntries)来计算nextIndex和matchIndex？
					// 因为rf.LogEntries可能已经发生变化了
					// 取最大值避免越改越小
					rf.nextIndex[server] = Max(args.PrevLogIndex+len(args.Entries)+1, rf.nextIndex[server])
					rf.matchIndex[server] = Max(args.PrevLogIndex+len(args.Entries), rf.matchIndex[server])
					// 遍历matchIndex查看是否需要提交
					rf.commitLog()
					DPrintf("Finished: {Leader: %d (term: %d)} append entries {index: %d -- %d} to follower %d, matchIndex: %v, nextIndex: %v",
						rf.me, rf.CurrentTerm, args.PrevLogIndex+1, args.PrevLogIndex+len(args.Entries), server, rf.matchIndex, rf.nextIndex)
					rf.mu.Unlock()
					break
				}
			}
			rf.mu.Unlock()
			break
		} else { // 出现网络故障则无限重试
			continue
		}
	}
}

func (rf *Raft) searchConflictLogIndex(reply *AppendEntriesReply) int {
	if reply.ConflictTerm == -1 {
		return reply.MinIndex
	}
	conflictIndex := reply.MinIndex
	for i := len(rf.LogEntries) - 1; i >= 0; i-- {
		if rf.LogEntries[i].Term == reply.ConflictTerm {
			conflictIndex = i + 1 + rf.FirstIndex()
			break
		}
	}
	return conflictIndex
}

//
// 假设存在 N 满足`N > commitIndex`，
// 使得大多数的 `matchIndex[i] ≥ N`以及`log[N].term == currentTerm` 成立，则令 `commitIndex = N`
//
func (rf *Raft) commitLog() {
	matchCopy := make([]int, len(rf.matchIndex))
	copy(matchCopy, rf.matchIndex)
	// 排序
	sort.Ints(matchCopy)
	mid := len(matchCopy) / 2
	N := matchCopy[mid]
	if N > rf.commitIndex && rf.LogEntries[N-rf.FirstIndex()].Term == rf.CurrentTerm {
		rf.commitIndex = N
		DPrintf("{Leader: %d (term: %d)} commit log {index: %d, entry: %+v}, matchIndex: %v", rf.me, rf.CurrentTerm, N, rf.LogEntries[N-rf.FirstIndex()], rf.matchIndex)
	}
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
	// rf.mu.Lock()
	DPrintf("{peer: %d} request vote, term: %d", rf.me, rf.CurrentTerm)
	// 任期号递增
	rf.CurrentTerm = rf.CurrentTerm + 1
	// 为自己投票
	rf.VotedFor = rf.me
	rf.persist()
	// 收到的投票数
	numVotes := 1
	// 最后一条日志索引
	index := len(rf.LogEntries) - 1
	var lastTerm int
	if index >= 0 {
		lastTerm = rf.LogEntries[index].Term
	} else {
		lastTerm = rf.LastIncludedTerm
	}
	args := &RequestVoteArgs{rf.CurrentTerm, rf.me, index + rf.FirstIndex(), lastTerm}
	// rf.mu.Unlock()
	// 向所有服务器发送投票请求
	for i := range rf.peers {
		if i != rf.me {
			go func(server int) {
				reply := &RequestVoteReply{}
				// 收到正常回复
				if rf.sendRequestVote(server, args, reply) {
					rf.mu.Lock()
					defer rf.mu.Unlock()
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
								// 初始nextIndex和matchIndex
								rf.initLeaderIndex()
								// 发送心跳包
								rf.sendHeartBeat(HeartsBeat)
								// 唤醒append协程
								for i := range rf.peers {
									if i != rf.me {
										rf.conds[i].Signal()
									}
								}
							}
							DPrintf("peer %d get vote from %d", rf.me, server)
						} else {
							rf.ModifyCurrentTerm(reply.Term)
							// DPrintf("{peer: %d} (term: %d) refuse voting to %d(term: %d)", server, reply.Term, rf.me, rf.CurrentTerm)
						}
					}
				}
			}(i)
		}
	}
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
				rf.startElection()
				// 重置选举超时时间
				rf.electionTimer.reset(RandomElectionTimeout())
			}
			rf.mu.Unlock()
		}
		// 心跳超时
		if rf.heartHeatTimer.timeout() {
			rf.mu.Lock()
			if rf.state == Leader {
				// leader给所有服务器发送心跳包
				rf.sendHeartBeat(HeartsBeat)
			}
			rf.mu.Unlock()
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
// applyCh将命令应用到状态机
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	// Your initialization code here (2A, 2B, 2C).
	// 初始化计时器和状态
	rf.applyCh = applyCh
	rf.CurrentTerm = 0
	rf.VotedFor = -1
	rf.electionTimer = &Timer{}
	// 计时器重置
	rf.electionTimer.reset(RandomElectionTimeout())
	rf.heartHeatTimer = &Timer{}
	rf.heartHeatTimer.reset(StableHeartBeatTimeout())
	rf.state = Follower
	rf.LogEntries = append(rf.LogEntries, Log{nil, rf.CurrentTerm})
	rf.LastIncludedIndex = -1
	// 初始化持久性状态：CurrentTerm, VotedFor, LogEntries
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	// 使用快照时，不能初始化为0
	rf.commitIndex = rf.LastIncludedIndex
	rf.lastApplied = rf.LastIncludedIndex
	// 初始值为领导人最后的日志条目的索引+1
	rf.nextIndex = make([]int, len(rf.peers))
	index := len(rf.LogEntries)
	for i := range rf.peers {
		rf.nextIndex[i] = index + rf.FirstIndex()
	}
	// 初始值为0
	rf.matchIndex = make([]int, len(rf.peers))
	rf.conds = make([]*sync.Cond, len(rf.peers))
	for i := range rf.conds {
		rf.conds[i] = sync.NewCond(&sync.Mutex{})
	}
	// 启动append协程
	rf.startAppend()
	// 开始选票
	// start ticker goroutine to start elections
	go rf.ticker()
	// 应用到状态机
	go rf.apply()
	return rf
}

//
// 成为leader之后初始化nextIndex和matchIndex
//
func (rf *Raft) initLeaderIndex() {
	index := len(rf.LogEntries)
	for i := range rf.peers {
		// 初始值为领导人最后的日志条目的索引+1
		rf.nextIndex[i] = index + rf.FirstIndex()
		// 初始值为0
		rf.matchIndex[i] = 0
	}
}

//
// 应用到状态机，一次应用多条日志，不然效率太低，当日志太多的时候可能无法在给定时间内达成一致
//
func (rf *Raft) apply() {
	for rf.killed() == false {
		time.Sleep(time.Millisecond * 10)
		rf.mu.Lock()
		if rf.commitIndex > rf.lastApplied {
			entries := make([]Log, rf.commitIndex-rf.lastApplied)
			firstIndex := rf.FirstIndex()
			// TODO: out of range
			// 不能初始化为0
			copy(entries, rf.LogEntries[rf.lastApplied+1-firstIndex:rf.commitIndex+1-firstIndex])
			commitIndex := rf.commitIndex
			rf.mu.Unlock()
			for i, log := range entries {
				msg := ApplyMsg{
					CommandValid: true,
					Command:      log.Command,
					CommandIndex: i + rf.lastApplied + 1,
				}
				rf.applyCh <- msg
			}
			rf.mu.Lock()
			rf.lastApplied = commitIndex
			DPrintf("{Peer %d(term: %d)} apply logs {index: (%d, %d]}", rf.me, rf.CurrentTerm, rf.lastApplied-len(entries), rf.lastApplied)
			rf.mu.Unlock()
		} else {
			rf.mu.Unlock()
		}
	}
}
