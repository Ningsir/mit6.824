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
	"log"
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

const RpcTimeout = time.Millisecond * 100

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
	// 最后一个被包含在快照中的日志的任期
	LastIncludedTerm int

	// 条件变量
	// 用于日志复制的条件变量
	conds []*sync.Cond
	// 用于applier协程的条件变量
	applierCond *sync.Cond
}

//
// 日志条目中第一条日志索引（包括已经写入快照的日志）
//
func (rf *Raft) firstIndexWithoutLock() int {
	return rf.LastIncludedIndex + 1
}

//
// 根据任期切换状态
//
func (rf *Raft) updateCurrentTermWithoutLock(term int) bool {
	if rf.CurrentTerm < term {
		rf.CurrentTerm = term
		rf.VotedFor = -1
		rf.state = Follower
		rf.persist()
		return true
	}
	return false
}

func (rf *Raft) becomeFollower(term int) {
	rf.CurrentTerm = term
	rf.VotedFor = -1
	rf.state = Follower
}

func (rf *Raft) becomeLeader(term int) {
	rf.state = Leader
}

func LogTrim(logs []Log) []Log {
	res := append([]Log{}, logs...)
	logs = nil
	return res
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.CurrentTerm
	if rf.state == Leader {
		isleader = true
	}
	return term, isleader
}

//
// without lock.
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
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.CurrentTerm)
	e.Encode(rf.VotedFor)
	e.Encode(rf.LogEntries)
	e.Encode(rf.LastIncludedIndex)
	e.Encode(rf.LastIncludedTerm)
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
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
		DPrintf("{Peer=%d(Term=%d)} Decode error when readPersist", rf.me, term)
	} else {
		rf.CurrentTerm = term
		rf.VotedFor = votedFor
		rf.LogEntries = logs
		rf.LastIncludedIndex = index
		rf.LastIncludedTerm = lastTerm
		DPrintf("{Peer=%d(Term=%d)} read persist VotedFor=%d, LastIncludedIndex=%d, LastIncludeTerm=%d, Logs=%+v",
			rf.me, term, votedFor, index, lastTerm, logs)
	}
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	firstIndex := rf.firstIndexWithoutLock()
	if index < firstIndex {
		return
	}
	if index < len(rf.LogEntries)+firstIndex {
		rf.LastIncludedIndex = index
		rf.LastIncludedTerm = rf.LogEntries[index-firstIndex].Term
		// 截断日志，那么nextIndex, matchIndex, commitIndex, lastApplied, prevLogIndex都需要做出相应调整
		rf.LogEntries = LogTrim(rf.LogEntries[index+1-firstIndex:])
		// rf.lastApplied = Max(index, rf.lastApplied)
		// rf.commitIndex = Max(index, rf.commitIndex)
		rf.persist()
		rf.persister.SaveStateAndSnapshot(rf.persister.ReadRaftState(), snapshot)
		DPrintf("{Peer=%d(Term=%d, state=%d)} create snapshot with {lastIncludeIndex=%d, lastIncludeTerm=%d}, the length of the rest of logs: %d",
			rf.me, rf.CurrentTerm, rf.state, rf.LastIncludedIndex, rf.LastIncludedTerm, len(rf.LogEntries))
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	firstIndex := rf.firstIndexWithoutLock()
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
	DPrintf("{Leader=%d(Term=%d, state=%d)} append entries to self with {command=%v, index=%d}",
		rf.me, rf.CurrentTerm, rf.state, command, index)
	rf.wakeUpAppend()
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

func (rf *Raft) getHeartBeatArgs(server int) AppendEntriesArgs {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	args := AppendEntriesArgs{}
	args.Term = rf.CurrentTerm
	args.LeaderId = rf.me
	args.LeaderCommit = rf.commitIndex
	index := rf.nextIndex[server] - 1
	args.PrevLogIndex = index
	if args.PrevLogIndex < rf.firstIndexWithoutLock() {
		args.PrevLogTerm = rf.LastIncludedTerm
	} else {
		args.PrevLogTerm = rf.LogEntries[args.PrevLogIndex-rf.firstIndexWithoutLock()].Term
	}
	return args
}

func (rf *Raft) bcastHeartBeat() {
	rf.heartHeatTimer.reset(StableHeartBeatTimeout())
	for i := range rf.peers {
		if i != rf.me {
			go rf.sendHeartBeat(i)
		}
	}
}

func (rf *Raft) sendHeartBeat(server int) {
	args := rf.getHeartBeatArgs(server)
	reply := &AppendEntriesReply{}
	if rf.sendAppendEntries(server, &args, reply) {
		rf.handleAppendAndHeartBeat(server, &args, reply)
	} else {
		DPrintf("WARNING: {Leader=%d(Term=%d)} fails to send heartbeat to Follower=%d with args=%+v",
			args.LeaderId, args.Term, server, args)
	}
}

func (rf *Raft) searchConflictLogIndex(reply *AppendEntriesReply) int {
	if reply.ConflictTerm == -1 {
		return reply.MinIndex
	}
	conflictIndex := reply.MinIndex
	for i := len(rf.LogEntries) - 1; i >= 0; i-- {
		if rf.LogEntries[i].Term == reply.ConflictTerm {
			conflictIndex = i + 1 + rf.firstIndexWithoutLock()
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
	if N > rf.commitIndex && rf.LogEntries[N-rf.firstIndexWithoutLock()].Term == rf.CurrentTerm {
		rf.commitIndex = N
		rf.condWakeUpApplier()
		DPrintf("{Leader=%d(Term=%d, state=%d)} commits log --> commitIndex=%d",
			rf.me, rf.CurrentTerm, rf.state, rf.commitIndex)
	}
}

//
// 处理reply
//
func (rf *Raft) handleAppendAndHeartBeat(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// 任期相同
	if rf.CurrentTerm == args.Term && rf.state == Leader {
		if reply.Term > rf.CurrentTerm {
			if rf.updateCurrentTermWithoutLock(reply.Term) {
				DPrintf("Leader %d convert to Follower", rf.me)
			}
			return
		}
		// 日志不匹配，修改参数重新执行
		if reply.Success == false {
			// 修改nextIndex重试
			index := rf.searchConflictLogIndex(reply)
			rf.nextIndex[server] = index
			return
		} else { // 日志条目匹配上
			// 当一个server对应有多个append协程时需要取最大值
			rf.nextIndex[server] = Max(args.PrevLogIndex+len(args.Entries)+1, rf.nextIndex[server])
			rf.matchIndex[server] = Max(args.PrevLogIndex+len(args.Entries), rf.matchIndex[server])
			// 遍历matchIndex查看是否需要提交
			rf.commitLog()
			DPrintf("Finish Appending Entries: {Leader=%d(Term=%d, state=%d)} finishes appending entries to Follower=%d with {index=[%d, %d), matchIndex=%d, nextIndex=%d, entries=%+v}",
				args.LeaderId, args.Term, rf.state, server, args.PrevLogIndex+1, args.PrevLogIndex+len(args.Entries)+1, rf.matchIndex[server], rf.nextIndex[server], args.Entries)
			return
		}
	}
}

//
// 唤醒appendLoop协程
//
func (rf *Raft) wakeUpAppend() {
	// 唤醒append(i)协程
	for i := range rf.peers {
		if i != rf.me {
			rf.conds[i].Signal()
		}
	}
}

func (rf *Raft) bcastAppend() {
	for i := range rf.peers {
		if i != rf.me {
			go rf.appendLoop(i)
		}
	}
}

func (rf *Raft) needAppend(server int) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.nextIndex[server] < rf.firstIndexWithoutLock()+len(rf.LogEntries) && rf.state == Leader
}

//
// 对于一个Follower，同一时间可能会收到来自Leader的多个append请求，所以需要注意那些过时的请求。
// 问题：如果Follower是reconnect或者重启，那么怎么唤醒呢？
// 不需要唤醒，如果存在日志没有复制到该Follower，那么就不会阻塞，所以也就不需要唤醒。
//
func (rf *Raft) appendLoop(server int) {
	for rf.killed() == false {
		rf.conds[server].L.Lock()
		// 没有新的log需要append或者不是leader, 则一直阻塞
		rf.mu.Lock()
		for !rf.needAppendWithoutLock(server) {
			DPrintf("Current Peer=%d(state=%d, length of logs=%d) appends to Peer=%d(nextIndex=%d) waited",
				rf.me, rf.state, rf.firstIndexWithoutLock()+len(rf.LogEntries), server, rf.nextIndex[server])
			rf.mu.Unlock()
			rf.conds[server].Wait()
			rf.mu.Lock()
		}
		rf.mu.Unlock()
		chFinished := make(chan int)
		// 如果不使用协程，必须等到前一轮append结束才能执行下一轮的append
		// 那么如果出现网络故障，就必须等到请求超时才能执行下一轮的append，导致效率低下。
		go rf.sendAppend(server, chFinished)
		rf.conds[server].L.Unlock()
		// sendAppend执行完成或者执行超时，将发起下一次请求
		select {
		case <-chFinished:
			break
		case <-time.After(RpcTimeout): // 网络故障或者不再是Leader引起请求超时
			break
		}
	}
}

func (rf *Raft) getAppendArgsWithoutLock(server int) AppendEntriesArgs {
	args := AppendEntriesArgs{}
	args.Term = rf.CurrentTerm
	args.LeaderId = rf.me
	args.LeaderCommit = rf.commitIndex
	index := rf.nextIndex[server] - 1
	args.PrevLogIndex = index
	if args.PrevLogIndex < rf.firstIndexWithoutLock() {
		args.PrevLogTerm = rf.LastIncludedTerm
	} else {
		args.PrevLogTerm = rf.LogEntries[args.PrevLogIndex-rf.firstIndexWithoutLock()].Term
	}
	args.Entries = make([]Log, len(rf.LogEntries[index+1-rf.firstIndexWithoutLock():]))
	// 避免AppendEntries rpc修改args引起数据争用
	copy(args.Entries, rf.LogEntries[index+1-rf.firstIndexWithoutLock():])
	return args
}

func (rf *Raft) getInstallSnapshotArgsWithoutLock(server int) InstallSnapshotArgs {
	args := InstallSnapshotArgs{}
	args.Term = rf.CurrentTerm
	args.LeaderId = rf.me
	args.LastIncludedIndex = rf.LastIncludedIndex
	args.LastIncludedTerm = rf.LastIncludedTerm
	args.Data = rf.persister.ReadSnapshot()
	return args
}

func (rf *Raft) needSendSnapshotWithoutLock(server int) bool {
	return rf.nextIndex[server] < rf.firstIndexWithoutLock() && rf.state == Leader
}

func (rf *Raft) needAppendWithoutLock(server int) bool {
	return rf.nextIndex[server] < rf.firstIndexWithoutLock()+len(rf.LogEntries) && rf.state == Leader
}

func (rf *Raft) sendAppend(server int, ch chan int) {
	rf.mu.Lock()
	// Note: 判断是否需要发送快照或者日志和获取rpc参数是一个整体，如果分别加锁，可能在获取参数的时候，并不满足条件。
	// 发送快照
	if rf.needSendSnapshotWithoutLock(server) {
		args := rf.getInstallSnapshotArgsWithoutLock(server)
		reply := &InstallSnapshotReply{}
		DPrintf("Start Send Snapshot: {Leader=%d(Term=%d, state=%d)} sends snapshot to Follower=%d(nextIndex=%d, matchIndex=%d) with {LastIncludedIndex=%d, LastIncludedTerm=%d}",
			args.LeaderId, args.Term, rf.state, server, rf.nextIndex[server], rf.matchIndex[server], args.LastIncludedIndex, args.LastIncludedTerm)
		rf.mu.Unlock()
		if rf.sendInstallSnapshot(server, &args, reply) {
			rf.handleInstallSnapshot(server, &args, reply)
		} else {
			DPrintf("WARNING: {Leader=%d(Term=%d)} fails to send snapshot to Follower=%d with {LastIncludedIndex=%d, LastIncludedTerm=%d}",
				args.LeaderId, args.Term, server, args.LastIncludedIndex, args.LastIncludedTerm)
		}
	} else if rf.needAppendWithoutLock(server) { // 发送日志
		args := rf.getAppendArgsWithoutLock(server)
		reply := &AppendEntriesReply{}
		// 避免AppendEntries rpc修改args引起数据争用
		DPrintf("Start Append: {Leader=%d(Term=%d, state=%d)} appends entries to Follower=%d(nextIndex=%d, matchIndex=%d) with args=%+v",
			args.LeaderId, args.Term, rf.state, server, rf.nextIndex[server], rf.matchIndex[server], args)
		rf.mu.Unlock()
		if rf.sendAppendEntries(server, &args, reply) {
			rf.handleAppendAndHeartBeat(server, &args, reply)
		} else {
			DPrintf("WARNING: {Leader=%d(Term=%d)} fails to append entries to Follower=%d with args=%+v",
				args.LeaderId, args.Term, server, args)
		}
	} else {
		rf.mu.Unlock()
	}
	ch <- 0
}

//
// 处理快照的reply
//
func (rf *Raft) handleInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term == rf.CurrentTerm && rf.state == Leader {
		if !rf.updateCurrentTermWithoutLock(reply.Term) {
			// 当一个server对应有多个append协程时需要取最大值
			rf.nextIndex[server] = Max(args.LastIncludedIndex+1, rf.nextIndex[server])
			rf.matchIndex[server] = Max(args.LastIncludedIndex, rf.matchIndex[server])
			// 遍历matchIndex查看是否需要提交
			rf.commitLog()
			DPrintf("Finish Sending Snapshot: {Leader=%d(Term=%d, state=%d)} finishes handling InstallSnapshot to Follower=%d with {LastIncludedIndex=%d, LastIncludedTerm=%d}-->nextIndex=%d, matchIndex=%d",
				args.LeaderId, args.Term, rf.state, server, args.LastIncludedIndex, args.LastIncludedTerm, rf.nextIndex[server], rf.matchIndex[server])
		}
	}
}

//
// 成为leader之后初始化nextIndex和matchIndex
//
func (rf *Raft) initLeaderIndexWithoutLock() {
	index := len(rf.LogEntries)
	for i := range rf.peers {
		// 初始值为领导人最后的日志条目的索引+1
		rf.nextIndex[i] = index + rf.firstIndexWithoutLock()
		// 初始值为0
		rf.matchIndex[i] = 0
	}
}

func (rf *Raft) getRequestVoteArgsWithoutLock() RequestVoteArgs {
	// 最后一条日志索引
	index := len(rf.LogEntries) - 1
	var lastTerm int
	if index >= 0 {
		lastTerm = rf.LogEntries[index].Term
	} else {
		lastTerm = rf.LastIncludedTerm
	}
	args := RequestVoteArgs{rf.CurrentTerm, rf.me, index + rf.firstIndexWithoutLock(), lastTerm}
	return args
}

//
// 开始选举
//
func (rf *Raft) startElection(args *RequestVoteArgs) {
	// 重置选举超时时间
	rf.electionTimer.reset(RandomElectionTimeout())
	DPrintf("{Peer=%d} requests vote with args=%+v", args.CandidateId, args)
	// 收到的投票数
	numVotes := 1
	for i := range rf.peers {
		if i != rf.me {
			go func(server int) {
				reply := &RequestVoteReply{}
				// 收到正常回复
				if rf.sendRequestVote(server, args, reply) {
					rf.mu.Lock()
					// 检查是否为过期的回复
					if rf.CurrentTerm == args.Term && rf.state == Candidate {
						// 获得投票
						if reply.VoteGranted {
							numVotes += 1
							DPrintf("Peer=%d gets vote from %d", rf.me, server)
							// 选出leader立即发送心跳包，而不是收到所有选举结果后再发送
							if numVotes > len(rf.peers)/2 {
								DPrintf("{Peer=%d(term=%d)} is a leader.", rf.me, rf.CurrentTerm)
								rf.state = Leader
								// 初始nextIndex和matchIndex
								rf.initLeaderIndexWithoutLock()
								// 发送心跳包
								rf.bcastHeartBeat()
							}
						} else {
							rf.updateCurrentTermWithoutLock(reply.Term)
							DPrintf("{Peer=%d(term=%d)} refuse voting to %d(Term=%d, state=%d)",
								server, reply.Term, rf.me, rf.CurrentTerm, rf.state)
						}
					}
					rf.mu.Unlock()
				} else {
					DPrintf("WARNING: {Candidate=%d(term=%d)} fails to request vote to Follower=%d with args=%+v",
						args.CandidateId, args.Term, server, args)
				}
			}(i)
		}
	}
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
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
				// 任期号递增
				rf.CurrentTerm = rf.CurrentTerm + 1
				// 为自己投票
				rf.VotedFor = rf.me
				rf.persist()
				args := rf.getRequestVoteArgsWithoutLock()
				rf.startElection(&args)
			}
			rf.mu.Unlock()
		}
		// 心跳超时
		if rf.heartHeatTimer.timeout() {
			rf.mu.Lock()
			if rf.state == Leader {
				// leader给所有服务器发送心跳包
				rf.bcastHeartBeat()
			}
			rf.mu.Unlock()
		}
	}
}

//
// 唤醒applier协程
//
func (rf *Raft) condWakeUpApplier() {
	if rf.commitIndex > rf.lastApplied {
		rf.applierCond.Signal()
		DPrintf("{Peer=%d(Term=%d, state=%d)} wake up applier with {commitIndex=%d, lastApplied=%d}",
			rf.me, rf.CurrentTerm, rf.state, rf.commitIndex, rf.lastApplied)
	}
}

func (rf *Raft) needApply() bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.commitIndex > rf.lastApplied
}

func (rf *Raft) NeedApply() bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.commitIndex > rf.lastApplied
}

//
// 应用到状态机，一次应用多条日志，不然效率太低，当日志太多的时候可能无法在给定时间内达成一致
//
func (rf *Raft) applier() {
	for rf.killed() == false {
		rf.applierCond.L.Lock()
		for !rf.needApply() {
			rf.applierCond.Wait()
		}
		rf.mu.Lock()
		// 双重检查：因为在执行完`needApply()`之后lastApplied可能又发生了变化。
		if rf.commitIndex > rf.lastApplied {
			entries := make([]Log, rf.commitIndex-rf.lastApplied)
			firstIndex := rf.firstIndexWithoutLock()
			if rf.lastApplied+1 < firstIndex {
				log.Fatalf("Server %d: lastApplied %d < firstIndex %d", rf.me, rf.lastApplied, firstIndex)
			}
			copy(entries, rf.LogEntries[rf.lastApplied+1-firstIndex:rf.commitIndex+1-firstIndex])
			lastApplied := rf.lastApplied
			commitIndex := rf.commitIndex
			// 先修改lastApplied再将日志传输到通道中，与InstallSnapshot保持一致
			// 下面两项操作都是一个整体，如果在执行1修改lastApplied后, 再执行InstallSnapshot修改lastApplied,
			// 然后再执行applyCh<-snapshot, 最后执行applyCh<-msg, 那么将导致apply out of order, 也就是将已经应用到状态机的日志再次应用到状态机。
			// 1. applier: lastApplied->log->applyCh
			// 2. InstallSnapshot: lastApplied->snapshot->applyCh
			rf.lastApplied = commitIndex
			DPrintf("{Peer=%d(Term=%d, state=%d)} apply logs {index: (%d, %d]}",
				rf.me, rf.CurrentTerm, rf.state, rf.lastApplied-len(entries), rf.lastApplied)
			rf.mu.Unlock()
			for i, log := range entries {
				rf.mu.Lock()
				// 如果rf.lastApplied被InstallSnapshot修改，则这些日志不再需要被apply，因为快照中已经apply
				if commitIndex == rf.lastApplied {
					rf.mu.Unlock()
					msg := ApplyMsg{
						CommandValid: true,
						Command:      log.Command,
						CommandIndex: i + lastApplied + 1,
					}
					rf.applyCh <- msg
				} else {
					DPrintf("{Peer=%d(Term=%d, state=%d)} lastApplied changed {old=%d, new=%d}",
						rf.me, rf.CurrentTerm, rf.state, commitIndex, rf.lastApplied)
					rf.mu.Unlock()
				}
			}
		} else {
			rf.mu.Unlock()
		}
		rf.applierCond.L.Unlock()
	}
}

func (rf *Raft) LastTerm() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	index := len(rf.LogEntries) - 1
	if index < 0 {
		return rf.LastIncludedTerm
	}
	return rf.LogEntries[index].Term
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
	rf.applyCh = applyCh
	rf.CurrentTerm = 0
	rf.VotedFor = -1
	rf.electionTimer = &Timer{}
	// 计时器重置
	rf.electionTimer.reset(RandomElectionTimeout())
	rf.heartHeatTimer = &Timer{}
	rf.heartHeatTimer.reset(StableHeartBeatTimeout())
	rf.state = Follower
	rf.LogEntries = []Log{{nil, rf.CurrentTerm}}
	rf.LastIncludedIndex = -1
	// 初始化持久性状态：CurrentTerm, VotedFor, LogEntries
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	// 使用快照时，不能初始化为0
	rf.commitIndex = rf.LastIncludedIndex
	if rf.LastIncludedIndex == -1 {
		rf.lastApplied = 0
	} else {
		rf.lastApplied = rf.LastIncludedIndex
	}
	// 初始值为领导人最后的日志条目的索引+1
	rf.nextIndex = make([]int, len(rf.peers))
	index := len(rf.LogEntries)
	for i := range rf.peers {
		rf.nextIndex[i] = index + rf.firstIndexWithoutLock()
	}
	// 初始值为0
	rf.matchIndex = make([]int, len(rf.peers))
	rf.conds = make([]*sync.Cond, len(rf.peers))
	for i := range rf.conds {
		rf.conds[i] = sync.NewCond(&sync.Mutex{})
	}
	rf.applierCond = sync.NewCond(&sync.Mutex{})
	// 启动append协程
	rf.bcastAppend()
	// start ticker goroutine to start elections
	go rf.ticker()
	// 应用到状态机
	go rf.applier()

	return rf
}
