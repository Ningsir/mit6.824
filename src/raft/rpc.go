package raft

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.CurrentTerm > args.Term {
		reply.VoteGranted = false
		reply.Term = rf.CurrentTerm
		return
	}
	rf.updateCurrentTermWithoutLock(args.Term)
	reply.VoteGranted = false
	reply.Term = rf.CurrentTerm
	if rf.VotedFor == -1 || rf.VotedFor == args.CandidateId {
		// 候选人的日志至少和自己一样新
		index := len(rf.LogEntries) - 1
		var lastTerm int
		if index < 0 {
			lastTerm = rf.LastIncludedTerm
		} else {
			lastTerm = rf.LogEntries[index].Term
		}
		lastIndex := rf.firstIndexWithoutLock() + index
		// 如果两份日志最后的条目的任期号不同，那么任期号大的日志更加新。
		// 如果两份日志最后的条目任期号相同，那么日志比较长的那个就更加新。
		if lastTerm < args.LastLogTerm ||
			((lastTerm == args.LastLogTerm) && lastIndex <= args.LastLogIndex) {
			rf.electionTimer.reset(RandomElectionTimeout())
			// 给候选者投票
			rf.VotedFor = args.CandidateId
			reply.VoteGranted = true
			reply.Term = rf.CurrentTerm
			DPrintf("Peer=%d(Term=%d, lastTerm=%d, lastLogIndex=%d) votes for %d(lastTerm=%d, lastLogIndex=%d)",
				rf.me, rf.CurrentTerm, lastTerm, lastIndex, rf.VotedFor, args.LastLogTerm, args.LastLogIndex)
			return
		}
	}
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
	// 重置选举超时计时器
	rf.electionTimer.reset(RandomElectionTimeout())
	// leader任期过小
	if args.Term < rf.CurrentTerm {
		reply.Success, reply.Term = false, rf.CurrentTerm
		return
	}
	rf.updateCurrentTermWithoutLock(args.Term)
	firstIndex := rf.firstIndexWithoutLock()
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
		DPrintf("{Follower=%d(Term=%d)}, ConflictTerm=%d, MinIndex=%d} mismatch {Leader=%d(Term=%d), PrevLogIndex=%d, PrevLogTerm=%d}",
			rf.me, rf.CurrentTerm, reply.ConflictTerm, reply.MinIndex, args.LeaderId, args.Term, args.PrevLogIndex, args.PrevLogTerm)
		return
	}

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
		DPrintf("{Follower=%d(Term=%d)} append entries with last entry=%+v",
			rf.me, rf.CurrentTerm, rf.LogEntries[len(rf.LogEntries)-1])
	}
	// 根据参数leaderCommit更新commitIndex
	if args.LeaderCommit > rf.commitIndex {
		// 是与最后一个新条目的索引取最小值
		// 因为发送心跳时，日志条目为空，那么LeaderCommit可能大于最大索引
		rf.commitIndex = Min(args.LeaderCommit, args.PrevLogIndex+len(args.Entries))
		rf.condWakeUpApplier()
		DPrintf("{Follower=%d(Term=%d)} commits log {commitIndex=%d, lastApplied=%d}", rf.me, rf.CurrentTerm, rf.commitIndex, rf.lastApplied)
	}
	reply.Term = rf.CurrentTerm
	reply.Success = true
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	// follower安装快照，并将快照传递给服务端
	// 还有可能leader两次append，那么就出现两次调用两轮InstallSnapshot
	rf.mu.Lock()
	DPrintf("{Follower=%d(Term=%d)} receives snapshot from {Leader=%d(Term=%d)} with {lastIncludeIndex=%d, lastIncludedTerm=%d}",
		rf.me, rf.CurrentTerm, args.LeaderId, args.Term, args.LastIncludedIndex, args.LastIncludedTerm)
	rf.electionTimer.reset(RandomElectionTimeout())
	if args.Term < rf.CurrentTerm {
		reply.Term = rf.CurrentTerm
		rf.mu.Unlock()
		return
	}
	rf.updateCurrentTermWithoutLock(args.Term)
	// 旧的快照，直接丢弃
	if rf.commitIndex >= args.LastIncludedIndex {
		reply.Term = rf.CurrentTerm
		rf.mu.Unlock()
		return
	}
	firstIndex := rf.firstIndexWithoutLock()
	rf.LastIncludedIndex = args.LastIncludedIndex
	rf.LastIncludedTerm = args.LastIncludedTerm
	// 截断日志，commitIndex, lastApplied需要做出相应调整
	if args.LastIncludedIndex >= firstIndex+len(rf.LogEntries) {
		rf.LogEntries = rf.LogEntries[:0]
	} else {
		rf.LogEntries = rf.LogEntries[args.LastIncludedIndex+1-firstIndex:]
	}
	rf.lastApplied = args.LastIncludedIndex
	rf.commitIndex = args.LastIncludedIndex
	rf.persist()
	rf.persister.SaveStateAndSnapshot(rf.persister.ReadRaftState(), args.Data)
	DPrintf("{Peer=%d(Term=%d, lastApplied=%d, commitIndex=%d)} InstallSnapshot:{lastIncludeIndex=%d, lastIncludeTerm=%d}, the length of the rest of logs: %d",
		rf.me, rf.CurrentTerm, rf.lastApplied, rf.commitIndex, rf.LastIncludedIndex, rf.LastIncludedTerm, len(rf.LogEntries))
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
