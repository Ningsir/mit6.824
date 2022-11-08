package raft

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
	MinIndex int
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
