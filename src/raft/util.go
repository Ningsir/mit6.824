package raft

import "log"

// Debugging
const Debug = true

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
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

func DeepCopyAppendEntriesArgs(args *AppendEntriesArgs) AppendEntriesArgs {
	res := AppendEntriesArgs{}
	res.LeaderId = args.LeaderId
	res.Term = args.Term
	res.LeaderCommit = args.LeaderCommit
	res.PrevLogIndex = args.PrevLogIndex
	res.PrevLogTerm = args.PrevLogTerm
	res.Entries = make([]Log, len(args.Entries))
	copy(res.Entries, args.Entries)
	return res
}
