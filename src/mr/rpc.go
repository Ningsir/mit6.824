package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//
type TaskState int

// 任务状态
const (
	// 任务完成
	Finished TaskState = 0
	// 工作中
	Working TaskState = 1
	// 就绪状态
	Ready TaskState = 2
)

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

type MapReduceArgs struct {
	// 已经完成任务的任务id
	TaskId int
}

type MapReply struct {
	FileName string
	NReduce  int
	TaskId   int
	// map任务状态
	State TaskState
}

type ReduceReply struct {
	FileName string
	TaskId   int
	// reduce任务状态
	State TaskState
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
