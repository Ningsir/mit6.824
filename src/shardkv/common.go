package shardkv

//
// Sharded key/value server.
// Lots of replica groups, each running Raft.
// Shardctrler decides which group serves each shard.
// Shardctrler may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK                = "OK"
	ErrNoKey          = "ErrNoKey"
	ErrWrongGroup     = "ErrWrongGroup"
	ErrWrongLeader    = "ErrWrongLeader"
	ErrTimeout        = "ErrTimeout"
	ErrNotServing     = "ErrNotServing"
	ErrWrongConfigNum = "ErrWrongConfigNum"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Id       int64
	ClientId int64
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
}

type GetReply struct {
	Err   Err
	Value string
}

type PushShardArgs struct {
	Data              map[int]ShardKVStorage
	ConfigNum         int
	ClientLastApplied map[int64]IdAndResponse
}

type PushShardReply struct {
	Err Err
}

type DeleteShardArgs struct {
	ShardIds  []int
	ConfigNum int
}

type DeleteShardReply struct {
	Err Err
}
