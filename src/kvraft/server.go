package kvraft

import (
	"bytes"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

const Debug = true

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type OpType int

const (
	GET    OpType = 0
	APPEND OpType = 1
	PUT    OpType = 2
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	// 命令类型
	Type  OpType
	Key   string
	Value string
	// 命令ID
	Id int64
	// 客户端ID
	ClientId int64
}

type IdAndResponse struct {
	commandId int64
	response  string
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	// 存储kv store中的数据
	data map[string]string
	// 记录已经被应用到状态机中的命令id，防止重复执行同一条命令
	// hasApplied  map[int64]bool
	lastApplied int
	persister   *raft.Persister
	// 记录每个客户端最后一次应用的命令id及其响应
	clientLastApplied map[int64]IdAndResponse
	// 命令执行结果
	commandResult map[int]string
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	// 1. 如何知道已经应用到状态机？
	// applyCh返回的索引等于Start返回的索引，表示该命令已经被提交

	// 2. 如何确保命令不被重复执行?
	// read命令重复执行没有影响

	// 3. 如何判断命令提交失败？可能leader被取代了
	// 任期发生变化

	// 1. 得到命令Op
	// 2. 调用raft中的start()将命令复制到日志中
	// 3. 判断命令是否已经被应用到状态机中，如果应用到状态机中，则返回对应结果
	op := Op{GET, args.Key, "", args.Id, args.ClientId}
	index, oldTerm, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	kv.mu.Lock()
	id, ok := kv.clientLastApplied[args.ClientId]
	kv.mu.Unlock()
	// 命令已经被执行过
	if ok && id.commandId == args.Id {
		reply.Err = OK
		reply.Value = id.response
		return
	}
	DPrintf("Server %d Get args: %+v", kv.me, args)
	for true {
		time.Sleep(time.Millisecond * 10)
		kv.mu.Lock()
		value, ok := kv.commandResult[index]
		// 已经应用到状态机
		if ok {
			reply.Err = OK
			reply.Value = value
			delete(kv.commandResult, index)
			kv.mu.Unlock()
			return
		}
		// _, ok := kv.hasApplied[args.Id]
		// // 已经应用到状态机
		// if ok {
		// 	value, ok := kv.data[args.Key]
		// 	if ok {
		// 		reply.Err = OK
		// 		reply.Value = value
		// 	} else {
		// 		reply.Err = ErrNoKey
		// 	}
		// 	kv.mu.Unlock()
		// 	return
		// }
		var currentTerm int
		currentTerm, isLeader = kv.rf.GetState()
		// 不再是leader
		if !isLeader || oldTerm != currentTerm {
			DPrintf("Old leader %d, old term %d to new term %d", kv.me, oldTerm, currentTerm)
			reply.Err = ErrWrongLeader
			kv.mu.Unlock()
			return
		}
		kv.mu.Unlock()
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	op := Op{}
	if args.Op == "Put" {
		op = Op{PUT, args.Key, args.Value, args.Id, args.ClientId}

	} else if args.Op == "Append" {
		op = Op{APPEND, args.Key, args.Value, args.Id, args.ClientId}
	} else {
		DPrintf("must be Put or Append command")
		return
	}
	kv.mu.Lock()
	id, ok := kv.clientLastApplied[args.ClientId]
	kv.mu.Unlock()
	// 命令已经被执行过
	if ok && id.commandId == args.Id {
		reply.Err = OK
		return
	}
	index, oldTerm, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	DPrintf("Server %d PutAppend args: %+v", kv.me, args)
	// TODO: 使用循环判断命令是否被执行，然后再返回给客户端，不能及时返回结果，具有一定延迟
	// 条件变量？
	for true {
		time.Sleep(time.Millisecond * 10)
		kv.mu.Lock()
		// _, ok := kv.hasApplied[args.Id]
		_, ok := kv.commandResult[index]
		// 已经应用到状态机
		if ok {
			reply.Err = OK
			delete(kv.commandResult, index)
			kv.mu.Unlock()
			return
		}
		var currentTerm int
		currentTerm, isLeader = kv.rf.GetState()
		// leader->follower->leader没有察觉出来已经进行了一次leader选举
		// 不再是leader: 任期发生变化也不再是leader
		if !isLeader || oldTerm != currentTerm {
			DPrintf("Old leader %d, old term %d to new term %d", kv.me, oldTerm, currentTerm)
			reply.Err = ErrWrongLeader
			kv.mu.Unlock()
			return
		}
		kv.mu.Unlock()
	}
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	// TODO: server重启, 读取快照, ReadSnapshot
	// 快照内容：复制状态机中的数据

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.persister = persister
	kv.data = make(map[string]string)
	// kv.hasApplied = make(map[int64]bool)
	kv.clientLastApplied = make(map[int64]IdAndResponse)
	kv.commandResult = make(map[int]string)
	kv.readSnapshot(persister.ReadSnapshot())
	go kv.applier()
	return kv
}

func (kv *KVServer) readSnapshot(snapshot []byte) {
	if snapshot == nil || len(snapshot) < 1 {
		return
	}
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	var data map[string]string
	var clientMap map[int64]IdAndResponse
	if d.Decode(&data) != nil && d.Decode(&clientMap) != nil {
		DPrintf("ReadSnapshot Decode error")
	} else {
		kv.mu.Lock()
		kv.data = data
		kv.clientLastApplied = clientMap
		kv.mu.Unlock()
		DPrintf("server %d read sanpshot: %+v", kv.me, data)
	}
}
func (kv *KVServer) executeCommand(op Op) string {
	switch op.Type {
	case GET:
		value, ok := kv.data[op.Key]
		if ok {
			return value
		} else {
			return ErrNoKey
		}
	case APPEND:
		old := kv.data[op.Key]
		kv.data[op.Key] = old + op.Value
	case PUT:
		kv.data[op.Key] = op.Value
	}
	return OK
}

// 应用到状态机
func (kv *KVServer) applier() {
	// lastApplied := 0
	for m := range kv.applyCh {
		// 处理快照
		if m.SnapshotValid {
			DPrintf("Server %d CondInstallsnapshot, lastIncludedIndex: %v, lastApplied: %v\n", kv.me, m.SnapshotIndex, kv.lastApplied)
			if kv.rf.CondInstallSnapshot(m.SnapshotTerm,
				m.SnapshotIndex, m.Snapshot) {
				kv.lastApplied = m.SnapshotIndex
				kv.readSnapshot(m.Snapshot)
			} else {
				DPrintf("Server %d Installsnapshot: get old snapshot", kv.me)
			}
		} else if m.CommandValid && m.CommandIndex > kv.lastApplied {
			DPrintf("Server %d apply commandIndex %v lastApplied %v, command: %+v\n", kv.me, m.CommandIndex, kv.lastApplied, m.Command)
			kv.mu.Lock()
			kv.lastApplied = m.CommandIndex
			// interface转换为struct
			op := m.Command.(Op)
			id, ok := kv.clientLastApplied[op.ClientId]
			// 命令已经执行过
			if ok && id.commandId == op.Id {
				kv.mu.Unlock()
				continue
			}
			// 执行对应命令, 并将结果写入commandResult
			res := kv.executeCommand(op)
			kv.commandResult[m.CommandIndex] = res
			// 执行完的命令如何将结果返回给客户端呢
			// index: response
			// 表示该条命令已经被应用到状态机
			// kv.hasApplied[op.Id] = true
			kv.clientLastApplied[op.ClientId] = IdAndResponse{op.Id, res}
			kv.mu.Unlock()
			// 快照包括当前复制状态机的数据
			if kv.maxraftstate != -1 && kv.persister.RaftStateSize() > kv.maxraftstate {
				DPrintf("Server %d Start Install Snapshot, raft size: %d, maxraftstate: %d, lastIncudedIndex: %d",
					kv.me, kv.persister.RaftStateSize(), kv.maxraftstate, m.CommandIndex)
				w := new(bytes.Buffer)
				e := labgob.NewEncoder(w)
				v := kv.data
				e.Encode(v)
				e.Encode(kv.clientLastApplied)
				kv.rf.Snapshot(m.CommandIndex, w.Bytes())
				DPrintf("Server %d Finish Install Snapshot, raft size: %d, lastIncudedIndex: %d",
					kv.me, kv.persister.RaftStateSize(), m.CommandIndex)
			}
		} else {
			DPrintf("Ignore: Index %v lastApplied %v\n", m.CommandIndex, kv.lastApplied)
		}
	}
	DPrintf("Node: %d applier finished", kv.me)
}
