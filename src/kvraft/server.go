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

const ExecuteTimeout = time.Millisecond * 200

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
	CommandId int64
	Response  string
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
	lastApplied int
	persister   *raft.Persister
	// 记录每个客户端最后一次应用的命令id及其响应，防止同一个客户端重复执行同一条命令
	clientLastApplied map[int64]IdAndResponse
	resultChan        map[int]chan string
}

//
// 如果命令ID小于clientLastApplied中对应客户端的命令ID，则表示该命令已经被执行过。
//
func (kv *KVServer) duplicateCommand(op Op) bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	value, ok := kv.clientLastApplied[op.ClientId]
	// 命令已经被执行过
	if ok && value.CommandId >= op.Id {
		return true
	}
	return false
}

func (kv *KVServer) duplicateCommandWithoutLock(op Op) bool {
	value, ok := kv.clientLastApplied[op.ClientId]
	// 命令已经被执行过
	if ok && value.CommandId >= op.Id {
		return true
	}
	return false
}

func (kv *KVServer) removeResultChan(index int) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	delete(kv.resultChan, index)
}

func (kv *KVServer) getResultChanWithoutLock(index int) (chan string, bool) {
	ch, ok := kv.resultChan[index]
	return ch, ok
}

// 1. 如何获取到执行结果？
// 使用通道，applier将执行结果传输到对应通道中。
// 2. 如何确保同一客户端的命令不被重复执行?
// <clientId, sequenceNum>唯一标识一条命令
// 3. 调用Start之后一定能够获取到结果吗？
// 不一定。可能是old leader，这样日志就不会被提交也就不会被应用到状态机
// 4. 如何检测执行过程server状态发生了改变了呢？
// 等待超时即可
func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.

	// 1. 得到命令Op
	// 2. 调用raft中的start()将命令复制到日志中
	// 3. 判断命令是否已经被应用到状态机中，如果应用到状态机中，则返回对应结果
	kv.mu.Lock()
	op := Op{GET, args.Key, "", args.Id, args.ClientId}
	index, oldTerm, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	}

	DPrintf("kvraft: Server=%d(raft Term=%d) Get with {args=%+v, command index=%d}",
		kv.me, oldTerm, args, index)
	// 使用默认的无缓冲区的通道：可能超时退出导致发送方一直阻塞。
	// 使用带缓冲区的通道，避免超时退出导致发送方一直阻塞。
	kv.resultChan[index] = make(chan string, 1)
	ch := kv.resultChan[index]
	kv.mu.Unlock()
	select {
	case result := <-ch:
		DPrintf("kvraft: Server=%d(raft Term=%d) Get(index=%d) result=%+v",
			kv.me, oldTerm, index, result)
		if result == ErrNoKey {
			reply.Err = ErrNoKey
		} else {
			reply.Err = OK
			reply.Value = result
		}
	case <-time.After(ExecuteTimeout): // 网络故障或者不再是Leader引起请求超时
		DPrintf("kvraft: Server=%d(raft Term=%d) Get(index=%d) Timeout",
			kv.me, oldTerm, index)
		reply.Err = ErrTimeout
	}
	kv.removeResultChan(index)
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	op := Op{}
	if args.Op == "Put" {
		op = Op{PUT, args.Key, args.Value, args.Id, args.ClientId}

	} else if args.Op == "Append" {
		op = Op{APPEND, args.Key, args.Value, args.Id, args.ClientId}
	} else {
		log.Fatalf("must be Put or Append command")
		return
	}
	kv.mu.Lock()
	if kv.duplicateCommandWithoutLock(op) {
		DPrintf("kvraft: Server=%d process duplicate command=%+v, clientLastAplied: %v",
			kv.me, op, kv.clientLastApplied)
		reply.Err = OK
		kv.mu.Unlock()
		return
	}
	index, oldTerm, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	}
	DPrintf("kvraft: Server=%d(raft term=%d) PutAppend with {args=%+v, command index=%d}",
		kv.me, oldTerm, args, index)
	kv.resultChan[index] = make(chan string, 1)
	ch := kv.resultChan[index]
	kv.mu.Unlock()
	select {
	case result := <-ch:
		DPrintf("kvraft: Server=%d(raft Term=%d) PutAppend(index=%d) result=%+v",
			kv.me, oldTerm, index, result)
		reply.Err = OK
	case <-time.After(ExecuteTimeout):
		DPrintf("kvraft: Server=%d(raft Term=%d) PutAppend(index=%d) Timeout",
			kv.me, oldTerm, index)
		reply.Err = ErrTimeout
	}
	kv.removeResultChan(index)
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

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.

	kv.persister = persister
	kv.data = make(map[string]string)
	kv.lastApplied = 0
	kv.clientLastApplied = make(map[int64]IdAndResponse)
	kv.resultChan = make(map[int]chan string)
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
	data := make(map[string]string)
	clientMap := make(map[int64]IdAndResponse)
	if d.Decode(&data) != nil || d.Decode(&clientMap) != nil {
		log.Fatalf("kvraft: ReadSnapshot Decode error")
	} else {
		kv.mu.Lock()
		kv.data = data
		kv.clientLastApplied = clientMap
		DPrintf("kvraft: Server=%d read sanpshot: %+v, clientLastApplied: %+v", kv.me, kv.data, kv.clientLastApplied)
		kv.mu.Unlock()
	}
}

func (kv *KVServer) executeCommandWithoutLock(op Op) string {
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
	for !kv.killed() {
		select {
		case m := <-kv.applyCh:
			// 处理快照
			if m.SnapshotValid {
				DPrintf("kvraft: Server=%d CondInstallsnapshot, lastIncludedIndex=%d, lastIncludeTerm=%d, lastApplied=%d",
					kv.me, m.SnapshotIndex, m.SnapshotTerm, kv.lastApplied)
				if kv.rf.CondInstallSnapshot(m.SnapshotTerm,
					m.SnapshotIndex, m.Snapshot) {
					kv.lastApplied = m.SnapshotIndex
					if m.Snapshot == nil || len(m.Snapshot) < 1 {
						log.Fatalf("kvraft: CondInstallsnapshot read nil snapshot")
						return
					}
					kv.readSnapshot(m.Snapshot)
				} else {
					DPrintf("kvraft: Server=%d Installsnapshot: get old snapshot", kv.me)
				}
			} else if m.CommandValid {
				// interface转换为struct
				op := m.Command.(Op)
				kv.mu.Lock()
				// 需要保证`m.CommandIndex`处的命令没有发生变化
				ch, ok := kv.getResultChanWithoutLock(m.CommandIndex)
				kv.lastApplied = m.CommandIndex
				// 重复的Put或者Append命令直接返回结果
				// 重复的命令可以写入到日志中，但是不能让重复的命令去改变状态机
				var res string
				if op.Type != GET && kv.duplicateCommandWithoutLock(op) {
					DPrintf("kvraft: Server=%d process duplicate command=%+v, clientLastAplied: %v",
						kv.me, op, kv.clientLastApplied)
					res = OK
				} else {
					// 执行对应命令, 并将结果写入commandResult
					res = kv.executeCommandWithoutLock(op)
					// read命令可以重复执行
					if op.Type != GET {
						kv.clientLastApplied[op.ClientId] = IdAndResponse{op.Id, res}
					}
					DPrintf("kvraft: Server=%d apply {commandIndex=%d, lastApplied=%d, command=%+v}", kv.me, m.CommandIndex, kv.lastApplied, m.Command)
				}
				kv.mu.Unlock()
				// 为什么只有leader可以返回结果？
				// 因为leader转为follower后，index对应的命令可能发生了变化，也就是返回的结果不是最初的那条命令的结果
				// TODO: 确保任期没有变化
				if _, isLeader := kv.rf.GetState(); isLeader && ok {
					ch <- res
				}
				// 快照包括当前复制状态机的数据
				if kv.maxraftstate != -1 && kv.persister.RaftStateSize() > kv.maxraftstate {
					DPrintf("kvraft: Server=%d Start Install Snapshot, raft size=%d, maxraftstate=%d, lastIncudedIndex=%d",
						kv.me, kv.persister.RaftStateSize(), kv.maxraftstate, m.CommandIndex)
					w := new(bytes.Buffer)
					e := labgob.NewEncoder(w)
					// encode：或者使用大写字段的结构体，或者将其复制到另外字段
					v := kv.data
					clientLastApplied := kv.clientLastApplied
					e.Encode(v)
					e.Encode(clientLastApplied)
					kv.rf.Snapshot(m.CommandIndex, w.Bytes())
					DPrintf("kvraft: Server=%d Finish Install Snapshot, snapshot size=%d, raft size=%d, lastIncudedIndex=%d, clientLastAplied=%+v",
						kv.me, len(w.Bytes()), kv.persister.RaftStateSize(), m.CommandIndex, kv.clientLastApplied)
				}
			} else {
				DPrintf("kvraft: Server=%d Ignore log or snapshot: lastApplied=%d, applyCh=%+v", kv.me, kv.lastApplied, m)
			}
		}
	}
	DPrintf("kvraft: Server=%d applier finished", kv.me)
}
