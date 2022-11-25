package shardkv

import (
	"bytes"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"6.824/shardctrler"
)

const Debug = true

const ExecuteTimeout = time.Millisecond * 200

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type CommandType string

const (
	// get append put
	Operations CommandType = "Operations"
	// 在group中增加shard
	CreateShard CommandType = "CreateShard"
	// 删除shard
	DeleteShard CommandType = "DeleteShard"
	// 更新配置
	UpdateConfig CommandType = "UpdateConfig"
)

type CommonOp struct {
	Type    CommandType
	Command interface{}
}

type OpType string

const (
	GET     OpType = "GET"
	APPEND  OpType = "APPEND"
	PUT     OpType = "PUT"
	EMPTYOP OpType = "EMPTYOP"
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

// for (get, append, put), (insert and delete shard), (update config)
type ApplyResult struct {
	Err   Err
	Value string
}

type ShardKV struct {
	mu       sync.Mutex
	me       int
	rf       *raft.Raft
	applyCh  chan raft.ApplyMsg
	make_end func(string) *labrpc.ClientEnd
	gid      int
	ctrlers  []*labrpc.ClientEnd
	// 控制器客户端
	ctrCk        *shardctrler.Clerk
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	dead int32 // set by Kill()
	// 存储kv store中的数据，通过shard id和key进行访问
	storage map[int]*ShardKVStorage
	// 记录已经被应用到状态机中的命令id，防止重复执行同一条命令
	lastApplied int
	persister   *raft.Persister
	// 记录每个客户端最后一次应用的命令id及其响应，防止同一个客户端重复执行同一条命令
	clientLastApplied map[int64]IdAndResponse
	resultChan        map[int]chan ApplyResult
	lastConfig        shardctrler.Config
	currentConfig     shardctrler.Config
}

func (kv *ShardKV) duplicateCommandWithoutLock(op Op) bool {
	value, ok := kv.clientLastApplied[op.ClientId]
	// 命令已经被执行过
	if ok && value.CommandId >= op.Id {
		return true
	}
	return false
}

func (kv *ShardKV) makeResultChan(index int) chan ApplyResult {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	kv.resultChan[index] = make(chan ApplyResult, 1)
	return kv.resultChan[index]
}

func (kv *ShardKV) removeResultChan(index int) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	delete(kv.resultChan, index)
}

func (kv *ShardKV) getResultChan(index int) (chan ApplyResult, bool) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	ch, ok := kv.resultChan[index]
	return ch, ok
}

func (kv *ShardKV) inCurrentGroup(key string) bool {
	shard := key2shard(key)
	gid := kv.currentConfig.Shards[shard]
	return gid == kv.gid
}

func (kv *ShardKV) isServing(key string) bool {
	shard := key2shard(key)
	return kv.storage[shard].Status == Serving
}

//
// 当shard迁移还没有完成则不能拉取新的config
//
func (kv *ShardKV) needPullConfig() bool {
	if len(kv.storage) == 0 || kv.storage == nil {
		return true
	}
	for _, storage := range kv.storage {
		if storage.Status != Serving {
			return false
		}
	}
	return true
}

func (kv *ShardKV) pullConfig() {
	for !kv.killed() {
		if _, isLeader := kv.rf.GetState(); isLeader {
			kv.mu.Lock()
			// 1. 首先判断是否可以拉取新的配置，如果所有shard都是Serving状态，则可以拉取最新配置。
			if kv.needPullConfig() {
				// 2. 拉取配置，看是否比现在的配置更新。
				num := kv.currentConfig.Num
				DPrintf("shardkv pullconfig started: {Server=%d, group=%d} configNum=%d",
					kv.me, kv.gid, num+1)
				kv.mu.Unlock()
				// **注意**：需要释放锁，因为Query()是一个阻塞操作，比较耗时
				config := kv.ctrCk.Query(num + 1)
				// 避免拉取到空的config
				if config.Num == num+1 {
					DPrintf("shardkv pullconfig finished: {Server=%d, group=%d} config=%+v",
						kv.me, kv.gid, config)
					// 3. 执行`rf.start()`同步配置
					command := CommonOp{}
					command.Type = UpdateConfig
					command.Command = config
					kv.Execute(command)
				} else {
					DPrintf("shardkv pullconfig err: {Server=%d, group=%d} config=%+v, predicted config num=%d",
						kv.me, kv.gid, config, num+1)
				}
			} else {
				kv.mu.Unlock()
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func (kv *ShardKV) needInsertEmptyLog() bool {
	lastTerm := kv.rf.LastTerm()
	currentTerm, _ := kv.rf.GetState()
	return currentTerm > lastTerm
}
func (kv *ShardKV) insertEmptyLog() {
	for !kv.killed() {
		if _, isLeader := kv.rf.GetState(); isLeader {
			if kv.needInsertEmptyLog() {
				DPrintf("shardkv executeEmptyCommand started: {Server=%d, group=%d} needApply=%v", kv.me, kv.gid, kv.rf.NeedApply())
				op := Op{}
				op.Type = EMPTYOP
				commonOp := CommonOp{}
				commonOp.Command = op
				commonOp.Type = Operations
				kv.Execute(commonOp)
				DPrintf("shardkv executeEmptyCommand finished: {Server=%d, group=%d} needApply=%v", kv.me, kv.gid, kv.rf.NeedApply())
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

//
// 问题：push操作将会重复执行直到对应shard被删除掉
//
func (kv *ShardKV) needPush() bool {
	for _, storage := range kv.storage {
		if storage.Status == Pushing {
			return true
		}
	}
	return false
}

//
// 将要执行push操作的group
//
func (kv *ShardKV) g2sWithPush() map[int][]int {
	g2s := make(map[int][]int)
	for shard, data := range kv.storage {
		if data.Status == Pushing {
			gid := kv.currentConfig.Shards[shard]
			g2s[gid] = append(g2s[gid], shard)
		}
	}
	return g2s
}

func (kv *ShardKV) genPushShardArgs(shardIDs []int) PushShardArgs {
	args := PushShardArgs{}
	args.ConfigNum = kv.currentConfig.Num
	args.Data = make(map[int]ShardKVStorage)
	for _, shard := range shardIDs {
		args.Data[shard] = *(kv.storage[shard].DeepCopy())
	}
	args.ClientLastApplied = make(map[int64]IdAndResponse)
	for key, value := range kv.clientLastApplied {
		args.ClientLastApplied[key] = value
	}
	return args
}

// 客户端操作：调用`PushShardService` rpc将shard push到其他group中
func (kv *ShardKV) pushShard() {
	// TODO：如果需要push才执行push，没必要实时检测
	for !kv.killed() {
		if _, isLeader := kv.rf.GetState(); isLeader {
			kv.mu.Lock()
			if kv.needPush() {
				g2s := kv.g2sWithPush()
				// 调用PushShardService rpc将shard push到远端
				var wg sync.WaitGroup
				for gid, shardIDs := range g2s {
					// 初始化参数
					// 注意：需要深拷贝否则可能发生数据争用
					args := kv.genPushShardArgs(shardIDs)
					var servers []string
					var ok bool
					// leave后使用currentConfig可能找不到对应的group, join后使用lastConfig也可能找不到对应的group
					servers, ok = kv.lastConfig.Groups[gid]
					if !ok {
						servers = kv.currentConfig.Groups[gid]
					}
					DPrintf("shardkv pushshard started: {Server=%d, group=%d} starts push shards=%v to group=%d(servers=%+v) with config=%+v",
						kv.me, kv.gid, shardIDs, gid, servers, kv.currentConfig)
					wg.Add(1)
					go func(servers []string, args PushShardArgs) {
						defer wg.Done()
						for _, server := range servers {
							var reply PushShardReply
							srv := kv.make_end(server)
							if srv.Call("ShardKV.PushShardService", &args, &reply) && reply.Err == OK {
								DPrintf("shardkv pushshard finished: {Server=%d, group=%d} finishs push with args=%+v",
									kv.me, kv.gid, args)
								// 问题：能否在将shard push到其他group之后执行delete操作？
								// 不能，因为在调用start方法之前leader可能变为follower，那样就会有部分shards永远不会被delete
								// 解决办法：启动一个协程用于检测shard的状态，当shard从pulling变为serving，则通知之前的group执行delete操作。

								// 已经push到远程的shard状态应该改变，否则会一直执行push操作
								commonOp := CommonOp{}
								commonOp.Command = args
								commonOp.Type = CreateShard
								// 将Pushing状态的shard更新为Gcing
								kv.Execute(commonOp)
							} else {
								DPrintf("shardkv pushshard err=%s: {Server=%d, group=%d} push with args=%+v",
									reply.Err, kv.me, kv.gid, args)
							}
						}
					}(servers, args)
				}
				kv.mu.Unlock()
				wg.Wait()
			} else {
				kv.mu.Unlock()
			}
		}
		time.Sleep(35 * time.Millisecond)
	}
}

// 如果存在lastConfig中不存在的shard，且shard状态为NotifyDeleting，则需要通知其他group执行delete操作
func (kv *ShardKV) needDelete() bool {
	for shard, storage := range kv.storage {
		if storage.Status == NotifyDeleting && kv.lastConfig.Num != 0 &&
			kv.lastConfig.Shards[shard] != kv.gid {
			return true
		}
	}
	return false
}

//
// 将要执行delete操作的group
//
func (kv *ShardKV) g2sWithDelete() map[int][]int {
	g2s := make(map[int][]int)
	for shard, data := range kv.storage {
		if data.Status == NotifyDeleting && kv.lastConfig.Shards[shard] != kv.gid {
			gid := kv.lastConfig.Shards[shard]
			g2s[gid] = append(g2s[gid], shard)
		}
	}
	return g2s
}

func (kv *ShardKV) genDeleteShardArgs(shardIDs []int) DeleteShardArgs {
	args := DeleteShardArgs{}
	args.ConfigNum = kv.currentConfig.Num
	args.ShardIds = make([]int, len(shardIDs))
	for i, shard := range shardIDs {
		args.ShardIds[i] = shard
	}
	return args
}

// 客户端操作：调用`DeleteShardService` rpc通知其他group可以删除的shards
func (kv *ShardKV) deleteShard() {
	for !kv.killed() {
		if _, isLeader := kv.rf.GetState(); isLeader {
			kv.mu.Lock()
			if kv.needDelete() {
				g2s := kv.g2sWithDelete()
				// 调用DeleteShardService rpc将shard push到远端
				var wg sync.WaitGroup
				for gid, shardIDs := range g2s {
					// 初始化参数
					// 注意：需要深拷贝否则可能发生数据争用
					args := kv.genDeleteShardArgs(shardIDs)
					var servers []string
					var ok bool
					// leave后使用currentConfig可能找不到对应的group, join后使用lastConfig也可能找不到对应的group
					servers, ok = kv.lastConfig.Groups[gid]
					if !ok {
						servers = kv.currentConfig.Groups[gid]
					}
					DPrintf("shardkv deleteshard started: {Server=%d, group=%d} starts delete rpc shards=%v in group=%d(servers=%+v) with config=%+v",
						kv.me, kv.gid, shardIDs, gid, servers, kv.currentConfig)
					wg.Add(1)
					go func(servers []string, args DeleteShardArgs) {
						defer wg.Done()
						for _, server := range servers {
							var reply DeleteShardReply
							srv := kv.make_end(server)
							if srv.Call("ShardKV.DeleteShardService", &args, &reply) && reply.Err == OK {
								DPrintf("shardkv deleteshard finished: {Server=%d, group=%d} finishs delete shards with args=%+v",
									kv.me, kv.gid, args)
								// 将状态为NotifyDeleting的shard更新为Serving
								commonOp := CommonOp{}
								commonOp.Command = args
								commonOp.Type = DeleteShard
								kv.Execute(commonOp)
							} else {
								DPrintf("shardkv deleteshard err=%s: {Server=%d, group=%d} delete with args=%+v",
									reply.Err, kv.me, kv.gid, args)
							}
						}
					}(servers, args)
				}
				kv.mu.Unlock()
				wg.Wait()
			} else {
				kv.mu.Unlock()
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

//
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *ShardKV) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *ShardKV) readSnapshot(snapshot []byte) {
	if snapshot == nil || len(snapshot) < 1 {
		return
	}
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	storage := make(map[int]*ShardKVStorage)
	clientMap := make(map[int64]IdAndResponse)
	var lastConfig shardctrler.Config
	var currentConfig shardctrler.Config
	if d.Decode(&storage) != nil || d.Decode(&clientMap) != nil ||
		d.Decode(&lastConfig) != nil || d.Decode(&currentConfig) != nil {
		log.Fatalf("shardkv readsnapshot: ReadSnapshot Decode error")
	} else {
		kv.mu.Lock()
		kv.storage = storage
		kv.clientLastApplied = clientMap
		kv.lastConfig = lastConfig
		kv.currentConfig = currentConfig
		DPrintf("shardkv readsnapshot: {Server=%d, group=%d} read sanpshot=%+v, clientLastApplied=%+v, currentConfig=%+v",
			kv.me, kv.gid, kv.storage, kv.clientLastApplied, kv.currentConfig)
		kv.mu.Unlock()
	}
}

func (kv *ShardKV) condInstallSnapshot(m raft.ApplyMsg) {
	// 处理快照
	DPrintf("shardkv condInstallSnapshot: Server=%d CondInstallsnapshot, lastIncludedIndex=%d, lastIncludeTerm=%d, lastApplied=%d",
		kv.me, m.SnapshotIndex, m.SnapshotTerm, kv.lastApplied)
	if kv.rf.CondInstallSnapshot(m.SnapshotTerm,
		m.SnapshotIndex, m.Snapshot) {
		kv.lastApplied = m.SnapshotIndex
		if m.Snapshot == nil || len(m.Snapshot) < 1 {
			log.Fatalf("shardkv condInstallSnapshot: CondInstallsnapshot read nil snapshot")
			return
		}
		kv.readSnapshot(m.Snapshot)
	} else {
		DPrintf("shardkv condInstallSnapshot: Server=%d Installsnapshot: get old snapshot", kv.me)
	}
}

func (kv *ShardKV) snapshot(m raft.ApplyMsg) {
	DPrintf("shardkv snapshot: {Server=%d, group=%d} Start Install Snapshot, raft size=%d, maxraftstate=%d, lastIncudedIndex=%d",
		kv.me, kv.gid, kv.persister.RaftStateSize(), kv.maxraftstate, m.CommandIndex)
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	// encode：或者使用大写字段的结构体，或者将其复制到另外字段
	v := kv.storage
	clientLastApplied := kv.clientLastApplied
	lastConfig := kv.lastConfig
	currentConfig := kv.currentConfig
	e.Encode(v)
	e.Encode(clientLastApplied)
	e.Encode(lastConfig)
	e.Encode(currentConfig)
	kv.rf.Snapshot(m.CommandIndex, w.Bytes())
	DPrintf("shardkv snapshot: {Server=%d, group=%d} Finish Install Snapshot, snapshot size=%d, raft size=%d, lastIncudedIndex=%d, clientLastAplied=%+v",
		kv.me, kv.gid, len(w.Bytes()), kv.persister.RaftStateSize(), m.CommandIndex, kv.clientLastApplied)
}

func (kv *ShardKV) executeCommandWithoutLock(op Op) ApplyResult {
	shard := key2shard(op.Key)
	result := ApplyResult{}
	switch op.Type {
	case GET:
		value, ok := kv.storage[shard].Get(op.Key)
		if ok {
			result.Err = OK
			result.Value = value
		} else {
			result.Err = ErrNoKey
		}
	case APPEND:
		kv.storage[shard].Append(op.Key, op.Value)
		result.Err = OK
	case PUT:
		kv.storage[shard].Put(op.Key, op.Value)
		result.Err = OK
	}
	return result
}

// 将get, put, append操作应用到状态机
// 注意：在执行阶段config可能会发生变化
// 注意：重复的Put或者Append命令直接返回结果，重复的命令可以写入到日志中，但是不能让重复的命令去改变状态机
func (kv *ShardKV) applyOpeartion(m raft.ApplyMsg) ApplyResult {
	commonOp := m.Command.(CommonOp)
	op := commonOp.Command.(Op)
	kv.mu.Lock()
	defer kv.mu.Unlock()
	kv.lastApplied = m.CommandIndex
	var result ApplyResult
	if op.Type == EMPTYOP {
		result.Err = OK
		DPrintf("shardkv apply empty operations: {Server=%d, group=%d} apply {commandIndex=%d, command=%+v}",
			kv.me, kv.gid, m.CommandIndex, m.Command)
		return result
	}
	if !kv.inCurrentGroup(op.Key) {
		result.Err = ErrWrongGroup
	} else if !kv.isServing(op.Key) {
		// shard处于非Serving状态的请求直接拒绝
		result.Err = ErrNotServing
	} else if op.Type != GET && kv.duplicateCommandWithoutLock(op) {
		DPrintf("shardkv apply operations duplicateCommand: {Server=%d, group=%d} process duplicate command=%+v, clientLastAplied: %v",
			kv.me, kv.gid, op, kv.clientLastApplied)
		result.Err = OK
	} else {
		DPrintf("shardkv apply operations started: {Server=%d, group=%d} apply {commandIndex=%d, lastApplied=%d, command=%+v} {key=%s->value=%s}",
			kv.me, kv.gid, m.CommandIndex, kv.lastApplied, m.Command, op.Key, kv.storage[key2shard(op.Key)].Data[op.Key])
		// 执行对应命令, 并将结果写入commandResult
		result = kv.executeCommandWithoutLock(op)
		// read命令可以重复执行
		if op.Type != GET {
			kv.clientLastApplied[op.ClientId] = IdAndResponse{op.Id, string(result.Err)}
		}
		DPrintf("shardkv apply operations finished: {Server=%d, group=%d} apply {commandIndex=%d, command=%+v} result={key=%s->value=%s}",
			kv.me, kv.gid, m.CommandIndex, m.Command, op.Key, kv.storage[key2shard(op.Key)].Data[op.Key])
	}
	return result
}

func (kv *ShardKV) applyShard(m raft.ApplyMsg) ApplyResult {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	commonOp := m.Command.(CommonOp)
	pushArgs := commonOp.Command.(PushShardArgs)
	var result ApplyResult
	if pushArgs.ConfigNum != kv.currentConfig.Num {
		result.Err = ErrWrongConfigNum
		DPrintf("shardkv applyInsertShard ErrWrongConfigNum: {Server=%d, group=%d} insert shard err with wrongConfigNum op num=%d, current num=%d",
			kv.me, kv.gid, pushArgs.ConfigNum, kv.currentConfig.Num)
	} else {
		DPrintf("shardkv applyInsertShard started: {Server=%d, group=%d} insert data=%+v to storage=%+v with currentconfig=%+v, clientLastApplied=%+v",
			kv.me, kv.gid, pushArgs, kv.storage, kv.currentConfig, kv.clientLastApplied)
		// 已经存在该shard且处于Pulling状态才执行apply操作。
		for shard, storage1 := range pushArgs.Data {
			storage2, ok := kv.storage[shard]
			// 被push的group将对应shard状态更新为NotifyDeleting
			if ok && storage2.Status == Pulling {
				if len(storage2.Data) != 0 {
					log.Fatalf("shardkv applyInsertShard fatal: {Server=%d, group=%d} storage=%+v is not null",
						kv.me, kv.gid, storage2)
				}
				for k, v := range storage1.Data {
					storage2.Put(k, v)
				}
				storage2.Status = NotifyDeleting
			} else if ok && storage2.Status == Pushing { // 执行push的group将对应shard状态更新为Gcing
				storage2.Status = Gcing
			}
		}
		// 更新clientLastApplied
		for client, data := range pushArgs.ClientLastApplied {
			localData, ok := kv.clientLastApplied[client]
			if (ok && data.CommandId > localData.CommandId) || !ok {
				kv.clientLastApplied[client] = data
			}
		}
		result.Err = OK
		DPrintf("shardkv applyInsertShard finished: {Server=%d, group=%d} storage=%+v, clientLastApplied=%+v, currentconfig=%+v",
			kv.me, kv.gid, kv.storage, kv.clientLastApplied, kv.currentConfig)
	}
	return result
}

// 获取当前group中的所有shards
func (kv *ShardKV) getShardsInGroup(config shardctrler.Config) map[int]int {
	res := make(map[int]int, 0)
	for shard, gid := range config.Shards {
		if gid == kv.gid {
			res[shard] = 0
		}
	}
	return res
}

func (kv *ShardKV) shardsPull(currentShards map[int]int, lastShards map[int]int) {
	var ok bool
	for shard := range currentShards {
		_, ok = lastShards[shard]
		if !ok {
			kv.storage[shard] = MakeStorage()
			// 最开始的Shard状态初始化为Serving
			if kv.lastConfig.Num == 0 {
				kv.storage[shard].Status = Serving
			} else {
				kv.storage[shard].Status = Pulling
			}
		}
	}
}

func (kv *ShardKV) shardsPush(currentShards map[int]int, lastShards map[int]int) {
	var ok bool
	for shard, _ := range lastShards {
		_, ok = currentShards[shard]
		if !ok {
			kv.storage[shard].Status = Pushing
		}
	}
}

// 将更新的config应用到状态机
func (kv *ShardKV) applyConfig(m raft.ApplyMsg) ApplyResult {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	commonOp := m.Command.(CommonOp)
	config := commonOp.Command.(shardctrler.Config)
	var result ApplyResult
	if config.Num == kv.currentConfig.Num+1 {
		DPrintf("shardkv applyconfig started: {Server=%d, group=%d} storage=%+v",
			kv.me, kv.gid, kv.storage)
		// 更新currentConfig
		kv.lastConfig = kv.currentConfig
		kv.currentConfig = config
		// 上一轮config中当前group所包含的所有shards
		lastShards := kv.getShardsInGroup(kv.lastConfig)
		// 当前config中所包含的shards
		currentShards := kv.getShardsInGroup(kv.currentConfig)
		// 将要迁移到其他group的shard：状态改为Pushing
		kv.shardsPush(currentShards, lastShards)
		// 新增的shard：状态为Pulling
		kv.shardsPull(currentShards, lastShards)
		DPrintf("shardkv applyconfig finished: {Server=%d, group=%d} lastConfig=%+v, currentconfig=%+v, storage=%+v",
			kv.me, kv.gid, kv.lastConfig, kv.currentConfig, kv.storage)
		result.Err = OK
	} else {
		result.Err = ErrWrongConfigNum
		DPrintf("shardkv applyconfig ErrWrongConfigNum: {Server=%d, group=%d} current config num=%d, apply config num=%d",
			kv.me, kv.gid, kv.currentConfig.Num, config.Num)
	}
	return result
}

// 删除不需要的shards
// 将状态为Gcing的shard直接删除，状态为NotifyDeleting的shard的状态改为Serving
func (kv *ShardKV) applyDeleteShards(m raft.ApplyMsg) ApplyResult {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	commonOp := m.Command.(CommonOp)
	op := commonOp.Command.(DeleteShardArgs)
	var result ApplyResult
	if op.ConfigNum == kv.currentConfig.Num {
		DPrintf("shardkv applyDelete started: {Server=%d, group=%d} delete shards=%+v from storage=%+v",
			kv.me, kv.gid, op.ShardIds, kv.storage)
		for _, shard := range op.ShardIds {
			data, ok := kv.storage[shard]
			if ok && data.Status == Gcing {
				delete(kv.storage, shard)
			} else if ok && data.Status == NotifyDeleting {
				data.Status = Serving
			}
		}
		result.Err = OK
		DPrintf("shardkv applyDelete finished: {Server=%d, group=%d} storage=%+v, config=%+v",
			kv.me, kv.gid, kv.storage, kv.currentConfig)
	} else {
		result.Err = ErrWrongConfigNum
		DPrintf("shardkv applyDelete ErrWrongConfigNum: {Server=%d, group=%d} delete shards=%+v err with wrongConfigNum op num=%d, current num=%d",
			kv.me, kv.gid, op.ShardIds, op.ConfigNum, kv.currentConfig.Num)
	}
	return result
}

// 应用到状态机
func (kv *ShardKV) applier() {
	for m := range kv.applyCh {
		// 处理快照
		if m.SnapshotValid {
			kv.condInstallSnapshot(m)
		} else if m.CommandValid {
			// interface转换为通用的操作：CommonOp
			var result ApplyResult
			commonOp := m.Command.(CommonOp)
			switch commonOp.Type {
			case Operations:
				result = kv.applyOpeartion(m)
			case UpdateConfig:
				result = kv.applyConfig(m)
			case CreateShard:
				result = kv.applyShard(m)
			case DeleteShard:
				result = kv.applyDeleteShards(m)
			}
			ch, ok := kv.getResultChan(m.CommandIndex)
			// 问题：为什么只有leader可以返回结果？
			// 因为leader转为follower后，index对应的命令可能发生了变化，也就是返回的结果不是最初的那条命令的结果
			// TODO: 确保任期没有变化
			if _, isLeader := kv.rf.GetState(); isLeader && ok {
				ch <- result
			}
			// 快照包括当前复制状态机的数据
			if kv.maxraftstate != -1 && kv.persister.RaftStateSize() > kv.maxraftstate {
				kv.snapshot(m)
			}
		} else {
			DPrintf("shardkv applier: Server=%d Ignore log or snapshot: lastApplied=%d, applyCh=%+v", kv.me, kv.lastApplied, m)
		}
	}
	DPrintf("shardkv applier: Server=%d applier finished", kv.me)
}

//
// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	// TODO: 问题：命令能否使用指针，指针又是如何持久化的呢？？
	labgob.Register(Op{})
	labgob.Register(CommonOp{})
	labgob.Register(shardctrler.Config{})
	labgob.Register(PushShardArgs{})
	labgob.Register(DeleteShardArgs{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers

	// Your initialization code here.

	// Use something like this to talk to the shardctrler:
	kv.ctrCk = shardctrler.MakeClerk(kv.ctrlers)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.persister = persister
	kv.storage = make(map[int]*ShardKVStorage)
	kv.lastApplied = 0
	kv.clientLastApplied = make(map[int64]IdAndResponse)
	kv.resultChan = make(map[int]chan ApplyResult)
	kv.currentConfig = shardctrler.Config{}
	kv.lastConfig = shardctrler.Config{}
	kv.readSnapshot(persister.ReadSnapshot())
	DPrintf("shardkv startServer: {Server=%d, group=%d} startServer with storage=%+v, clientLastApplied=%+v, currentConfig=%+v, lastConfig=%+v",
		kv.me, kv.gid, kv.storage, kv.clientLastApplied, kv.currentConfig, kv.lastConfig)
	DPrintf("shardkv startServer: {Server=%d, group=%d} startServer with logs=%+v", kv.me, kv.gid, kv.rf.LogEntries)
	go kv.applier()
	go kv.pullConfig()
	go kv.pushShard()
	go kv.deleteShard()
	go kv.insertEmptyLog()
	return kv
}
