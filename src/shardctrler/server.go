package shardctrler

import (
	"log"
	"sort"
	"sync"
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
	JOIN  OpType = 0
	LEAVE OpType = 1
	MOVE  OpType = 2
	QUERY OpType = 3
)

type IdAndResponse struct {
	CommandId int64
	Response  Err
}

type RpcResult struct {
	WrongLeader bool
	Err         Err
	Config      Config
}

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.

	configs []Config // indexed by config num

	// 记录已经被应用到状态机中的命令id，防止重复执行同一条命令
	lastApplied int
	persister   *raft.Persister
	// Join Leave Move不能重复执行
	// 记录每个客户端最后一次应用的命令id及其响应，防止同一个客户端重复执行同一条命令
	clientLastApplied map[int64]IdAndResponse
	resultChan        map[int]chan RpcResult
}

type Op struct {
	// Your data here.
	// 命令类型
	Type OpType
	// Join
	Servers map[int][]string
	// Leave
	GIDs []int
	// Move
	Shard int
	GID   int
	// Query
	Num int
	// 命令ID
	Id int64
	// 客户端ID
	ClientId int64
}

func (sc *ShardCtrler) duplicateCommandWithoutLock(op Op) bool {
	value, ok := sc.clientLastApplied[op.ClientId]
	// 命令已经被执行过
	if ok && value.CommandId >= op.Id {
		return true
	}
	return false
}

func (sc *ShardCtrler) removeResultChan(index int) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	delete(sc.resultChan, index)
}

func (sc *ShardCtrler) getResultChanWithoutLock(index int) (chan RpcResult, bool) {
	ch, ok := sc.resultChan[index]
	return ch, ok
}

func (sc *ShardCtrler) start(op Op) RpcResult {
	result := RpcResult{}
	sc.mu.Lock()
	if op.Type != QUERY && sc.duplicateCommandWithoutLock(op) {
		DPrintf("shardctrler: Server=%d process duplicate command=%+v, clientLastAplied: %v",
			sc.me, op, sc.clientLastApplied)
		result.WrongLeader = false
		result.Err = OK
		sc.mu.Unlock()
		return result
	}
	index, oldTerm, isLeader := sc.rf.Start(op)
	if !isLeader {
		result.WrongLeader = true
		result.Err = ErrWrongLeader
		sc.mu.Unlock()
		return result
	}
	result.WrongLeader = false
	DPrintf("shardctrler: Server=%d(raft term=%d) start with {op=%+v, command index=%d}",
		sc.me, oldTerm, op, index)
	sc.resultChan[index] = make(chan RpcResult, 1)
	ch := sc.resultChan[index]
	sc.mu.Unlock()
	select {
	case data := <-ch:
		DPrintf("shardctrler: Server=%d(raft Term=%d) start(index=%d) result=%+v",
			sc.me, oldTerm, index, data)
		result.Err = data.Err
		result.Config = data.Config
	case <-time.After(ExecuteTimeout):
		DPrintf("shardctrler: Server=%d(raft Term=%d) start(index=%d) Timeout",
			sc.me, oldTerm, index)
		result.Err = ErrTimeout
	}
	sc.removeResultChan(index)
	return result
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	// Join和Leave之后如何重新分配shard?
	// 从拥有shard最多的group中分配shard给拥有shard最少的group
	op := Op{}
	op.ClientId = args.ClientId
	op.Id = args.Id
	op.Servers = args.Servers
	op.Type = JOIN
	result := sc.start(op)
	reply.Err = result.Err
	reply.WrongLeader = result.WrongLeader
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	op := Op{}
	op.ClientId = args.ClientId
	op.Id = args.Id
	op.GIDs = args.GIDs
	op.Type = LEAVE
	result := sc.start(op)
	reply.Err = result.Err
	reply.WrongLeader = result.WrongLeader
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	op := Op{}
	op.ClientId = args.ClientId
	op.Id = args.Id
	op.Shard = args.Shard
	op.GID = args.GID
	op.Type = MOVE
	result := sc.start(op)
	reply.Err = result.Err
	reply.WrongLeader = result.WrongLeader
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	op := Op{}
	op.ClientId = args.ClientId
	op.Id = args.Id
	op.Num = args.Num
	op.Type = QUERY
	result := sc.start(op)
	reply.Err = result.Err
	reply.WrongLeader = result.WrongLeader
	reply.Config = result.Config
}

//
// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

func DeepCopy(config *Config) Config {
	config_copy := Config{}
	config_copy.Num = config.Num
	copy(config_copy.Shards[:], config.Shards[:])
	config_copy.Groups = make(map[int][]string)
	for key, value := range config.Groups {
		config_copy.Groups[key] = make([]string, len(value))
		copy(config_copy.Groups[key], value)
	}
	return config_copy
}

func (sc *ShardCtrler) group2Shard() map[int][]int {
	config := sc.configs[len(sc.configs)-1]
	g2s := make(map[int][]int)
	for key := range config.Groups {
		g2s[key] = make([]int, 0, NShards)
	}
	// 如果config.Groups的key的数量不为空
	if len(config.Groups) != 0 {
		for shard, gid := range config.Shards {
			g2s[gid] = append(g2s[gid], shard)
		}
	}
	return g2s
}

func (sc *ShardCtrler) rebalance(g2s map[int][]int) map[int][]int {
	for {
		max, min := sc.findMaxAndMinGroup(g2s)
		maxShards := len(g2s[max])
		minShards := len(g2s[min])
		if maxShards-minShards > 1 {
			g2s[min] = append(g2s[min], g2s[max][maxShards-1])
			g2s[max] = g2s[max][0 : maxShards-1]
		}
		if maxShards-minShards <= 1 {
			break
		}
	}
	return g2s
}

// 找出拥有最多和最少shard的group
func (sc *ShardCtrler) findMaxAndMinGroup(g2s map[int][]int) (int, int) {
	max := -1
	min := NShards
	maxShards := -1
	minShards := NShards + 1
	// map的遍历是无序的，每次遍历结果都不一样
	allGids := make([]int, 0)
	for gid := range g2s {
		allGids = append(allGids, gid)
	}
	// 排序以确保对g2s的遍历的唯一性
	sort.Ints(allGids)
	for _, gid := range allGids {
		shards := g2s[gid]
		if maxShards < len(shards) {
			maxShards = len(shards)
			max = gid
		}
		if minShards > len(shards) {
			minShards = len(shards)
			min = gid
		}
	}
	return max, min
}

//
// 首先根据shards数组获取一个从group映射到shards的map(gid-->shards)
// 遍历g2s找出拥有最多和最少shards的group, 然后将一个shard从最多的group中移动最少的group中，重复执行，
// 直到最大值和最小值的差值小于等于1
// 注意：go中对map的遍历每次结果都不一样，为了保证结果一致，需要先获取key，然后对key排序，然后通过遍历这个有序的key数组来遍历map.
//
func (sc *ShardCtrler) processJoin(op Op) {
	if len(op.Servers) == 0 {
		return
	}
	g2s := sc.group2Shard()
	for key := range op.Servers {
		g2s[key] = make([]int, 0, NShards)
	}
	// 如果最后一条配置的group为空则需要将shard添加到一个group中才能执行reblance
	if len(sc.configs[len(sc.configs)-1].Groups) == 0 {
		gids := make([]int, 0)
		for gid := range op.Servers {
			gids = append(gids, gid)
		}
		sort.Ints(gids)
		config := sc.configs[len(sc.configs)-1]
		for shard := range config.Shards {
			g2s[gids[0]] = append(g2s[gids[0]], shard)
		}
	}
	DPrintf("shardctrler: Server=%d start process Join with {op=%+v, g2s=%+v, config=%+v}",
		sc.me, op, g2s, sc.configs[len(sc.configs)-1])
	g2s = sc.rebalance(g2s)
	DPrintf("shardctrler: Server=%d process Join after balance g2s=%+v", sc.me, g2s)
	// 添加一条config并修改对应的Num, Shards, Group数据
	config := DeepCopy(&sc.configs[len(sc.configs)-1])
	config.Num = config.Num + 1
	// 更新Group
	for key, value := range op.Servers {
		config.Groups[key] = make([]string, 0, len(value))
		config.Groups[key] = append(config.Groups[key], value...)
	}
	// 更新Shards
	for gid, shards := range g2s {
		for _, shard := range shards {
			config.Shards[shard] = gid
		}
	}
	sc.configs = append(sc.configs, config)
	DPrintf("shardctrler: Server=%d finish process Join config=%+v",
		sc.me, sc.configs[len(sc.configs)-1])
}

func contains(gids []int, g int) bool {
	for _, value := range gids {
		if value == g {
			return true
		}
	}
	return false
}

//
// 将gidsLeave中的group所包含的shards移动到现有的group中
//
func (sc *ShardCtrler) leave(g2s map[int][]int, gidsLeave []int) map[int][]int {
	// leave之后的所有group
	gids := make([]int, 0)
	for key := range g2s {
		if !contains(gidsLeave, key) {
			gids = append(gids, key)
		}
	}
	// 排序以确保结果唯一性
	sort.Ints(gids)
	idx0 := 0
	// 将gidsLeave对应的shard移动到gids的group中
	for idx1 := 0; idx1 < len(gidsLeave); idx1++ {
		var g1 int
		var g2 int
		// 如果将所有group都leave
		if len(gids) == 0 {
			g1, g2 = 0, gidsLeave[idx1]
		} else {
			g1, g2 = gids[idx0], gidsLeave[idx1]
		}
		g2s[g1] = append(g2s[g1], g2s[g2]...)
		idx0++
		if idx0 >= len(gids) {
			idx0 = 0
		}
	}
	// 删除g2s中gidsLeave对应的条目
	for _, gid := range gidsLeave {
		delete(g2s, gid)
	}
	return g2s
}

func (sc *ShardCtrler) processLeave(op Op) {
	if len(op.GIDs) == 0 {
		return
	}
	g2s := sc.group2Shard()
	DPrintf("shardctrler: Server=%d start process Leave with {op=%+v, g2s=%+v, config=%+v}",
		sc.me, op, g2s, sc.configs[len(sc.configs)-1])
	// leave
	g2s = sc.leave(g2s, op.GIDs)
	g2s = sc.rebalance(g2s)
	DPrintf("shardctrler: Server=%d process Leave after balance g2s=%+v", sc.me, g2s)
	// 添加一条config并修改对应的Num, Shards, Group数据
	config := DeepCopy(&sc.configs[len(sc.configs)-1])
	config.Num = config.Num + 1
	// 删除Group
	for _, gid := range op.GIDs {
		delete(config.Groups, gid)
	}
	// 更新Shards
	for gid, shards := range g2s {
		for _, shard := range shards {
			config.Shards[shard] = gid
		}
	}
	sc.configs = append(sc.configs, config)
	DPrintf("shardctrler: Server=%d finish process Leave config=%+v",
		sc.me, sc.configs[len(sc.configs)-1])
}

func (sc *ShardCtrler) processMove(op Op) {
	DPrintf("shardctrler: Server=%d start process move with {op=%+v, config=%+v}",
		sc.me, op, sc.configs[len(sc.configs)-1])
	// 添加一条config并修改对应的Num, Shards, Group数据
	config := DeepCopy(&sc.configs[len(sc.configs)-1])
	config.Num = config.Num + 1
	// 更新shards
	config.Shards[op.Shard] = op.GID
	sc.configs = append(sc.configs, config)
	DPrintf("shardctrler: Server=%d finish process move config=%+v",
		sc.me, sc.configs[len(sc.configs)-1])
}

func (sc *ShardCtrler) processQuery(op Op) Config {
	DPrintf("shardctrler: Server=%d start process Query with {op=%+v, config=%+v}",
		sc.me, op, sc.configs[len(sc.configs)-1])
	if op.Num == -1 {
		return sc.configs[len(sc.configs)-1]
	}
	for _, config := range sc.configs {
		if op.Num == config.Num {
			return config
		}
	}
	config := Config{}
	DPrintf("shardctrler: Server=%d cant find config of Num=%d",
		sc.me, op.Num)
	return config
}

func (sc *ShardCtrler) executeCommandWithoutLock(op Op) RpcResult {
	var result RpcResult
	// TODO: 执行命令出错怎么办？
	// go并没有提供try-catch，不能因为用户的一个错误命令导致系统崩溃
	switch op.Type {
	case QUERY:
		config := sc.processQuery(op)
		result.Config = config
	case JOIN:
		sc.processJoin(op)
	case MOVE:
		sc.processMove(op)
	case LEAVE:
		sc.processLeave(op)
	}
	result.Err = OK
	return result
}

// 应用到状态机
func (sc *ShardCtrler) applier() {
	for m := range sc.applyCh {
		if m.CommandValid {
			// interface转换为struct
			op := m.Command.(Op)
			sc.mu.Lock()
			// 需要保证`m.CommandIndex`处的命令没有发生变化
			ch, ok := sc.getResultChanWithoutLock(m.CommandIndex)
			sc.lastApplied = m.CommandIndex
			// 重复的非Query命令直接返回结果
			// 重复的命令可以写入到日志中，但是不能让重复的命令去改变状态机
			var result RpcResult
			if op.Type != QUERY && sc.duplicateCommandWithoutLock(op) {
				DPrintf("shardctrler: Server=%d process duplicate command=%+v, clientLastAplied: %v",
					sc.me, op, sc.clientLastApplied)
				result.Err = OK
			} else {
				// 执行对应命令, 并将结果写入commandResult
				result = sc.executeCommandWithoutLock(op)
				// read命令可以重复执行
				if op.Type != QUERY {
					sc.clientLastApplied[op.ClientId] = IdAndResponse{op.Id, result.Err}
				}
				DPrintf("shardctrler: Server=%d apply {commandIndex=%d, lastApplied=%d, command=%+v}", sc.me, m.CommandIndex, sc.lastApplied, m.Command)
			}
			sc.mu.Unlock()
			// 为什么只有leader可以返回结果？
			// 因为leader转为follower后，index对应的命令可能发生了变化，也就是返回的结果不是最初的那条命令的结果
			// TODO: 确保任期没有变化
			if _, isLeader := sc.rf.GetState(); isLeader && ok {
				ch <- result
			}
		} else {
			DPrintf("shardctrler: Server=%d Ignore log or snapshot: lastApplied=%d, applyCh=%+v", sc.me, sc.lastApplied, m)
		}
	}

	DPrintf("shardctrler: Server=%d applier finished", sc.me)
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.
	sc.lastApplied = 0
	sc.clientLastApplied = make(map[int64]IdAndResponse)
	sc.resultChan = make(map[int]chan RpcResult)
	go sc.applier()
	return sc
}
