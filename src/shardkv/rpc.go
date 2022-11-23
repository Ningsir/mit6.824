package shardkv

import (
	"log"
	"time"
)

// start: get, put, append三种操作
// 问题：调用`rf.Start`之后config发生变化如何处理？
// config更新也需要使用raft进行同步，在apply的时候根据新的config判断是否需要处理这条命令即可
func (kv *ShardKV) startOperation(op Op) ApplyResult {
	result := ApplyResult{}
	if _, isLeader := kv.rf.GetState(); !isLeader {
		result.Err = ErrWrongLeader
		DPrintf("shardkv operation ErrWrongLeader: {Server=%d, group=%d} op=%+v",
			kv.me, kv.gid, op)
		return result
	}
	DPrintf("shardkv start0: {Server=%d, group=%d} op=%+v", kv.me, kv.gid, op)
	kv.mu.Lock()
	DPrintf("shardkv start1: {Server=%d, group=%d} op=%+v", kv.me, kv.gid, op)
	// 不属于该group的请求直接拒绝
	if !kv.inCurrentGroup(op.Key) {
		result.Err = ErrWrongGroup
		DPrintf("shardkv operation ErrWrongGroup: {Server=%d, group=%d} start op=%+v(shard=%d) to wrong group with config=%+v",
			kv.me, kv.gid, op, key2shard(op.Key), kv.currentConfig)
		kv.mu.Unlock()
		return result
	}
	// shard处于非Serving状态的请求直接拒绝
	if !kv.isServing(op.Key) {
		result.Err = ErrNotServing
		DPrintf("shardkv operation ErrNotServing: {Server=%d, group=%d} start {op=%+v shard=%d} not serving with {config=%+v, storage=%+v}",
			kv.me, kv.gid, op, key2shard(op.Key), kv.currentConfig, kv.storage)
		kv.mu.Unlock()
		return result
	}
	if op.Type != GET && kv.duplicateCommandWithoutLock(op) {
		DPrintf("shardkv operation duplicateCommand: {Server=%d, group=%d} process duplicate command=%+v, clientLastAplied=%+v",
			kv.me, kv.gid, op, kv.clientLastApplied)
		result.Err = OK
		kv.mu.Unlock()
		return result
	}
	kv.mu.Unlock()
	commonOp := CommonOp{}
	commonOp.Command = op
	commonOp.Type = Operations

	res := kv.Execute(commonOp)
	return res
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	op := Op{}
	op.Key = args.Key
	op.Type = GET
	result := kv.startOperation(op)
	reply.Err = result.Err
	reply.Value = result.Value
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	var op Op
	if args.Op == "Put" {
		op = Op{PUT, args.Key, args.Value, args.Id, args.ClientId}
	} else if args.Op == "Append" {
		op = Op{APPEND, args.Key, args.Value, args.Id, args.ClientId}
	} else {
		log.Fatalf("must be Put or Append command")
		return
	}
	result := kv.startOperation(op)
	reply.Err = result.Err
}

// 服务端操作：只有当迁移命令被应用到状态机时才返回结果
// 问题：如何避免重复执行？
// 只能处理相同config Num的请求, apply的时候如果对应shard的状态为Serving则不执行push操作避免覆盖之前的结果。
func (kv *ShardKV) PushShardService(args *PushShardArgs, reply *PushShardReply) {
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	kv.mu.Lock()
	// 只能处理相同config Num的请求
	if args.ConfigNum != kv.currentConfig.Num {
		reply.Err = ErrWrongConfigNum
		DPrintf("shardkv PushShardService ErrWrongConfigNum: {Server=%d, group=%d} args configNum(%d) != currentNum(%d)",
			kv.me, kv.gid, args.ConfigNum, kv.currentConfig.Num)
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	commonOp := CommonOp{}
	commonOp.Command = *args
	commonOp.Type = CreateShard

	res := kv.Execute(commonOp)
	reply.Err = res.Err
}

// 服务端操作：删除状态为Gcing的shard
func (kv *ShardKV) DeleteShardService(args *DeleteShardArgs, reply *DeleteShardReply) {
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	kv.mu.Lock()
	// 只能处理相同config Num的请求
	if args.ConfigNum != kv.currentConfig.Num {
		reply.Err = ErrWrongConfigNum
		DPrintf("shardkv DeleteShardService ErrWrongConfigNum: {Server=%d, group=%d} args configNum(%d) != currentNum(%d)",
			kv.me, kv.gid, args.ConfigNum, kv.currentConfig.Num)
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()
	commonOp := CommonOp{}
	commonOp.Command = *args
	commonOp.Type = DeleteShard

	res := kv.Execute(commonOp)
	reply.Err = res.Err
}

// 执行delete、更新config、push shard等操作
func (kv *ShardKV) Execute(commonOp CommonOp) ApplyResult {
	index, oldTerm, isLeader := kv.rf.Start(commonOp)
	result := ApplyResult{}
	if !isLeader {
		result.Err = ErrWrongLeader
		DPrintf("shardkv execute ErrWrongLeader: {Server=%d, group=%d}(raft term=%d) {op=%+v, command index=%d}",
			kv.me, kv.gid, oldTerm, commonOp, index)
		return result
	}
	DPrintf("shardkv execute started: {Server=%d, group=%d}(raft term=%d) start with {op=%+v, command index=%d}",
		kv.me, kv.gid, oldTerm, commonOp, index)
	ch := kv.makeResultChan(index)
	select {
	case data := <-ch:
		DPrintf("shardkv execute finished: {Server=%d, group=%d}(raft Term=%d) op(index=%d) result=%+v",
			kv.me, kv.gid, oldTerm, index, data)
		result.Err = data.Err
		result.Value = data.Value
	case <-time.After(ExecuteTimeout):
		DPrintf("shardkv execute ErrTimeout: {Server=%d, group=%d}(raft Term=%d) op(index=%d) Timeout",
			kv.me, kv.gid, oldTerm, index)
		result.Err = ErrTimeout
	}
	kv.removeResultChan(index)
	return result
}
