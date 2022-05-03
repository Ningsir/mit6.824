package kvraft

import (
	"crypto/rand"
	"math/big"
	"sync"

	"6.824/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	// 上一轮leader
	lastLeader  int
	clientId    int64
	sequenceNum int64
	mu          sync.Mutex
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.lastLeader = 0
	ck.clientId = nrand()
	ck.sequenceNum = 0
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {

	numServers := len(ck.servers)
	// TODO: 如何保证每一条命令的ID不同
	ck.mu.Lock()
	defer ck.mu.Unlock()
	args := &GetArgs{key, ck.sequenceNum, ck.clientId}
	ck.sequenceNum += 1
	// ck.mu.Unlock()
	reply := &GetReply{}
	for i := ck.lastLeader; i < numServers; i = (i + 1) % numServers {
		if ck.servers[i].Call("KVServer.Get", args, reply) {
			switch reply.Err {
			case OK:
				ck.lastLeader = i
				return reply.Value
			case ErrNoKey:
				ck.lastLeader = i
				return ""
			case ErrWrongLeader:
				continue
			}
		}
	}
	return ""
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	numServers := len(ck.servers)
	// TODO: 如何保证每一条命令的ID不同
	ck.mu.Lock()
	defer ck.mu.Unlock()
	args := &PutAppendArgs{key, value, op, ck.sequenceNum, ck.clientId}
	ck.sequenceNum += 1
	reply := &PutAppendReply{}
	for i := ck.lastLeader; i < numServers; i = (i + 1) % numServers {
		if ck.servers[i].Call("KVServer.PutAppend", args, reply) {
			switch reply.Err {
			case OK:
				ck.lastLeader = i
				return
			case ErrNoKey:
				ck.lastLeader = i
				return
			case ErrWrongLeader:
				continue
			}
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
