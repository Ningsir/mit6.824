package raft

import (
	"math/rand"
	"sync"
	"time"
)

type Timer struct {
	// 上一次重置计时器的时间
	time_    time.Time
	timeout_ int64
	mu       sync.Mutex
}

func RandomElectionTimeout() int {
	rand.Seed(int64(time.Now().Nanosecond()))
	return rand.Intn(200) + 300
}

func StableHeartBeatTimeout() int {
	return 100
}

// 重置计时器
func (timer *Timer) reset(ms int) {
	timer.mu.Lock()
	timer.time_ = time.Now()
	// 生成300-500的随机数
	timer.timeout_ = int64(ms)
	timer.mu.Unlock()
}

// 判断是否超时
func (timer *Timer) timeout() bool {
	timer.mu.Lock()
	res := time.Now().Sub(timer.time_).Milliseconds() > timer.timeout_
	timer.mu.Unlock()
	return res
}
