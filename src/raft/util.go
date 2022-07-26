package raft

import (
	"math/rand"
	"os"
	"sort"
	"time"

	"github.com/pedrogao/log"
)

var (
	// Debug Debugging
	Debug  = false
	logger = log.New(log.WithSkipLevel(3))
)

func init() {
	if os.Getenv("debug") != "" || Debug {
		logger.SetOptions(log.WithLevel(log.DebugLevel))
	} else {
		logger.SetOptions(log.WithLevel(log.ErrorLevel))
	}
}

func DPrintf(format string, a ...any) /*(n int, err error)*/ {
	logger.Debugf(format, a...)
	return
}

// time
const (
	electionTimeoutMin = 150
	electionTimeoutMax = 300
	// 测试者把你限制在每秒10次心跳
	// 所以心跳超时时间控制在 < 1s / 10 即可，即 100 ms 是极大值
	// 这里心跳间隔选择使用 50 ms
	heartbeatInterval = 50 * time.Millisecond
	// apply log interval
	applyInterval = 30 * time.Millisecond
)

// 获取选举超时时间，随机的，避免出现同时选举的情况
func getRandomElectTimeout() time.Duration {
	r := rand.Int63n(electionTimeoutMax-electionTimeoutMin) + electionTimeoutMin
	return time.Duration(r) * time.Millisecond
}

// index
func getMajorIndex(matchIndex []int) int {
	t := make([]int, len(matchIndex))
	copy(t, matchIndex)
	sort.Sort(sort.Reverse(sort.IntSlice(t)))
	idx := len(t) / 2
	return t[idx]
}

func minInt(i int, j int) int {
	if i < j {
		return i
	}
	return j
}

func maxInt(i int, j int) int {
	if i > j {
		return i
	}
	return j
}
