package raft

import (
	"log"
	"math/rand"
	"time"
)

// Debugging
const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

func (rf *Raft) IsLeader() bool {
	return rf.role == Leader
}

// return currentTerm and whether this server
// believes it is the leader.
// return : (term, isleader)
func (rf *Raft) GetState() (int, bool) {
	//查询rf状态时，必须要上锁，不然TestBackup2B会出现死锁
	rf.mu.Lock()
	defer rf.mu.Unlock()

	return rf.currentTerm, rf.IsLeader()
}

func (rf *Raft) getRandExpireTime(serverId int) time.Duration {
	rand.Seed(time.Now().Unix() + int64(serverId))
	return time.Duration((rand.Intn(MaxVoteTime-MinVoteTime) + MinVoteTime)) * time.Millisecond
}

// 因为有快照的存在，逻辑上的log index需要真实log index加上快照中的lastIncludedIndex
// 逻辑上的log index是单调递增，因为论文中commitIndex和lastApplied也是单调递增的
// @return: logically last log index.
func (rf *Raft) getLastLogIndex() int {
	return len(rf.logs) - 1 + rf.lastIncludedIndex
}

// 返回最后一个log的term
func (rf *Raft) getLastLogTerm() int {
	if len(rf.logs)-1 == 0 {
		return rf.lastIncludedTerm
	} else {
		return rf.logs[len(rf.logs)-1].Term
	}
}

// 这里index是指逻辑log index
// 根据logic index获取log
func (rf *Raft) getRealLog(index int) LogEntry {
	return rf.logs[index-rf.lastIncludedIndex]
}

// 根据logic index获取log term
func (rf *Raft) getRealLogTerm(index int) int {
	//此时logs应当只有一个LogEntry{}
	if index-rf.lastIncludedIndex == 0 {
		return rf.lastIncludedTerm
	}
	return rf.logs[index-rf.lastIncludedIndex].Term
}

func (rf *Raft) IsCandicateLogsOK(Cindex int, Cterm int) bool {
	lastIndex := rf.getLastLogIndex()
	lastTerm := rf.getLastLogTerm()
	//1.候选人term大于自己，给票
	//2.term相同，但是候选人logs长度>=自己，给票
	return Cterm > lastTerm || (Cterm == lastTerm && Cindex >= lastIndex)
}

func min(a int, b int) int {
	if a >= b {
		return a
	}
	return b
}
