package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

//note:测试的时候直接用官网的命令行
//因为VSCode Golang 单元测试时默认 timeout 30s超时
//也可以通过修改设置解决
//{
//    "go.testTimeout": "1h"
//}

//2A note
//领导选举照着论文实现没什么大问题
//主要是下面这句话的"up-to-date"具体如何判断？
// If votedFor is null or candidateId, and candidate’s log is at
// least as up-to-date as receiver’s log, grant vote (§5.2, §5.4)

//2B note
//注意logs数组，PrevLogIndex，commitIndex，lastApplied的下标问题。
//还有 rf.mu 相关的锁问题，防止死锁发生。

//2C note
//注意rf.persist()放置的位置，其它的照着样例写就行
//测试的时候TestFigure82C和TestFigure8Unreliable2C
//在1000此循环下无法通过，但是100次可以通过，因为触发了测试30s的超时
//代码逻辑上应该没什么问题，后续代码要优化一下速度

//之前使用的是select{} 运行速度上有问题
//后面改用三个协程ticker，没什么问题了

//2D note
//多打日志debug，理解raft执行流程
//最麻烦的就是下标问题，尤其是snapshot可能直接把logs清空了的情况
//debug了好久才发现这个bug-_-

import (
	//	"bytes"

	"bytes"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labgob"
	"6.824/labrpc"
)

type Status int

type VoteState int

type AppendEntriesState int

// var HeartBeatTimeout = 120 * time.Millisecond

// 设置投票时间范围150-300ms
const (
	MinVoteTime = 150
	MaxVoteTime = 300
)

const (
	HeartBeatInterval = 35 //心跳间隔时间
	AppliedInterval   = 15 //检查是否可以把command应用到状态机的时间间隔
)

const (
	Follower Status = iota
	Candidate
	Leader
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.

// use for applyCh
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// Log Entry
type LogEntry struct {
	Term int
	//command type
	Command interface{}
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	//all node variable
	currentTerm int        //当前任期
	votedFor    int        //当前任期内把票投给了谁
	logs        []LogEntry //保存所有的log entry,log entry需要包涵cmd和term

	commitIndex int //logs当前已经提交的下标，从0单调递增
	lastApplied int //已经被应用到状态机中的logs下标，所以有lastApplied<=commitIndex

	//Leader node variable
	//TODO: 不用Slice，改用map[int]int存储
	nextIndex  []int //对于每一个node，需要发给它的下一个log entry的下标
	matchIndex []int //对于每一个node，已经复制给它的最大log entry下标

	role            Status    //node 的角色： leader follower candidate
	electionTimeout time.Time //计时器
	getVoted        int       //拿到的票数

	applyChan chan ApplyMsg //从client获取的command，保存在channel里面

	//2D
	lastIncludedIndex int
	lastIncludedTerm  int
}

// *********************************ALL RPC struct*********************************
// example RequestVote RPC arguments structure.
// field names must start with capital letters!

// ********************领导选举（Leader Election） RPC********************
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term int

	CandidateId int //寻求投票的node id

	LastLogIndex int //index of candidate’s last log entry
	LastLogTerm  int //term of candidate’s last log entry
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool //对于candidate，判断是否拿到了选票，true为拿到
}

// ********************日志复制(Log replication) RPC********************
// 如果Entries是nil，就当作heartbeat
type AppendEntriesArgs struct {
	Term         int //leader's term
	LeaderId     int //leader id
	PrevLogIndex int //前一个处理的log entry的下标
	PrevLogTerm  int //前一个处理的log entry的任期
	LeaderCommit int //leader commited index

	Entries []LogEntry //leader发送的log entries，如果为空，就当作heartbeat使用

}

type AppendEntriesReply struct {
	Term            int
	Success         bool //true if follower contained entry matching prevLogIndex and prevLogTerm
	UpdateNextIndex int
}

// ********************快照（Snapshot） RPC********************
type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapshotReply struct {
	Term int
}

//*********************持久化（Persist）存储raft节点state*****************

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logs)
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)

}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }

	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var curTerm int
	var votedFor int
	var logs []LogEntry
	var lastIncludedIndex int
	var lastIncludedTerm int
	if d.Decode(&curTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&logs) != nil ||
		d.Decode(&lastIncludedIndex) != nil ||
		d.Decode(&lastIncludedTerm) != nil {
		//debug decode error
		DPrintf("errors in func-readPersist\n")
	} else {
		rf.currentTerm = curTerm
		rf.votedFor = votedFor
		rf.logs = logs
		rf.lastIncludedIndex = lastIncludedIndex
		rf.lastIncludedTerm = lastIncludedTerm
	}
}

// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	if rf.killed() {
		return false
	}
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	if rf.killed() {
		return false
	}

	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) sendSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	if rf.killed() {
		return false
	}
	ok := rf.peers[server].Call("Raft.InstallSnapShot", args, reply)
	return ok
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {

	// Your code here (2B).

	rf.mu.Lock()
	defer rf.mu.Unlock()

	index := -1
	term := -1
	isLeader := true
	if rf.killed() {
		return index, term, false
	}
	//if this server isn't the leader, returns false.
	if !rf.IsLeader() {
		return index, term, false
	}

	isLeader = true

	//生成追加日志
	log := LogEntry{
		Term:    rf.currentTerm,
		Command: command,
	}

	rf.logs = append(rf.logs, log)
	index = rf.getLastLogIndex()
	term = rf.currentTerm

	rf.persist()
	//debug
	DPrintf("in Term[%v]:rf[%v]:generate new log index is %v\n", rf.currentTerm, rf.me, index)

	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	// fmt.Printf("create a new Raft server\n")
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	// Your initialization code here (2A, 2B, 2C).

	rf.mu.Lock()

	rf.currentTerm = 0
	rf.votedFor = -1 //表示没有投给任何node

	rf.commitIndex = 0
	rf.lastApplied = 0

	//logs初始化，添加一个空的entry，fist index is 1
	rf.logs = make([]LogEntry, 0)
	rf.logs = append(rf.logs, LogEntry{})

	//应用状态机Chan init
	rf.applyChan = applyCh

	rf.mu.Unlock()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	if rf.lastIncludedIndex > 0 {
		rf.lastApplied = rf.lastIncludedIndex
	}

	// start ticker goroutine to start elections
	go rf.electionTicker()

	go rf.appendTicker()

	go rf.appliedTicker()

	return rf
}

//********************************ticker**********************************

func (rf *Raft) electionTicker() {
	for !rf.killed() {
		nowTime := time.Now()
		time.Sleep(rf.getRandExpireTime(rf.me))

		rf.mu.Lock()
		//当voted
		if rf.electionTimeout.Before(nowTime) && !rf.IsLeader() {
			//变成candidate
			rf.currentTerm += 1
			rf.role = Candidate
			rf.votedFor = rf.me
			rf.getVoted = 1
			rf.persist()

			//发起选举
			//debug
			DPrintf("in Term[%v]:rf[%v] holds an election\n", rf.currentTerm, rf.me)

			rf.holdElection()

			//没选上的话，要不要回到之前的term?
			//没必要这样,term作用类似于逻辑时钟,保证单调递增就行
			// if !rf.IsLeader() {
			// 	rf.currentTerm -= 1
			// }

			//更新计时器
			rf.electionTimeout = time.Now()
		}
		rf.mu.Unlock()

	}
}

/*
 * leader定时发送追加日志RPC或者是心跳包
 */
func (rf *Raft) appendTicker() {
	for !rf.killed() {

		time.Sleep(HeartBeatInterval * time.Millisecond)
		rf.mu.Lock()
		if rf.IsLeader() {
			rf.mu.Unlock()

			rf.leaderAppendEntries()
			continue
		}

		rf.mu.Unlock()
	}
}

// 每个server定时把ApplyMsg应用到applyChan中

func (rf *Raft) appliedTicker() {
	for !rf.killed() {
		time.Sleep(AppliedInterval * time.Millisecond)
		rf.mu.Lock()

		if rf.lastApplied >= rf.commitIndex {
			rf.mu.Unlock()
			continue
		}
		//make applymsg
		msgs := make([]ApplyMsg, 0)
		for rf.lastApplied < rf.commitIndex && rf.lastApplied < rf.getLastLogIndex() {
			rf.lastApplied += 1
			msg := ApplyMsg{
				CommandValid: true,
				Command:      rf.getRealLog(rf.lastApplied).Command,
				CommandIndex: rf.lastApplied,

				SnapshotValid: false,
			}
			msgs = append(msgs, msg)
		}
		rf.mu.Unlock()
		//send applymsg to ApplyChan
		for _, v := range msgs {
			//debug
			DPrintf("rf[%v]:ApplyMsg%v has apply to Chan\n", rf.me, v)
			rf.applyChan <- v
		}
	}
}

// **********************************Leader Election**********************************
func (rf *Raft) holdElection() {
	//send requestVote rpc to other servers
	for i := range rf.peers {
		if i == rf.me {
			continue
		}

		go func(serverId int) {
			rf.mu.Lock()
			args := RequestVoteArgs{
				Term:         rf.currentTerm,
				CandidateId:  rf.me,
				LastLogIndex: rf.getLastLogIndex(),
				LastLogTerm:  rf.getLastLogTerm(),
			}
			reply := RequestVoteReply{}
			rf.mu.Unlock()

			ok := rf.sendRequestVote(serverId, &args, &reply)

			if ok {
				rf.mu.Lock()
				//如果不是candidate
				//或者当前任期已经大于发送RequestVote时的任期了,
				//表示已经有别的candidate成为了leader,并且那个leader已经更新了这个server的term
				if rf.role != Candidate || args.Term < rf.currentTerm {
					rf.mu.Unlock()
					return
				}

				//处理网络分区情况
				if args.Term < reply.Term {
					if rf.currentTerm < reply.Term {
						rf.currentTerm = reply.Term
					}

					rf.setState(Follower, -1, 0, true)

					rf.mu.Unlock()
					return
				}

				if reply.VoteGranted && reply.Term == rf.currentTerm {
					rf.getVoted += 1

					if rf.getVoted > len(rf.peers)/2 {
						//debug
						DPrintf("rf[%v] becomes Leader\n", rf.me)

						//超过半数同意，变成leader
						rf.setState(Leader, -1, 0, false)

						//init nextIndex
						rf.nextIndex = make([]int, len(rf.peers))
						for i := range rf.peers {
							if i == rf.me {
								continue
							}
							rf.nextIndex[i] = rf.getLastLogIndex() + 1
						}
						//init matchIndex
						rf.matchIndex = make([]int, len(rf.peers))
						rf.matchIndex[rf.me] = -1

						rf.electionTimeout = time.Now()

						rf.mu.Unlock()
						return
					}
					rf.mu.Unlock()
					return

				}
				rf.mu.Unlock()
				return
			}
		}(i)
	}

}

// RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	//server挂掉了
	if rf.killed() {
		reply.Term = -1
		reply.VoteGranted = false
		return
	}

	//candidate的任期比自己还小，不投票
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	}

	//收到的RPC任期>自己的当前任期，重制自己状态
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.setState(Follower, -1, 0, false)
	}

	//不给票
	if !rf.IsCandicateLogsOK(args.LastLogIndex, args.LastLogTerm) ||
		(rf.votedFor != -1 && rf.votedFor != args.CandidateId) {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	}
	//符合条件，给票
	rf.votedFor = args.CandidateId
	rf.currentTerm = args.Term
	rf.electionTimeout = time.Now()

	rf.persist()

	reply.VoteGranted = true
	reply.Term = rf.currentTerm

	//debug
	DPrintf("in term[%v]:{rf[%v] has voted for rf[%v]}\n", rf.currentTerm, rf.me, rf.votedFor)

}

// ****************************logs replication****************************
func (rf *Raft) leaderAppendEntries() {

	for i := range rf.peers {
		if i == rf.me {
			continue
		}

		go func(serverId int) {
			rf.mu.Lock()
			if !rf.IsLeader() {
				rf.mu.Unlock()
				return
			}

			//当follower的logs太旧了，直接发送快照过去
			if rf.nextIndex[serverId]-1 < rf.lastIncludedIndex {
				go rf.leaderSendSnapshot(serverId)
				rf.mu.Unlock()
				return
			}

			//快照冲突怎么办？
			//自己存储的snapshot和leader发过来的snapshot是分开的，不会冲突

			prevLogIndex := rf.nextIndex[serverId] - 1
			if prevLogIndex > rf.getLastLogIndex() {
				prevLogIndex = rf.getLastLogIndex()
			}
			prevLogTerm := rf.getRealLogTerm(prevLogIndex)
			args := AppendEntriesArgs{
				Term:         rf.currentTerm,
				LeaderId:     rf.me,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm:  prevLogTerm,
				LeaderCommit: rf.commitIndex,
			}
			reply := AppendEntriesReply{}
			if rf.getLastLogIndex() >= rf.nextIndex[serverId] {
				entries := make([]LogEntry, 0)
				entries = append(entries, rf.logs[rf.nextIndex[serverId]-rf.lastIncludedIndex:]...)
				args.Entries = entries
			} else {
				//for heartbeat
				args.Entries = nil
			}
			rf.mu.Unlock()

			//debug
			DPrintf("in Term[%v]:rf[%v]'args to rf[%v]:%v\n", rf.currentTerm, rf.me, serverId, args)

			ok := rf.sendAppendEntries(serverId, &args, &reply)

			if ok {
				rf.mu.Lock()
				defer rf.mu.Unlock()

				if !rf.IsLeader() {
					return
				}

				//网络分区
				if reply.Term > rf.currentTerm {
					rf.currentTerm = reply.Term
					rf.setState(Follower, -1, 0, true)

					return
				}

				if reply.Success {
					//if heartbeat, return directly
					if args.Entries == nil {
						return
					}

					//update matchIndex and nextIndex
					rf.matchIndex[serverId] = args.PrevLogIndex + len(args.Entries)
					rf.nextIndex[serverId] = rf.matchIndex[serverId] + 1

					//update commitIndex
					if rf.commitIndex < rf.lastIncludedIndex {
						rf.commitIndex = rf.lastIncludedIndex
					}
					for idx := rf.getLastLogIndex(); idx > rf.lastIncludedIndex; idx-- {
						sum := 0
						for p := range rf.peers {
							if p == rf.me {
								sum += 1
								continue
							}
							if rf.matchIndex[p] >= idx {
								sum += 1
							}
						}
						//over half servers have this log,update commitIndex
						if sum > len(rf.peers)/2 && rf.getRealLogTerm(idx) == rf.currentTerm {
							//debug
							DPrintf("leader[%v]'commitIndex is %v\n", rf.me, idx)
							//可能有多个reply返回会先后更新这个字段，只保留最大的commitIndex
							if idx >= rf.commitIndex {
								rf.commitIndex = idx
							}
							break
						}
					}
				} else {
					//logs有冲突
					if reply.UpdateNextIndex != -1 {
						rf.nextIndex[serverId] = reply.UpdateNextIndex
					}
				}
			}

		}(i)
	}
}

// AppendEntries RPC handler
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.killed() {
		reply.Success = false
		reply.Term = -1
		reply.UpdateNextIndex = -1
		return
	}

	if args.Term < rf.currentTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		reply.UpdateNextIndex = -1
		return
	}

	//update self's state
	rf.currentTerm = args.Term
	rf.setState(Follower, args.LeaderId, 0, true)

	//deal with log entries
	reply.Term = rf.currentTerm
	reply.UpdateNextIndex = -1
	reply.Success = true

	//感觉有点问题,这里在跑3B测试的时候，Slice数组可能会越界，
	//这里处理preLogIndex-lastIncludedIndex为负数，会导致Slice越界的问题
	if rf.lastIncludedIndex > args.PrevLogIndex {
		reply.Success = false
		reply.UpdateNextIndex = rf.lastIncludedIndex + 1
		return
	}

	//conflict
	//1. PreLogindex超过了自己logs的长度，表示中间有漏掉的log entries，理想情况下PreLogindex应当等于logs最右的下标
	if rf.getLastLogIndex() < args.PrevLogIndex {
		//debug
		DPrintf("rf[%v]:[func-AppendEntries]:lost logs\n", rf.me)
		reply.Success = false
		reply.UpdateNextIndex = rf.getLastLogIndex() + 1
		//debug
		DPrintf("rf[%v]:reply.UpdateNextIndex = %v\n", rf.me, reply.UpdateNextIndex)
		return
	} else {
		//2. RPC中的PreLogTerm和自己日志中的Term不相同,往前找前一个任期
		if rf.getRealLogTerm(args.PrevLogIndex) != args.PrevLogTerm {
			//debug
			DPrintf("rf[%v]:[func-AppendEntries]:update nextIndex\n", rf.me)
			reply.Success = false
			tmpTerm := rf.getRealLogTerm(args.PrevLogIndex)
			DPrintf("args.PrevLogIndex = %v ,args.PrevLogTerm = %v,follower[%v]:real PreLogTerm = %v\n", args.PrevLogIndex, args.PrevLogTerm, rf.me, tmpTerm)
			//debug
			// DPrintf("tempTerm=%v\n", tmpTerm)
			// DPrintf("PrevLogIndex=%v\n", args.PrevLogIndex)
			//默认为自己快照的下一个index
			//debug
			DPrintf("rf[%v]:lastIncludedIndex = %v\n", rf.me, rf.lastIncludedIndex)
			reply.UpdateNextIndex = rf.lastIncludedIndex + 1
			for i := args.PrevLogIndex; i > rf.lastIncludedIndex; i-- {
				if rf.getRealLogTerm(i) != tmpTerm {
					reply.UpdateNextIndex = i + 1
					break
				}
			}
			//debug
			DPrintf("rf[%v]:reply.UpdateNextIndex = %v\n", rf.me, reply.UpdateNextIndex)
			return
		}
	}
	//debug heartbeat
	// if args.Entries == nil {
	// 	fmt.Println("heartbeat")
	// }

	if args.Entries != nil {
		//假如存在“多余”log entry，丢掉preLogIndex后面的log entry
		rf.logs = rf.logs[:args.PrevLogIndex-rf.lastIncludedIndex+1]
		rf.logs = append(rf.logs, args.Entries...)
		//debug
		// DPrintf("Leader[%v] add logs[%v].. to Follower[%v]\n", args.LeaderId, args.PrevLogIndex+1, rf.me)
	}
	//持久化更新的state
	rf.persist()

	//update commitIndex
	//debug
	// fmt.Printf("follower update commitIndex\n")
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(rf.getLastLogIndex(), args.LeaderCommit)
		//debug
		DPrintf("follower[%v]'commitIndex is updated to %v \n", rf.me, rf.commitIndex)
	}

}

// ********************************Snapshot**********************************
// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	if rf.killed() {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	//已经快照过的下标大于index或者提交的下标小于index，都不进行snapshot
	if rf.lastIncludedIndex >= index || rf.commitIndex < index {
		return
	}

	//更新快照
	sLogs := make([]LogEntry, 0)
	sLogs = append(sLogs, LogEntry{})
	//这里可以优化一下
	for i := index + 1; i <= rf.getLastLogIndex(); i++ {
		sLogs = append(sLogs, rf.getRealLog(i))
	}

	//更新lastIncludedIndex，lastIncludedTerm
	//坑死我了:getRealLogTerm()里面用了lastIncludedIndex，要先更新lastIncludedTerm
	rf.lastIncludedTerm = rf.getRealLogTerm(index)
	rf.lastIncludedIndex = index

	rf.logs = sLogs

	//需要重制commitIndex和lastApplied，从index开始
	if index > rf.commitIndex {
		rf.commitIndex = index
	}

	if index > rf.lastApplied {
		rf.lastApplied = index
	}

	//小坑：这里保存state和snapshot,注意先snapshot，再state，不然state会被覆盖
	rf.persister.SaveStateAndSnapshot(nil, snapshot)
	rf.persist()

	//debug
	// fmt.Printf("in Term[%v]:rf[%v]:SnapShot to %v\n", rf.currentTerm, rf.me, rf.lastIncludedIndex)
}

// leader 发送给 follower snapshot
func (rf *Raft) leaderSendSnapshot(serverId int) {
	rf.mu.Lock()

	args := InstallSnapshotArgs{
		Term:              rf.currentTerm,
		LeaderId:          rf.me,
		LastIncludedIndex: rf.lastIncludedIndex,
		LastIncludedTerm:  rf.lastIncludedTerm,
		Data:              rf.persister.ReadSnapshot(),
	}
	reply := InstallSnapshotReply{}

	rf.mu.Unlock()

	ok := rf.sendSnapshot(serverId, &args, &reply)

	//deal with reply
	if ok {
		rf.mu.Lock()
		defer rf.mu.Unlock()

		if !rf.IsLeader() || reply.Term != rf.currentTerm {
			return
		}

		//自己的数据太旧了
		if reply.Term > rf.currentTerm {
			rf.setState(Follower, -1, 0, true)

			return
		}

		rf.matchIndex[serverId] = args.LastIncludedIndex
		rf.nextIndex[serverId] = rf.matchIndex[serverId] + 1

		return
	}
}

// InstallSnapshot RPC handler
func (rf *Raft) InstallSnapShot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	if rf.currentTerm > args.Term {
		reply.Term = rf.currentTerm
		rf.mu.Unlock()
		return
	}

	rf.currentTerm = args.Term
	reply.Term = rf.currentTerm

	rf.setState(Follower, -1, 0, true)

	if rf.lastIncludedIndex >= args.LastIncludedIndex {
		rf.mu.Unlock()
		return
	}

	index := args.LastIncludedIndex

	tmpLogs := make([]LogEntry, 0)
	tmpLogs = append(tmpLogs, LogEntry{})

	for i := index + 1; i <= rf.getLastLogIndex(); i++ {
		tmpLogs = append(tmpLogs, rf.getRealLog(i))
	}

	rf.lastIncludedIndex = args.LastIncludedIndex
	rf.lastIncludedTerm = args.LastIncludedTerm

	rf.logs = tmpLogs

	//重置commitIndex lastAppied
	if index > rf.commitIndex {
		rf.commitIndex = rf.lastIncludedIndex
	}
	if index > rf.lastApplied {
		rf.lastApplied = rf.lastIncludedIndex
	}

	//persist
	rf.persister.SaveStateAndSnapshot(nil, args.Data)
	rf.persist()

	msg := ApplyMsg{
		SnapshotValid: true,
		Snapshot:      args.Data,
		SnapshotTerm:  rf.lastIncludedTerm,
		SnapshotIndex: rf.lastIncludedIndex,
	}

	rf.mu.Unlock()

	//add msg to state machine
	rf.applyChan <- msg
}
