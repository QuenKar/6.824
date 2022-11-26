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

import (
	//	"bytes"

	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
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
type Status int

type VoteState int

type AppendEntriesState int

var HeartBeatTimeout = 120 * time.Millisecond

const (
	Follower Status = iota
	Candidate
	Leader
)

const (
	Normal  VoteState = iota //正常投票
	Killed                   //raft节点已终止了
	Expired                  //投票过期了
	Voted                    //本次Term已经投过了
)

const (
	AppendNormal    AppendEntriesState = iota //正常append
	AppendOutOfDate                           //追加的log entries过时
	AppendKilled                              //raft节点终止
	AppendRepeat                              //重复append log entries
	AppendCommited                            //追加的log entries已经提交
	AppendMismatch                            //log entries不匹配
)

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
	nextIndex  []int //对于每一个node，需要发给它的下一个log entry的下标
	matchIndex []int //对于每一个node，已经复制给它的最大log entry下标

	role     Status        //node 的角色 leader follower candidate
	overtime time.Duration //超时时间
	timer    *time.Ticker  //计时器

	applyChan chan ApplyMsg //从client获取的command，保存在channel里面

}

type AppendEntriesArgs struct {
	Term     int //leader's term
	LeaderId int //leader id

	PrevLogIndex int //前一个处理的log entry的下标
	PrevLogTerm  int //前一个处理的log entry的任期

	Entries []LogEntry //leader发送的log entries，如果为空，就当作heartbeat使用

	LeaderCommit int //leader commited index

}

type AppendEntriesReply struct {
	Term        int
	Success     bool               //true if follower contained entry matching prevLogIndex and prevLogTerm
	AppendState AppendEntriesState //判断append的状态
}

// return currentTerm and whether this server
// believes it is the leader.
// return : (term, isleader)
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	if rf.role == Leader {
		isleader = true
	} else {
		isleader = false
	}

	term = rf.currentTerm

	return term, isleader
}

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
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
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
}

// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
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
	VoteGranted bool      //对于candidate，判断是否拿到了选票，true为拿到
	VoteState   VoteState //判断vote状态
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).

	rf.mu.Lock()
	defer rf.mu.Unlock()
	//server挂掉了
	if rf.killed() {
		reply.Term = -1
		reply.VoteGranted = false
		reply.VoteState = Killed
		return
	}

	//候选人的任期比自己还小，不投票
	if args.Term < rf.currentTerm {
		reply.VoteState = Expired
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	}

	//收到的RPC任期>自己的当前任期，重制自己状态
	if args.Term > rf.currentTerm {
		rf.role = Follower
		rf.currentTerm = args.Term
		rf.votedFor = -1
	}

	//候选人的和自己的Term一样的情况下
	if rf.votedFor == -1 {
		//没投过票
		lastLogIdx := len(rf.logs) - 1
		lastLogTerm := 0
		if lastLogIdx >= 0 {
			lastLogTerm = rf.logs[lastLogIdx].Term
		}
		if lastLogIdx <= args.LastLogIndex && lastLogTerm <= args.LastLogTerm {
			//符合条件，给票
			rf.votedFor = args.CandidateId
			reply.VoteState = Normal
			reply.VoteGranted = true
			reply.Term = args.Term
			rf.timer.Reset(rf.overtime)

			//debug
			// fmt.Printf("[func-RequestVote] : {rf[%v] has voted for rf[%v]}\n", rf.me, rf.votedFor)
		} else {
			//候选人的logs没自己的新，不给票
			reply.VoteState = Expired
			reply.Term = rf.currentTerm
			reply.VoteGranted = false
			return
		}
	} else {
		//任期相同，但是已经投过票的情况，有两种:
		reply.VoteGranted = false
		reply.VoteState = Voted
		//给该候选人投过票了
		if rf.votedFor == args.CandidateId {
			rf.role = Follower

		} else {
			//没票了
			return
		}
		rf.timer.Reset(rf.overtime)
	}

}

// AppendEntries RPC handler
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.killed() {
		reply.AppendState = AppendKilled
		reply.Success = false
		reply.Term = -1
		return
	}

	if args.Term < rf.currentTerm {
		reply.AppendState = AppendOutOfDate
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}

	//更新自己的状态
	rf.currentTerm = args.Term
	rf.votedFor = args.LeaderId
	rf.role = Follower
	rf.timer.Reset(rf.overtime)

	//处理log entries

	//对reply赋值
	reply.AppendState = AppendNormal
	reply.Success = true
	reply.Term = rf.currentTerm
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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply, getVoted *int) bool {
	if rf.killed() {
		return false
	}

	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	//失败重传
	for !ok {
		if rf.killed() {
			return false
		}
		ok = rf.peers[server].Call("Raft.RequestVote", args, reply)
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 这里处理的情况是：candidate发送RequestVote投票后，被别的候选人
	// 抢先变成leader，然后自己的任期已经更新的比
	// 发送RequestVote时要大了，直接放弃本次竞选
	if args.Term < rf.currentTerm {
		return false
	}

	//对reply做处理
	switch reply.VoteState {
	case Expired:
		{
			rf.role = Follower
			rf.timer.Reset(rf.overtime)
			if reply.Term > rf.currentTerm {
				rf.currentTerm = reply.Term
				rf.votedFor = -1
			}
		}

	case Normal, Voted:
		{
			if reply.Term == rf.currentTerm && reply.VoteGranted {
				*getVoted++
			}
			//获得了超过一半的选票，变成leader
			if *getVoted > (len(rf.peers) / 2) {
				//debug
				fmt.Printf("rf[%v] becomes leader!\n", rf.me)

				rf.role = Leader
				rf.nextIndex = make([]int, len(rf.peers))
				for idx := range rf.nextIndex {
					rf.nextIndex[idx] = len(rf.logs) + 1 // 注意这里不是len(rf.logs)，因为logs的索引默认从1开始增加
				}
				rf.timer.Reset(HeartBeatTimeout)
			}
		}
	case Killed:
		return false
	}

	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {

	if rf.killed() {
		return false
	}
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)

	for !ok {
		if rf.killed() {
			return false
		}
		ok = rf.peers[server].Call("Raft.AppendEntries", args, reply)
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	switch reply.AppendState {
	case AppendKilled:
		{
			return false
		}
	case AppendNormal:
		{
			return false
		}
	case AppendOutOfDate:
		{
			rf.role = Follower
			rf.votedFor = -1
			rf.timer.Reset(rf.overtime)
			rf.currentTerm = reply.Term
		}
	}
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
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

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
	rf.mu.Lock()
	rf.timer.Stop()
	rf.mu.Unlock()
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for !rf.killed() {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().

		select {
		case <-rf.timer.C:
			if rf.killed() {
				return
			}
			//lock
			rf.mu.Lock()

			switch rf.role {
			case Follower:
				rf.role = Candidate

			case Candidate:
				rf.currentTerm += 1
				rf.votedFor = rf.me
				//这里需不需要再次rand overtime？yes!
				rf.overtime = time.Duration(150+rand.Intn(201)) * time.Millisecond
				rf.timer.Reset(rf.overtime)
				//统计投票，初始为1（自己给自己投）
				getVoted := 1

				//send requestVote rpc to other servers
				for idx := range rf.peers {
					if idx != rf.me {
						lastlogidx := len(rf.logs) - 1
						voteArgs := RequestVoteArgs{
							Term:         rf.currentTerm,
							CandidateId:  rf.me,
							LastLogIndex: lastlogidx,
							LastLogTerm:  0,
						}
						if lastlogidx >= 0 {
							voteArgs.LastLogTerm = rf.logs[lastlogidx].Term
						}

						voteReply := RequestVoteReply{}
						go rf.sendRequestVote(idx, &voteArgs, &voteReply, &getVoted)
					}
				}

			case Leader:
				// reset timer
				rf.timer.Reset(HeartBeatTimeout)
				//send heartbeat to other servers
				for idx := range rf.peers {
					if idx != rf.me {
						heartbeatArgs := AppendEntriesArgs{
							Term:         rf.currentTerm,
							LeaderId:     rf.me,
							PrevLogIndex: 0,
							PrevLogTerm:  0,
							Entries:      nil,
							LeaderCommit: rf.commitIndex,
						}

						heartbeatReply := AppendEntriesReply{}

						go rf.sendAppendEntries(idx, &heartbeatArgs, &heartbeatReply)
					}

				}

			default:
				fmt.Printf("unknown server role\n")
			}
			//unlock
			rf.mu.Unlock()
		}

	}
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
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	// Your initialization code here (2A, 2B, 2C).

	rf.applyChan = applyCh

	rf.currentTerm = 0
	rf.votedFor = -1 //表示没有投给任何node

	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.logs = make([]LogEntry, 0)
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))

	//初始状态是 follower
	rf.role = Follower
	//overtime is in [150,350]
	rf.overtime = time.Duration(150+rand.Intn(201)) * time.Millisecond
	//初始化定时器
	rf.timer = time.NewTicker(rf.overtime)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
