package shardctrler

import (
	"fmt"
	"sync"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

const (
	JoinOp  = "join"
	LeaveOp = "leave"
	MoveOp  = "move"
	QueryOp = "query"
)

const InvalidGID = 0

const Overtime = 100

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.
	seqMap    map[int64]int
	waitChMap map[int]chan Op

	configs []Config // indexed by config num
}

type Op struct {
	// Your data here.
	OpType      string
	SeqId       int
	ClientId    int64
	QueryNum    int
	JoinServers map[int][]string
	LeaveGIDs   []int
	MoveShard   int
	MoveGID     int
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	if !sc.rf.IsLeader() {
		reply.WrongLeader = true
		return
	}
	//传给raft层的cmd
	cmd := Op{
		OpType:      JoinOp,
		ClientId:    args.ClientId,
		SeqId:       args.SeqId,
		JoinServers: args.Servers,
	}
	idx, _, _ := sc.rf.Start(cmd)

	ch := sc.getWaitCh(idx)
	defer func() {
		sc.mu.Lock()
		delete(sc.waitChMap, idx)
		sc.mu.Unlock()
	}()

	timer := time.NewTicker(Overtime * time.Millisecond)
	defer timer.Stop()

	select {
	case replyCmd := <-ch:
		if replyCmd.SeqId != cmd.SeqId || replyCmd.ClientId != cmd.ClientId {
			reply.WrongLeader = false
			return
		} else {
			reply.Err = OK
			return
		}
	case <-timer.C:
		reply.WrongLeader = false
	}

}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	if !sc.rf.IsLeader() {
		reply.WrongLeader = true
		return
	}
	//传给raft层的cmd
	cmd := Op{
		OpType:    LeaveOp,
		SeqId:     args.SeqId,
		ClientId:  args.ClientId,
		LeaveGIDs: args.GIDs,
	}
	idx, _, _ := sc.rf.Start(cmd)

	ch := sc.getWaitCh(idx)
	defer func() {
		sc.mu.Lock()
		delete(sc.waitChMap, idx)
		sc.mu.Unlock()
	}()

	timer := time.NewTicker(Overtime * time.Millisecond)
	defer timer.Stop()

	select {
	case replyCmd := <-ch:
		if replyCmd.SeqId != cmd.SeqId || replyCmd.ClientId != cmd.ClientId {
			reply.WrongLeader = false
			return
		} else {
			reply.Err = OK
			return
		}
	case <-timer.C:
		reply.WrongLeader = false
	}
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	if !sc.rf.IsLeader() {
		reply.WrongLeader = true
		return
	}
	//传给raft层的cmd
	cmd := Op{
		OpType:    MoveOp,
		SeqId:     args.SeqId,
		ClientId:  args.ClientId,
		MoveShard: args.Shard,
		MoveGID:   args.GID,
	}
	idx, _, _ := sc.rf.Start(cmd)

	ch := sc.getWaitCh(idx)
	defer func() {
		sc.mu.Lock()
		delete(sc.waitChMap, idx)
		sc.mu.Unlock()
	}()

	timer := time.NewTicker(Overtime * time.Millisecond)
	defer timer.Stop()

	select {
	case replyCmd := <-ch:
		if replyCmd.SeqId != cmd.SeqId || replyCmd.ClientId != cmd.ClientId {
			reply.WrongLeader = false
			return
		} else {
			reply.Err = OK
			return
		}
	case <-timer.C:
		reply.WrongLeader = false
	}
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	if !sc.rf.IsLeader() {
		reply.WrongLeader = true
		return
	}
	//cmd传给raft层
	cmd := Op{
		OpType:   QueryOp,
		SeqId:    args.SeqId,
		ClientId: args.ClientId,
		QueryNum: args.Num,
	}
	idx, _, _ := sc.rf.Start(cmd)

	ch := sc.getWaitCh(idx)
	defer func() {
		sc.mu.Lock()
		delete(sc.waitChMap, idx)
		sc.mu.Unlock()
	}()

	timer := time.NewTicker(Overtime * time.Millisecond)
	defer timer.Stop()

	select {
	case replyCmd := <-ch:
		if replyCmd.SeqId != cmd.SeqId || replyCmd.ClientId != cmd.ClientId {
			reply.WrongLeader = false
		} else {
			reply.Err = OK
			if cmd.QueryNum == -1 || cmd.QueryNum >= len(sc.configs) {
				reply.Config = sc.configs[len(sc.configs)-1]
			} else {
				reply.Config = sc.configs[cmd.QueryNum]
			}
		}
	case <-timer.C:
		reply.WrongLeader = false
	}
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

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me
	//init a config
	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.
	sc.seqMap = make(map[int64]int)
	sc.waitChMap = make(map[int]chan Op)

	go sc.applyMsgHandler()

	return sc
}

func (sc *ShardCtrler) applyMsgHandler() {
	for {

		msg := <-sc.applyCh
		if msg.CommandValid {
			index := msg.CommandIndex
			op := msg.Command.(Op)
			if !sc.isDup(op.ClientId, op.SeqId) {
				sc.mu.Lock()
				sc.seqMap[op.ClientId] = op.SeqId
				switch op.OpType {
				case QueryOp:
					//no need
				case JoinOp:

					sc.configs = append(sc.configs, *sc.JoinHandler(op.JoinServers))
				case LeaveOp:
					sc.configs = append(sc.configs, *sc.LeaveHandler(op.LeaveGIDs))
				case MoveOp:
					sc.configs = append(sc.configs, *sc.MoveHandler(op.MoveShard, op.MoveGID))
				}
				//?
				// sc.seqMap[op.ClientId] = op.SeqId
				sc.mu.Unlock()
			}

			sc.getWaitCh(index) <- op
		}

	}
}

//*********************************Handler************************************

func (sc *ShardCtrler) JoinHandler(servers map[int][]string) *Config {
	fmt.Printf("sc[%v]:recv join rpc and join servers is:%v\n", sc.me, servers)
	//get the lastest config
	cfg := sc.configs[len(sc.configs)-1]
	newGroups := make(map[int][]string)

	//copy map
	for gid, serverlist := range cfg.Groups {
		newGroups[gid] = serverlist
	}
	//update config
	for gid, serverlist := range servers {
		newGroups[gid] = serverlist
	}
	//for loading balance
	//calculate gid -> shards count
	groupShardsCount := make(map[int]int)
	for gid := range newGroups {
		groupShardsCount[gid] = 0
	}
	for _, gid := range cfg.Shards {
		if gid != 0 {
			groupShardsCount[gid]++
		}
	}
	fmt.Printf("sc[%v]:after join,newGroups:%v\n", sc.me, newGroups)
	if len(groupShardsCount) != 0 {
		return &Config{
			Num:    len(sc.configs),
			Shards: sc.loadingBalance(groupShardsCount, cfg.Shards),
			Groups: newGroups,
		}
	}
	//no need to balance
	return &Config{
		Num:    len(sc.configs),
		Shards: [NShards]int{},
		Groups: newGroups,
	}
}

func (sc *ShardCtrler) LeaveHandler(gids []int) *Config {
	fmt.Printf("sc[%v]-[func-LeaveHandler]:leave gids:%v\n", sc.me, gids)
	leavegids := make(map[int]bool)

	for _, gid := range gids {
		leavegids[gid] = true
	}

	newGroups := make(map[int][]string)
	//get the lastest config
	cfg := sc.configs[len(sc.configs)-1]
	//filter by leavegids
	for gid, serverlist := range cfg.Groups {
		if _, ok := leavegids[gid]; !ok {
			newGroups[gid] = serverlist
		}
	}
	// fmt.Printf("%v\n", newGroups)
	//copy from cfg.shards
	newShards := cfg.Shards
	groupShardsCount := make(map[int]int)

	for gid := range newGroups {
		groupShardsCount[gid] = 0
	}
	fmt.Printf("cfg.shards:%v\n", cfg.Shards)
	for shard, gid := range cfg.Shards {
		if gid != 0 {
			if leavegids[gid] {
				newShards[shard] = InvalidGID
			} else {
				groupShardsCount[gid]++
			}
		}
	}
	fmt.Printf("after leave,newShards:%v\n", newShards)
	fmt.Printf("groupShardsCount:%v\n", groupShardsCount)

	if len(groupShardsCount) != 0 {
		return &Config{
			Num:    len(sc.configs),
			Shards: sc.loadingBalance(groupShardsCount, newShards),
			Groups: newGroups,
		}
	}
	//no need to balance
	return &Config{
		Num:    len(sc.configs),
		Shards: [NShards]int{},
		Groups: newGroups,
	}
}

func (sc *ShardCtrler) MoveHandler(shard int, gid int) *Config {
	cfg := sc.configs[len(sc.configs)-1]
	newCfg := Config{
		Num:    len(sc.configs),
		Shards: [NShards]int{},
		Groups: map[int][]string{},
	}

	// copy Shards
	newCfg.Shards = cfg.Shards
	newCfg.Shards[shard] = gid
	//copy Groups
	for gids, servers := range cfg.Groups {
		newCfg.Groups[gids] = servers
	}

	return &newCfg
}

func (sc *ShardCtrler) getWaitCh(index int) chan Op {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	ch, ok := sc.waitChMap[index]
	if !ok {
		sc.waitChMap[index] = make(chan Op, 1)
		ch = sc.waitChMap[index]
	}
	return ch
}

func (sc *ShardCtrler) isDup(clientId int64, seqId int) bool {
	sc.mu.Lock()
	defer sc.mu.Unlock()

	lastSeqId, ok := sc.seqMap[clientId]
	if !ok {
		return false
	}
	return seqId <= lastSeqId
}

//均衡负载
func (sc *ShardCtrler) loadingBalance(groupShardsCount map[int]int, shards [NShards]int) [NShards]int {
	// fmt.Printf("before bl,shards:%v\n", shards)
	length := len(groupShardsCount)
	ave := NShards / length
	remainder := NShards % length
	sortGids := sortGroupShard(groupShardsCount)

	// 先把负载多的部分free
	for i := 0; i < length; i++ {
		target := ave

		// 判断这个数是否需要更多分配，因为不可能完全均分，在前列的应该为ave+1
		if !moreAllocations(length, remainder, i) {
			target = ave + 1
		}

		// 超出负载
		if groupShardsCount[sortGids[i]] > target {
			overLoadGid := sortGids[i]
			changeNum := groupShardsCount[overLoadGid] - target
			for shard, gid := range shards {
				if changeNum <= 0 {
					break
				}
				if gid == overLoadGid {
					shards[shard] = InvalidGID
					changeNum--
				}
			}
			groupShardsCount[overLoadGid] = target
		}
	}

	// 为负载少的group分配多出来的group
	for i := 0; i < length; i++ {
		target := ave
		if !moreAllocations(length, remainder, i) {
			target = ave + 1
		}

		if groupShardsCount[sortGids[i]] < target {
			freeGid := sortGids[i]
			// fmt.Printf("freeGid:%v\n", freeGid)
			changeNum := target - groupShardsCount[freeGid]
			for shard, gid := range shards {
				if changeNum <= 0 {
					break
				}
				if gid == InvalidGID {
					shards[shard] = freeGid
					changeNum--
				}
			}
			groupShardsCount[freeGid] = target
		}

	}
	// fmt.Printf("after bl,shards:%v\n", shards)
	return shards
}

// 根据sortGroupShard进行排序
// groupShardsCount : groupId -> shard nums
func sortGroupShard(groupShardsCount map[int]int) []int {
	length := len(groupShardsCount)

	gidSlice := make([]int, 0, length)

	// map转换成有序的slice
	for gid := range groupShardsCount {
		gidSlice = append(gidSlice, gid)
	}

	// 让负载压力大的排前面
	// except: 4->3 / 5->2 / 6->1 / 7-> 1 (gids -> shard nums)
	for i := 0; i < length-1; i++ {
		for j := length - 1; j > i; j-- {

			if groupShardsCount[gidSlice[j]] < groupShardsCount[gidSlice[j-1]] || (groupShardsCount[gidSlice[j]] == groupShardsCount[gidSlice[j-1]] && gidSlice[j] < gidSlice[j-1]) {
				gidSlice[j], gidSlice[j-1] = gidSlice[j-1], gidSlice[j]
			}
		}
	}
	return gidSlice
}

func moreAllocations(length int, remainder int, i int) bool {

	// 这个目的是判断index是否在安排ave+1的前列:3、3、3、1 ,ave: 10/4 = 2.5 = 2,则负载均衡后应该是2+1,2+1,2,2
	if i < length-remainder {
		return true
	} else {
		return false
	}
}
