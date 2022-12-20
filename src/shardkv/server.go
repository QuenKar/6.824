package shardkv

import (
	"bytes"
	"log"
	"sync/atomic"
	"time"

	"sync"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"6.824/shardctrler"
)

//----------------------------------------------------结构体定义部分------------------------------------------------------

const (
	UpdateCfgInterval = 100 * time.Millisecond // update configuration periodly

	OpTimeout = 500 * time.Millisecond // limit operation execution time
)

type Shard struct {
	KvMap     map[string]string
	ConfigNum int // what version this Shard is in
}

type Op struct {

	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClientId  int64
	SeqId     int
	OpType    OpType // "get" "put" "append"
	Key       string
	Value     string
	UpdateCfg shardctrler.Config
	ShardId   int
	Shard     Shard
	SeqMap    map[int64]int
}

// OpReply is used to wake waiting RPC caller after Op arrived from applyCh
type OpReply struct {
	ClientId int64
	SeqId    int
	Err      Err
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	ctrlers      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.

	dead int32 // set by Kill()

	LatestCfg shardctrler.Config // 更新的最新的配置
	PrevCfg   shardctrler.Config // 更新之前的配置，用于比对是否全部更新完了

	shardsPersist []Shard // ShardId -> Shard 如果KvMap == nil则说明当前的数据不归当前分片管
	waitChMap     map[int]chan OpReply
	SeqMap        map[int64]int
	sck           *shardctrler.Clerk // sck is a client used to contact shard master
}

// StartServer me is the index of the current server in servers[].
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
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, masters []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = masters

	// Your initialization code here.

	kv.shardsPersist = make([]Shard, shardctrler.NShards)

	kv.SeqMap = make(map[int64]int)

	// Use something like this to talk to the shardctrler:
	// kv.mck = shardctrler.MakeClerk(kv.masters)
	kv.sck = shardctrler.MakeClerk(kv.ctrlers)
	kv.waitChMap = make(map[int]chan OpReply)

	snapshot := persister.ReadSnapshot()
	if len(snapshot) > 0 {
		kv.DecodeSnapShot(snapshot)
	}

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	go kv.applyMsgHandler()
	go kv.ConfigDetected()

	return kv
}

// applyMsgHandler 处理applyCh发送过来的ApplyMsg
func (kv *ShardKV) applyMsgHandler() {
	for {
		if kv.killed() {
			return
		}

		msg := <-kv.applyCh

		if msg.CommandValid {
			kv.mu.Lock()
			op := msg.Command.(Op)
			reply := OpReply{
				ClientId: op.ClientId,
				SeqId:    op.SeqId,
				Err:      OK,
			}

			if op.OpType == PutType || op.OpType == GetType || op.OpType == AppendType {

				shardId := key2shard(op.Key)

				//
				if kv.LatestCfg.Shards[shardId] != kv.gid {
					reply.Err = ErrWrongGroup
				} else if kv.shardsPersist[shardId].KvMap == nil {
					// 如果应该存在的切片没有数据那么这个切片就还没到达
					reply.Err = ShardNotArrived
				} else {

					if !kv.isDup(op.ClientId, op.SeqId) {

						kv.SeqMap[op.ClientId] = op.SeqId
						switch op.OpType {
						case PutType:
							kv.shardsPersist[shardId].KvMap[op.Key] = op.Value
						case AppendType:
							kv.shardsPersist[shardId].KvMap[op.Key] += op.Value
						case GetType:
							// 如果是Get都不用做
						default:
							log.Fatalf("invalid command type: %v.", op.OpType)
						}
					}
				}
			} else {
				// request from server of other group
				switch op.OpType {

				case UpConfigType:
					kv.updateCfgHandler(op)
				case AddShardType:

					// 如果配置号比op的SeqId还低说明不是最新的配置
					if kv.LatestCfg.Num < op.SeqId {
						reply.Err = ConfigNotArrived
						break
					}
					kv.addShardHandler(op)
				case RemoveShardType:
					// remove operation is from previous UpConfig
					kv.removeShardHandler(op)
				default:
					log.Fatalf("invalid command type: %v.", op.OpType)
				}
			}

			// 如果需要snapshot，且超出其stateSize
			if kv.maxraftstate != -1 && kv.rf.GetRaftStateSize() > kv.maxraftstate {
				snapshot := kv.EncodeSnapShot()
				kv.rf.Snapshot(msg.CommandIndex, snapshot)
			}

			ch := kv.getWaitCh(msg.CommandIndex)
			ch <- reply
			kv.mu.Unlock()

		}

		if msg.SnapshotValid {
			if kv.rf.CondInstallSnapshot(msg.SnapshotTerm, msg.SnapshotIndex, msg.Snapshot) {
				// 读取快照的数据
				kv.mu.Lock()
				kv.DecodeSnapShot(msg.Snapshot)
				kv.mu.Unlock()
			}
			continue
		}

	}

}

// Config Detect
func (kv *ShardKV) ConfigDetected() {
	kv.mu.Lock()

	curConfig := kv.LatestCfg
	rf := kv.rf
	kv.mu.Unlock()

	for !kv.killed() {
		// only leader needs to deal with configuration tasks
		if _, isLeader := rf.GetState(); !isLeader {
			time.Sleep(UpdateCfgInterval)
			continue
		}
		kv.mu.Lock()

		// 判断是否把不属于自己的部分给分给别人了
		if !kv.allSent() {
			SeqMap := make(map[int64]int)
			for k, v := range kv.SeqMap {
				SeqMap[k] = v
			}
			for shardId, gid := range kv.PrevCfg.Shards {

				// 将最新配置里不属于自己的分片分给别人
				if gid == kv.gid && kv.LatestCfg.Shards[shardId] != kv.gid && kv.shardsPersist[shardId].ConfigNum < kv.LatestCfg.Num {

					sendDate := kv.cloneShard(kv.LatestCfg.Num, kv.shardsPersist[shardId].KvMap)

					args := SendShardArg{
						LastAppliedRequestId: SeqMap,
						ShardId:              shardId,
						Shard:                sendDate,
						ClientId:             int64(gid),
						RequestId:            kv.LatestCfg.Num,
					}

					// shardId -> gid -> server names
					serversList := kv.LatestCfg.Groups[kv.LatestCfg.Shards[shardId]]
					servers := make([]*labrpc.ClientEnd, len(serversList))
					for i, name := range serversList {
						servers[i] = kv.make_end(name)
					}

					// 开启协程对每个客户端发送切片(这里发送的应是别的组别，自身的共识组需要raft进行状态修改）
					go func(servers []*labrpc.ClientEnd, args *SendShardArg) {

						index := 0
						start := time.Now()
						for {
							var reply AddShardReply
							// 对自己的共识组内进行add
							ok := servers[index].Call("ShardKV.AddShard", args, &reply)

							// 如果给予切片成功，或者时间超时，这两种情况都需要进行GC掉不属于自己的切片
							if ok && reply.Err == OK || time.Since(start) >= 2*time.Second {

								// sucess
								kv.mu.Lock()
								command := Op{
									OpType:   RemoveShardType,
									ClientId: int64(kv.gid),
									SeqId:    kv.LatestCfg.Num,
									ShardId:  args.ShardId,
								}
								kv.mu.Unlock()
								kv.startCommand(command, OpTimeout)
								break
							}
							index = (index + 1) % len(servers)
							if index == 0 {
								time.Sleep(UpdateCfgInterval)
							}
						}
					}(servers, &args)
				}
			}
			kv.mu.Unlock()
			time.Sleep(UpdateCfgInterval)
			continue
		}
		if !kv.allReceived() {
			kv.mu.Unlock()
			time.Sleep(UpdateCfgInterval)
			continue
		}

		// current configuration is configured, poll for the next configuration
		curConfig = kv.LatestCfg
		sck := kv.sck
		kv.mu.Unlock()

		newConfig := sck.Query(curConfig.Num + 1)
		if newConfig.Num != curConfig.Num+1 {
			time.Sleep(UpdateCfgInterval)
			continue
		}

		command := Op{
			OpType:    UpConfigType,
			ClientId:  int64(kv.gid),
			SeqId:     newConfig.Num,
			UpdateCfg: newConfig,
		}
		kv.startCommand(command, OpTimeout)
	}

}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	shardId := key2shard(args.Key)
	kv.mu.Lock()
	if kv.LatestCfg.Shards[shardId] != kv.gid {
		reply.Err = ErrWrongGroup
	} else if kv.shardsPersist[shardId].KvMap == nil {
		reply.Err = ShardNotArrived
	}
	kv.mu.Unlock()
	if reply.Err == ErrWrongGroup || reply.Err == ShardNotArrived {
		return
	}
	command := Op{
		OpType:   GetType,
		ClientId: args.ClientId,
		SeqId:    args.RequestId,
		Key:      args.Key,
	}
	err := kv.startCommand(command, OpTimeout)
	if err != OK {
		reply.Err = err
		return
	}
	kv.mu.Lock()

	if kv.LatestCfg.Shards[shardId] != kv.gid {
		reply.Err = ErrWrongGroup
	} else if kv.shardsPersist[shardId].KvMap == nil {
		reply.Err = ShardNotArrived
	} else {
		reply.Err = OK
		reply.Value = kv.shardsPersist[shardId].KvMap[args.Key]
	}
	kv.mu.Unlock()
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	shardId := key2shard(args.Key)
	kv.mu.Lock()
	if kv.LatestCfg.Shards[shardId] != kv.gid {
		reply.Err = ErrWrongGroup
	} else if kv.shardsPersist[shardId].KvMap == nil {
		reply.Err = ShardNotArrived
	}
	kv.mu.Unlock()
	if reply.Err == ErrWrongGroup || reply.Err == ShardNotArrived {
		return
	}
	command := Op{
		OpType:   args.Op,
		ClientId: args.ClientId,
		SeqId:    args.RequestId,
		Key:      args.Key,
		Value:    args.Value,
	}
	reply.Err = kv.startCommand(command, OpTimeout)
}

// AddShard move shards from caller to this server
func (kv *ShardKV) AddShard(args *SendShardArg, reply *AddShardReply) {
	command := Op{
		OpType:   AddShardType,
		ClientId: args.ClientId,
		SeqId:    args.RequestId,
		ShardId:  args.ShardId,
		Shard:    args.Shard,
		SeqMap:   args.LastAppliedRequestId,
	}
	reply.Err = kv.startCommand(command, OpTimeout)
}

func (kv *ShardKV) updateCfgHandler(op Op) {
	curConfig := kv.LatestCfg
	upConfig := op.UpdateCfg
	if curConfig.Num >= upConfig.Num {
		return
	}
	for shard, gid := range upConfig.Shards {
		if gid == kv.gid && curConfig.Shards[shard] == 0 {
			kv.shardsPersist[shard].KvMap = make(map[string]string)
			kv.shardsPersist[shard].ConfigNum = upConfig.Num
		}
	}
	kv.PrevCfg = curConfig
	kv.LatestCfg = upConfig

}

func (kv *ShardKV) addShardHandler(op Op) {
	// this shard is added or it is an outdated command
	if kv.shardsPersist[op.ShardId].KvMap != nil || op.Shard.ConfigNum < kv.LatestCfg.Num {
		return
	}

	kv.shardsPersist[op.ShardId] = kv.cloneShard(op.Shard.ConfigNum, op.Shard.KvMap)

	for clientId, seqId := range op.SeqMap {
		if r, ok := kv.SeqMap[clientId]; !ok || r < seqId {
			kv.SeqMap[clientId] = seqId
		}
	}
}

func (kv *ShardKV) removeShardHandler(op Op) {
	if op.SeqId < kv.LatestCfg.Num {
		return
	}
	kv.shardsPersist[op.ShardId].KvMap = nil
	kv.shardsPersist[op.ShardId].ConfigNum = op.SeqId
}

// EncodeSnapShot Snapshot get snapshot data of kvserver
func (kv *ShardKV) EncodeSnapShot() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	e.Encode(kv.shardsPersist)
	e.Encode(kv.SeqMap)
	e.Encode(kv.maxraftstate)
	e.Encode(kv.LatestCfg)
	e.Encode(kv.PrevCfg)

	return w.Bytes()
}

// DecodeSnapShot install a given snapshot
func (kv *ShardKV) DecodeSnapShot(snapshot []byte) {
	if snapshot == nil || len(snapshot) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)

	var shardsPersist []Shard
	var SeqMap map[int64]int
	var MaxRaftState int
	var Config, LastConfig shardctrler.Config

	if d.Decode(&shardsPersist) != nil || d.Decode(&SeqMap) != nil ||
		d.Decode(&MaxRaftState) != nil || d.Decode(&Config) != nil || d.Decode(&LastConfig) != nil {
		log.Fatalf("Server[%v]:failed to decode snapshot!\n", kv.me)
	} else {
		kv.shardsPersist = shardsPersist
		kv.SeqMap = SeqMap
		kv.maxraftstate = MaxRaftState
		kv.LatestCfg = Config
		kv.PrevCfg = LastConfig

	}
}

// Kill the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (kv *ShardKV) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *ShardKV) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *ShardKV) isDup(clientId int64, seqId int) bool {

	lastSeqId, exist := kv.SeqMap[clientId]
	if !exist {
		return false
	}
	return seqId <= lastSeqId
}

func (kv *ShardKV) getWaitCh(index int) chan OpReply {

	ch, exist := kv.waitChMap[index]
	if !exist {
		kv.waitChMap[index] = make(chan OpReply, 1)
		ch = kv.waitChMap[index]
	}
	return ch
}

func (kv *ShardKV) allSent() bool {
	for shard, gid := range kv.PrevCfg.Shards {
		if gid == kv.gid && kv.LatestCfg.Shards[shard] != kv.gid && kv.shardsPersist[shard].ConfigNum < kv.LatestCfg.Num {
			return false
		}
	}
	return true
}

func (kv *ShardKV) allReceived() bool {
	for shard, gid := range kv.PrevCfg.Shards {
		if gid != kv.gid && kv.LatestCfg.Shards[shard] == kv.gid && kv.shardsPersist[shard].ConfigNum < kv.LatestCfg.Num {
			return false
		}
	}
	return true
}

func (kv *ShardKV) startCommand(command Op, timeoutPeriod time.Duration) Err {
	kv.mu.Lock()
	index, _, isLeader := kv.rf.Start(command)
	if !isLeader {
		kv.mu.Unlock()
		return ErrWrongLeader
	}

	ch := kv.getWaitCh(index)
	kv.mu.Unlock()

	timer := time.NewTicker(timeoutPeriod)
	defer timer.Stop()

	select {
	case re := <-ch:
		kv.mu.Lock()
		delete(kv.waitChMap, index)
		if re.SeqId != command.SeqId || re.ClientId != command.ClientId {
			// One way to do this is for the server to detect that it has lost leadership,
			// by noticing that a different request has appeared at the index returned by Start()
			kv.mu.Unlock()
			return ErrInconsistentData
		}
		kv.mu.Unlock()
		return re.Err

	case <-timer.C:
		return ErrOverTime
	}
}

func (kv *ShardKV) cloneShard(ConfigNum int, KvMap map[string]string) Shard {

	moveShard := Shard{
		KvMap:     make(map[string]string),
		ConfigNum: ConfigNum,
	}

	for k, v := range KvMap {
		moveShard.KvMap[k] = v
	}

	return moveShard
}
