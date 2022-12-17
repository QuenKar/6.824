package kvraft

import (
	"bytes"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.

	SeqId    int
	Key      string
	Value    string
	ClientId int64
	Index    int //from raft service
	OpType   string
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	seqMap       map[int64]int     //clientId / seqId
	waitChMap    map[int]chan Op   // cmd.Index / chan Op
	kvPersistMap map[string]string //Key / Value

	lastSnapshotIndex int
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	if kv.killed() || !kv.rf.IsLeader() {
		reply.Err = ErrWrongLeader
		return
	}

	cmd := Op{
		OpType:   "Get",
		SeqId:    args.SeqId,
		Key:      args.Key,
		ClientId: args.ClientId,
	}

	//start a command
	idx, _, _ := kv.rf.Start(cmd)

	ch := kv.getWaitCh(idx)
	defer func() {
		kv.mu.Lock()
		delete(kv.waitChMap, cmd.Index)
		kv.mu.Unlock()
	}()

	timer := time.NewTicker(100 * time.Millisecond)
	defer timer.Stop()

	select {
	case replyCmd := <-ch:
		if replyCmd.ClientId != cmd.ClientId || replyCmd.SeqId != cmd.SeqId {
			reply.Err = ErrWrongLeader
		} else {
			kv.mu.Lock()
			reply.Err = OK
			reply.Value = kv.kvPersistMap[args.Key]
			kv.mu.Unlock()
			return
		}
	case <-timer.C:
		//over time
		reply.Err = ErrWrongLeader
	}

}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.

	if kv.killed() || !kv.rf.IsLeader() {
		reply.Err = ErrWrongLeader
		return
	}

	cmd := Op{
		OpType:   args.Op,
		SeqId:    args.SeqId,
		ClientId: args.ClientId,
		Key:      args.Key,
		Value:    args.Value,
	}

	//start a command
	idx, _, _ := kv.rf.Start(cmd)

	ch := kv.getWaitCh(idx)
	defer func() {
		kv.mu.Lock()
		delete(kv.waitChMap, cmd.Index)
		kv.mu.Unlock()
	}()

	timer := time.NewTicker(100 * time.Millisecond)
	defer timer.Stop()

	select {
	case replyCmd := <-ch:
		if replyCmd.ClientId != cmd.ClientId || replyCmd.SeqId != cmd.SeqId {
			reply.Err = ErrWrongLeader
		} else {
			reply.Err = OK
		}
	case <-timer.C:
		//over time
		reply.Err = ErrWrongLeader
	}
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.seqMap = make(map[int64]int)
	kv.waitChMap = make(map[int]chan Op)
	kv.kvPersistMap = make(map[string]string)

	kv.lastSnapshotIndex = -1

	//if KVServer recovers from a crash
	snapshot := persister.ReadSnapshot()
	if len(snapshot) > 0 {
		kv.DecodeSnapShot(snapshot)
	}

	go kv.applyMsgHandler()

	return kv
}

func (kv *KVServer) applyMsgHandler() {
	for {
		if kv.killed() {
			return
		}

		applyMsg := <-kv.applyCh
		if applyMsg.CommandValid {

			if applyMsg.CommandIndex <= kv.lastSnapshotIndex {
				return
			}

			index := applyMsg.CommandIndex
			op := applyMsg.Command.(Op)

			if !kv.isDup(op.ClientId, op.SeqId) {
				kv.mu.Lock()
				switch op.OpType {
				case "Put":
					kv.kvPersistMap[op.Key] = op.Value
				case "Append":
					kv.kvPersistMap[op.Key] += op.Value
				}
				kv.seqMap[op.ClientId] = op.SeqId
				kv.mu.Unlock()
			}

			//keep snapshot into rf
			if kv.lastSnapshotIndex != -1 && kv.rf.GetRaftStateSize() > kv.maxraftstate {
				kv.rf.Snapshot(applyMsg.CommandIndex, kv.CreateSnapShot())
			}

			// send op to wait chan
			kv.getWaitCh(index) <- op
		}

		//有snapshot，decode it
		if applyMsg.SnapshotValid {
			kv.mu.Lock()

			kv.DecodeSnapShot(applyMsg.Snapshot)
			kv.lastSnapshotIndex = applyMsg.SnapshotIndex

			kv.mu.Unlock()
		}

	}
}

func (kv *KVServer) getWaitCh(index int) chan Op {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	ch, ok := kv.waitChMap[index]
	if !ok {
		kv.waitChMap[index] = make(chan Op, 1)
		ch = kv.waitChMap[index]
	}
	return ch
}

func (kv *KVServer) isDup(clientId int64, seqId int) bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	lastSeqId, ok := kv.seqMap[clientId]
	if !ok {
		return false
	}
	return seqId <= lastSeqId
}

func (kv *KVServer) DecodeSnapShot(snapshot []byte) {
	if snapshot == nil || len(snapshot) < 1 {
		return
	}

	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)

	var persistmap map[string]string
	var seqMap map[int64]int

	if d.Decode(&persistmap) == nil && d.Decode(&seqMap) == nil {
		kv.kvPersistMap = persistmap
		kv.seqMap = seqMap
	} else {
		fmt.Printf("server[%v]:Failed to decode snapshot!\n", kv.me)
	}
}

// CreateSnapShot:{kvPersistMap,seqMap}
func (kv *KVServer) CreateSnapShot() []byte {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.kvPersistMap)
	e.Encode(kv.seqMap)
	data := w.Bytes()
	return data
}
