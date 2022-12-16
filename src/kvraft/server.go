package kvraft

import (
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

	go kv.applyMsgHandler()

	return kv
}

func (kv *KVServer) applyMsgHandler() {
	for {
		if kv.killed() {
			return
		}

		applyMsg := <-kv.applyCh
		index := applyMsg.CommandIndex
		op := applyMsg.Command.(Op)

		if !kv.isDuplicate(op.ClientId, op.SeqId) {
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

		kv.getWaitCh(index) <- op

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

func (kv *KVServer) isDuplicate(clientId int64, seqId int) bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	lastSeqId, exist := kv.seqMap[clientId]
	if !exist {
		return false
	}
	return seqId <= lastSeqId
}
