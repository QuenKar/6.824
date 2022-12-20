package shardkv

//
// Sharded key/value server.
// Lots of replica groups, each running Raft.
// Shardctrler decides which group serves each shard.
// Shardctrler may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

type Err string

const (
	OK                  = "OK"
	ErrNoKey            = "ErrNoKey"
	ErrWrongGroup       = "ErrWrongGroup"
	ErrWrongLeader      = "ErrWrongLeader"
	ShardNotArrived     = "ShardNotArrived"
	ConfigNotArrived    = "ConfigNotArrived"
	ErrInconsistentData = "ErrInconsistentData"
	ErrOverTime         = "ErrOverTime"
)

type OpType string

const (
	PutType         OpType = "Put"
	GetType         OpType = "Get"
	AppendType      OpType = "Append"
	UpConfigType    OpType = "UpConfig"
	AddShardType    OpType = "AddShard"
	RemoveShardType OpType = "RemoveShard"
)

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key   string
	Value string
	Op    OpType // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClientId  int64
	RequestId int
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	ClientId  int64
	RequestId int
}

type GetReply struct {
	Err   Err
	Value string
}

type SendShardArg struct {
	LastAppliedRequestId map[int64]int // for receiver to update its state
	ShardId              int
	Shard                Shard // Shard to be sent
	ClientId             int64
	RequestId            int
}

type AddShardReply struct {
	Err Err
}
