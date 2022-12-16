package kvraft

import (
	"crypto/rand"
	"math/big"
	mrand "math/rand"

	"6.824/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	seqId    int
	leaderId int
	clientId int64
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.clientId = nrand()
	ck.leaderId = mrand.Intn(len(servers))
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	ck.seqId++
	args := GetArgs{
		Key:      key,
		ClientId: ck.clientId,
		SeqId:    ck.seqId,
	}

	sid := ck.leaderId

	for {
		reply := GetReply{}

		ok := ck.servers[sid].Call("KVServer.Get", &args, &reply)

		if ok {
			switch reply.Err {
			case ErrNoKey:
				ck.leaderId = sid
				return ""
			case ErrWrongLeader:
				//这里可以让节点直接告诉leader是谁
				sid = (sid + 1) % len(ck.servers)
				continue
			case OK:
				ck.leaderId = sid
				return reply.Value
			}
		}
		//sid server crash!尝试别的server
		sid = (sid + 1) % len(ck.servers)
	}

}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	ck.seqId++
	args := PutAppendArgs{
		Key:      key,
		Value:    value,
		Op:       op,
		ClientId: ck.clientId,
		SeqId:    ck.seqId,
	}

	sid := ck.leaderId

	for {
		reply := PutAppendReply{}

		ok := ck.servers[sid].Call("KVServer.PutAppend", &args, &reply)

		if ok {
			switch reply.Err {
			case ErrWrongLeader:
				//这里可以让节点直接告诉leader是谁
				sid = (sid + 1) % len(ck.servers)
				continue
			case OK:
				ck.leaderId = sid
				return
			}
		}

		sid = (sid + 1) % len(ck.servers)
	}

}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
