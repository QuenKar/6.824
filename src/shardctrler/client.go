package shardctrler

//
// Shardctrler clerk.
//

import (
	"crypto/rand"
	"math/big"
	mrand "math/rand"
	"time"

	"6.824/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// Your data here.
	seqId    int
	clientId int64
	leaderId int
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
	// Your code here.
	ck.clientId = nrand()
	ck.leaderId = mrand.Intn(len(ck.servers))
	return ck
}

func (ck *Clerk) Query(num int) Config {
	ck.seqId++
	// Your code here.
	args := QueryArgs{
		Num:      num,
		SeqId:    ck.seqId,
		ClientId: ck.clientId,
	}

	sid := ck.leaderId

	for {
		// try each known server.
		reply := QueryReply{}

		ok := ck.servers[sid].Call("ShardCtrler.Query", &args, &reply)

		if ok {
			if reply.Err == OK && reply.WrongLeader == false {
				ck.leaderId = sid
				return reply.Config
			} else if reply.WrongLeader == true {
				//try other servers
				sid = (sid + 1) % len(ck.servers)
				continue
			}
		}
		//server crashed
		sid = (sid + 1) % len(ck.servers)
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	ck.seqId++
	// Your code here.
	args := JoinArgs{
		Servers:  servers,
		SeqId:    ck.seqId,
		ClientId: ck.clientId,
	}

	sid := ck.leaderId
	for {
		// try each known server.
		reply := JoinReply{}

		ok := ck.servers[sid].Call("ShardCtrler.Join", &args, &reply)

		if ok {
			if reply.Err == OK && reply.WrongLeader == false {
				ck.leaderId = sid
				return
			} else if reply.WrongLeader == true {
				//try other servers
				sid = (sid + 1) % len(ck.servers)
				continue
			}
		}
		//server crashed
		sid = (sid + 1) % len(ck.servers)
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Leave(gids []int) {
	ck.seqId++
	// Your code here.
	args := LeaveArgs{
		GIDs:     gids,
		SeqId:    ck.seqId,
		ClientId: ck.clientId,
	}
	sid := ck.leaderId
	for {
		// try each known server.
		reply := LeaveReply{}

		ok := ck.servers[sid].Call("ShardCtrler.Leave", &args, &reply)

		if ok {
			if reply.Err == OK && !reply.WrongLeader {
				ck.leaderId = sid
				return
			} else if reply.WrongLeader {
				//try other servers
				sid = (sid + 1) % len(ck.servers)
				continue
			}
		}
		//server crashed
		sid = (sid + 1) % len(ck.servers)
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	// Your code here.
	ck.seqId++
	args := MoveArgs{
		Shard:    shard,
		GID:      gid,
		SeqId:    ck.seqId,
		ClientId: ck.clientId,
	}
	sid := ck.leaderId
	for {
		// try each known server.
		reply := MoveReply{}

		ok := ck.servers[sid].Call("ShardCtrler.Move", &args, &reply)

		if ok {
			if reply.Err == OK && reply.WrongLeader == false {
				ck.leaderId = sid
				return
			} else if reply.WrongLeader == true {
				//try other servers
				sid = (sid + 1) % len(ck.servers)
				continue
			}
		}
		//server crashed
		sid = (sid + 1) % len(ck.servers)
		time.Sleep(100 * time.Millisecond)
	}
}
