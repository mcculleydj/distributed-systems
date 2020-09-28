package diskv

import (
	"crypto/rand"
	"fmt"
	"math/big"
	"net/rpc"
	"shardmaster"
	"sync"
	"time"
)

type Clerk struct {
	mu     sync.Mutex
	sm     *shardmaster.Clerk
	config shardmaster.Config
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(shardmasters []string) *Clerk {
	ck := new(Clerk)
	ck.sm = shardmaster.MakeClerk(shardmasters)
	return ck
}

//
// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be a pointer
// to a reply structure.
//
// the return value is true if the server responded, and false
// if call() was not able to contact the server. in particular,
// the reply's contents are only valid if call() returned true.
//
// you should assume that call() will return an
// error after a while if the server is dead.
// don't provide your own time-out mechanism.
//
// please use call() to send all RPCs, in client.go and server.go.
// please don't change this function.
//

func call(srv string, rpcname string,
	args interface{}, reply interface{}) bool {
	c, errx := rpc.Dial("unix", srv)
	if errx != nil {
		return false
	}
	defer c.Close()

	err := c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

func key2shard(key string) int {
	shard := 0
	if len(key) > 0 {
		shard = int(key[0])
	}
	shard %= shardmaster.NShards
	return shard
}

func (ck *Clerk) Get(key string) string {
	ck.mu.Lock()         // allow one RPC
	defer ck.mu.Unlock() // unlock upon completion

	xid := nrand() // assign transaction id

	for { // while:
		shard := key2shard(key)                  //    map key -> shard
		gid := ck.config.Shards[shard]           //    map shard -> gid
		servers, exists := ck.config.Groups[gid] //    map gid -> []servers

		if exists { //    if a set of servers exists for this gid:
			for _, s := range servers { //       for each server in servers:
				args := &GetArgs{}             //   	    instantiate args pointer
				args.Xid = xid                 // 			assign xid
				args.Key = key                 //			assign key
				args.ConfigNum = ck.config.Num // 			assign config number
				reply := &GetReply{}           // 			instantiate reply pointer

				// try RPC
				ok := call(s, "DisKV.Get", args, reply)

				if ok && (reply.Err == OK || reply.Err == ErrNoKey) {
					return reply.Value
				}

				if ok && reply.Err == ErrConfigSync { //          if client and server out of sync:
					break //          	break loop to query SM
				}

				// reply.Err should never be empty - catch exception
				if ok && reply.Err == "" {
					fmt.Printf("\nGet(%v) reply empty...\n", key)
				}
			}
		}

		// query SM for a new configuration after a short pause
		time.Sleep(50 * time.Millisecond)
		ck.config = ck.sm.Query(-1)
	} // end while
} // end Get()

func (ck *Clerk) PutAppend(key string, value string, op string) {
	ck.mu.Lock()         // allow one RPC
	defer ck.mu.Unlock() // unlock upon completion

	xid := nrand() // assign transaction id

	for { // while:
		shard := key2shard(key)                  //    map key -> shard
		gid := ck.config.Shards[shard]           //    map shard -> gid
		servers, exists := ck.config.Groups[gid] //    map gid -> []servers

		if exists { //    if a set of servers exists for this gid:
			for _, s := range servers { //       for each server in servers:
				args := &PutAppendArgs{}       //   	    instantiate args pointer
				args.Xid = xid                 // 			assign xid
				args.Key = key                 //			assign key
				args.Value = value             // 			assign value
				args.Op = op                   //          assign op
				args.ConfigNum = ck.config.Num // 			assign config number
				reply := &PutAppendReply{}     // 			instantiate reply pointer

				// try RPC
				ok := call(s, "DisKV.PutAppend", args, reply)

				if ok && reply.Err == OK {
					return
				}

				if ok && reply.Err == ErrConfigSync { //          if client and server out of sync:
					break //          	break loop to query SM
				}

				// reply.Err should never be empty; catch exception
				if ok && reply.Err == "" {
					fmt.Printf("\nPutAppend(%v, %v) reply empty...\n", key, value)
				}
			}
		}

		// query SM for a new configuration after a short pause
		time.Sleep(50 * time.Millisecond)
		ck.config = ck.sm.Query(-1)
	} // end while
} // end PutAppend()

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}

func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}

// EOF
