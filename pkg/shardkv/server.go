package shardkv

import "net"
import "fmt"
import "net/rpc"
import "log"
import "time"
import "paxos"
import "sync"
import "sync/atomic"
import "os"
import "syscall"
import "encoding/gob"
import "math/rand"
import "shardmaster"

type ShardKV struct {
	mu         sync.Mutex							// Mutex lock
	l          net.Listener 						// RPC listener
	me         int 									// index into servers
	dead       int32 								// for testing
	unreliable int32 								// for testing
	sm         *shardmaster.Clerk 					// shardmaster client
	px         *paxos.Paxos 						// paxos client
	gid        int64                               	// replica group ID
	servers    []string 							// paxos peers
	config	   shardmaster.Config                   // current config
	data	   map[string]string            	   	// kv store
	reads      map[int64]GetReply            	   	// cached replies to gets in the log
	writes     map[int64]PutAppendReply            	// cached replies to put/appends in the log
	receives   map[ShardId]ReceiveShardReply  	    // cached replies to receive shards in the log
	top        int                               	// the log is complete for all seq < top
	next       int                                  // working on reaching this config
}

type Op struct {
	Xid    int64                      // transaction id necessary for at-most-once semantics
	Ty     string                     // operation type (Get, Put, Append, Advance, or Receive)
	Key    string                     // key   (for reads/writes)
	Value  string                     // value (for writes)
	Sid    ShardId 				      // shard id
	Data   map[string]string          // transfer shard data
	Reads  map[int64]GetReply         // transfer reads cache
	Writes map[int64]PutAppendReply   // transfer writes cache
	Config shardmaster.Config         // next config
}

// RPC Handlers [Get, PutAppend, ReceiveShard]:

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) error {

	// lock and begin handler

	kv.mu.Lock()
	fmt.Printf("Get(%v) RPC arrived @ Svr %v-%v... locking.\n", args.Key, kv.gid, kv.me)
	defer func() {
		fmt.Printf("Get(%v) RPC handled by Svr %v-%v... unlocking.\n", args.Key, kv.gid, kv.me)
		kv.mu.Unlock()
	}()

	// if this xid was logged, return the stored reply

	if r, ok := kv.reads[args.Xid]; ok {
		reply.Value = r.Value
		reply.Err   = r.Err
		fmt.Printf("Srv %v-%v FOUND Get(%v) in cache... sending value: %v.\n", 
			       kv.gid, kv.me, args.Key, reply.Value)
		return nil
	}

	// check config sync

	if args.ConfigNum != kv.config.Num {
		fmt.Printf("Srv %v-%v is NOT IN SYNC with client, rejecting Get(%v) RPC.\n", kv.gid, kv.me, args.Key)
		fmt.Printf("Srv %v-%v on configuration %v... client trying to Get for configuration %v.\n",
			       kv.gid, kv.me, kv.config.Num, args.ConfigNum)
		reply.Err = ErrConfigSync
		return nil
	}

	if kv.next != kv.config.Num {
		fmt.Printf("Srv %v-%v is busy with a configuration change, rejecting Get(%v) RPC.\n", kv.gid, kv.me, args.Key)
		reply.Err = ErrConfigSync
		return nil
	}

	fmt.Printf("Get(%v) is not in the cache... Srv %v-%v making a Get proposal for seq %v.\n", args.Key, kv.gid, kv.me, kv.top)
	reply.Err = ErrNotInCache

	// create a Get proposal for kv.px

	proposal := Op { args.Xid, "Get", args.Key, "", ShardId{}, nil, nil, nil, shardmaster.Config{} }
	               
	// put the proposal on the wire

	kv.propose(proposal, kv.top)
	
	return nil
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {

	// lock and begin handler

	kv.mu.Lock()
	fmt.Printf("%v(%v, %v) RPC arrived @ Svr %v-%v... locking.\n", args.Op, args.Key, args.Value, kv.gid, kv.me)
	defer func() {
		fmt.Printf("%v(%v, %v) RPC handled by Srv %v-%v... unlocking.\n", args.Op, args.Key, args.Value, kv.gid, kv.me)
		kv.mu.Unlock()
	}()

	// if this xid was logged, return the stored reply

	if r, ok := kv.writes[args.Xid]; ok {		
		reply.Err = r.Err
		fmt.Printf("Srv %v-%v FOUND %v(%v, %v) in cache...\n", 
			       kv.gid, kv.me, args.Op, args.Key, args.Value)
		return nil
	}

	// check config sync

	if args.ConfigNum != kv.config.Num {
		fmt.Printf("Srv %v-%v is NOT IN SYNC with client, rejecting %v(%v, %v) RPC.\n", 
			       kv.gid, kv.me, args.Op, args.Key, args.Value)
		fmt.Printf("Srv %v-%v on configuration %v... client trying to PutAppend for configuration %v.\n", 
			       kv.gid, kv.me, kv.config.Num, args.ConfigNum)
		reply.Err = ErrConfigSync
		return nil
	}

	if kv.next != kv.config.Num {
		fmt.Printf("Srv %v-%v is busy with a configuration change, rejecting %v(%v, %v) RPC.\n", 
			       kv.gid, kv.me, args.Op, args.Key, args.Value)
		reply.Err = ErrConfigSync
		return nil
	}

	fmt.Printf("%v(%v, %v) is not in the cache... Srv %v-%v making a %v proposal for seq %v.\n",
		       args.Op, args.Key, args.Value, kv.gid, kv.me, args.Op, kv.top)
	reply.Err = ErrNotInCache

	// create a PutAppend proposal for kv.px

	proposal := Op { args.Xid, args.Op, args.Key, args.Value, ShardId{}, nil, nil, nil, shardmaster.Config{} }
	               
	// put the proposal on the wire

	kv.propose(proposal, kv.top)

	return nil
}

func (kv *ShardKV) ReceiveShard(args *ReceiveShardArgs, reply *ReceiveShardReply) error {

	// lock and begin handler

	kv.mu.Lock()
	fmt.Printf("ReceiveShard(%v) RPC arrived @ Srv %v-%v... locking.\n", args.Sid, kv.gid, kv.me)
	defer func() {
		fmt.Printf("ReceiveShard(%v) RPC handled by Srv %v-%v... unlocking.\n", args.Sid, kv.gid, kv.me)
		kv.mu.Unlock()
	}()

	fmt.Printf("ReceiveShard: Srv %v-%v's config data [C: %v, N: %v].\n", kv.gid, kv.me, kv.config.Num, kv.next)

	// if this ShardId was received, return the stored reply

	if r, ok := kv.receives[args.Sid]; ok {
		reply.Err = r.Err
		fmt.Printf("Srv %v-%v FOUND ReceiveShard(%v) in cache...\n", kv.gid, kv.me, args.Sid)
		return nil
	}

	if kv.next != kv.config.Num + 1 || kv.config.Num != args.Sid.ConfigNum {
		reply.Err = ErrConfigSync
		fmt.Printf("Srv %v-%v NOT IN SYNC [C: %v, N: %v]... rejecting ReceiveShard(%v).\n", 
			       kv.gid, kv.me, kv.config.Num, kv.next, args.Sid)
		return nil
	}

	fmt.Printf("ReceiveShard(%v) was not in cache... Srv %v-%v making a Receive proposal for seq %v.\n", 
		       args.Sid, kv.gid, kv.me, kv.top)
	reply.Err = ErrNotInCache

	// create a Receive proposal for kv.px

	proposal := Op { 0, "Receive", "", "", args.Sid, args.Data, args.Reads, args.Writes, shardmaster.Config{} }

	// put proposal on the wire

	kv.propose(proposal, kv.top)
	
	return nil
}

func (kv *ShardKV) propose(proposal Op, seq int) {
	kv.px.Start(seq, proposal)
}

// Execute Op [Get, Put, Append, Receive, Advance]

func (kv *ShardKV) execute(op Op, seq int) {
	ty := op.Ty

	// print some information about this execute call:

	if ty == "Get" {
		fmt.Printf("\tSrv %v-%v EXECUTING(op: %v(%v), seq: %v).\n", kv.gid, kv.me, ty, op.Key, seq)
	} else if ty == "Put" || ty == "Append" {
		fmt.Printf("\tSrv %v-%v EXECUTING(op: %v(%v, %v), seq: %v).\n", kv.gid, kv.me, ty, op.Key, op.Value, seq)
	} else if ty == "Receive" {
		fmt.Printf("\tSrv %v-%v EXECUTING(op: %v(%v), seq: %v).\n", kv.gid, kv.me, ty, op.Sid, seq)
	} else if ty == "Advance" {
		fmt.Printf("\tSrv %v-%v EXECUTING(op: %v(%v), seq: %v).\n", kv.gid, kv.me, ty, op.Config.Num, seq)
	}

	// apply writes (as applicable) and cache reply

	if ty == "Get" {
		kv.logRead(op, seq)
	} else if ty == "Put" || ty == "Append" {
		kv.applyAndLogWrite(op, seq)
	} else if ty == "Receive" {
		kv.applyAndLogReceive(op, seq)
	} else if ty == "Advance" {
		kv.applyAdvance(op, seq)
	}
}

func (kv *ShardKV) logRead(op Op, seq int) {
	var reply GetReply

	value, ok := kv.data[op.Key]

	if !ok {
		reply.Err = ErrNoKey
	} else {
		reply.Value = value
		reply.Err = OK
	}
	
	kv.reads[op.Xid] = reply
	kv.px.Done(seq)
	kv.top += 1
}

func (kv *ShardKV) applyAndLogWrite(op Op, seq int) {
	var reply PutAppendReply

	if op.Ty == "Put" {
		kv.data[op.Key] = op.Value
	} else if op.Ty == "Append" {
		kv.data[op.Key] += op.Value
	}

	reply.Err = OK
	kv.writes[op.Xid] = reply
	kv.px.Done(seq)
	kv.top += 1
}

func (kv *ShardKV) applyAndLogReceive(op Op, seq int) {
	var reply ReceiveShardReply

	for k, v := range op.Data {     // is it possible to overwrite more current data?
		kv.data[k] = v
	}

	for k, v := range op.Reads {
		kv.reads[k] = v
	}

	for k, v := range op.Writes {
		kv.writes[k] = v
	}

	reply.Err = OK
	kv.receives[op.Sid] = reply
	kv.px.Done(seq)
	kv.top += 1
}

func (kv *ShardKV) applyAdvance(op Op, seq int) {

	currentConfig := kv.config
	nextConfig := op.Config

	// is should be the case that C# == N#

	fmt.Printf("Srv %v-%v trying to advance to configuration %v... Config Data [C: %v, N: %v].\n",
		       kv.gid, kv.me, op.Config.Num, currentConfig.Num, kv.next)

	// if the target config is 1 ahead increment top so that Receives can populate the log
	// ahead of this Advance and increment next to signal that this server is trying to reconfig

	if kv.next == nextConfig.Num - 1 {
		kv.next += 1
		kv.top += 1
	} else {
		fmt.Printf("\n\tSrv %v-%v tried to execute applyAdvance where C != N.\n", kv.gid, kv.me)
		return
	}

	currentShards := currentConfig.Shards
	nextShards := nextConfig.Shards

	toSend := make([]ShardId, 0, NShards)
	toReceive := make([]ShardId, 0, NShards) 

	for i := 0; i < NShards; i++ {
		if currentShards[i] == kv.gid && nextShards[i] != kv.gid {
			fmt.Printf("Srv %v-%v must send shard %v to group %v.\n", 
				       kv.gid, kv.me, ShardId{kv.config.Num, i}, nextShards[i])
			toSend = append(toSend, ShardId{currentConfig.Num, i})
		}
	}

	for i := 0; i < NShards; i++ {
		if currentShards[i] != 0 && currentShards[i] != kv.gid && nextShards[i] == kv.gid {
			fmt.Printf("Srv %v-%v must receive shard %v from group %v.\n", 
				       kv.gid, kv.me, ShardId{kv.config.Num, i}, currentShards[i])
			toReceive = append(toReceive, ShardId{currentConfig.Num, i})
		}
	}

	count := 0

	for len(toSend) > 0 || len(toReceive) > 0 {

		for i := 0; i < len(toSend); i++ {
			servers, exists := nextConfig.Groups[nextShards[toSend[i].Shard]]
			if exists {
				if ok := kv.sendShard(toSend[i], servers, nextShards[toSend[i].Shard]); ok {
					toSend = append(toSend[0:i], toSend[i+1:]...)
				}
			} else {
				fmt.Printf("\n\tSrv %v-%v tried to send a shard to an INVALID group...\n", kv.gid, kv.me)
			}
		}
		
		fmt.Printf("Srv %v-%v attempting to receive shards... toSend: %v, toReceive: %v.\n", kv.gid, kv.me, toSend, toReceive)
		
		kv.mu.Unlock()
		time.Sleep(time.Duration(60 + rand.Intn(60)) * time.Millisecond)
		kv.mu.Lock()

		status, op := kv.px.Status(kv.top) 		
		if status == paxos.Decided {
	    	decided_op := op.(Op)
	    	if decided_op.Ty == "Receive" {
	    		kv.execute(decided_op, kv.top)
	    	}
		}

		for i := 0; i < len(toReceive); i++ {
			if _, ok := kv.receives[toReceive[i]]; ok {
				toReceive = append(toReceive[0:i], toReceive[i+1:]...)
			}
		}

		// fmt.Printf("Srv %v-%v finished checking receives for incoming shards... updated toReceive: %v.\n",
		// 	       kv.gid, kv.me, toReceive)

		if !(len(toSend) > 0 && len(toReceive) > 0) {
			fmt.Printf("Srv %v-%v iterating sync loop count: %v... toSend: %v, toReceive: %v.\n", 
				       kv.gid, kv.me, count, toSend, toReceive)
			count += 1
		}
	}

	kv.config = nextConfig
	fmt.Printf("\tSrv %v-%v has ADVANCED to configuration %v.\n", kv.gid, kv.me, kv.config.Num)

	// could print paxos log here to check for inconsistencies
	kv.px.Done(seq)
}

func (kv *ShardKV) sendShard(sid ShardId, servers []string, gid int64) bool {

	fmt.Printf("Srv %v-%v attempting to send shard %v to group %v.\n", kv.gid, kv.me, sid, gid)

	args := &ReceiveShardArgs{}
	args.Sid = sid
	args.Data = make(map[string]string)
	args.Reads = make(map[int64]GetReply)
	args.Writes = make(map[int64]PutAppendReply)
	reply := &ReceiveShardReply{}

	// package shard data, reads, and writes
	for k, v := range kv.data {
		if key2shard(k) == sid.Shard {
			args.Data[k] = v
		}
	}

	for k, v := range kv.reads {
		args.Reads[k] = v
	}

	for k, v := range kv.writes {
		args.Writes[k] = v
	}

	for _, s := range servers {
	
		ok := call(s, "ShardKV.ReceiveShard", args, reply)


		// trying to speed things up
		
		// if ok && reply.Err == ErrConfigSync {
		// 	time.Sleep(100 * time.Millisecond)
		// }

		// should be an exception if call() succeeds
		if ok && reply.Err == "" {
			fmt.Printf("\nRecieveShard(%v) reply empty...\n", args.Sid)
		}

		if ok && reply.Err == OK {
			fmt.Printf("Srv %v-%v's attempt to send shard %v to group %v was successful.\n", kv.gid, kv.me, sid, gid)
			return true
		}
	}
	fmt.Printf("Srv %v-%v's attempt to send shard %v to group %v failed.\n", kv.gid, kv.me, sid, gid)
	return false
}

func (kv *ShardKV) proposeAdvance(nextConfig shardmaster.Config) {
	proposal := Op { 0, "Advance", "", "", ShardId{}, nil, nil, nil, nextConfig }
	kv.propose(proposal, kv.top)
}

func (kv *ShardKV) tick() {
	kv.mu.Lock()

	// fmt.Printf("Srv %v-%v TICKED...\n", kv.gid, kv.me)

	// execute the operation at the top of the log
	status, op := kv.px.Status(kv.top) 		
	if status == paxos.Decided {
	    decided_op := op.(Op)
    	kv.execute(decided_op, kv.top) 
	}

	qconfig := kv.sm.Query(-1)                         // query the SM
	if kv.config.Num < qconfig.Num {			       // if this server is behind
		nextConfig := kv.sm.Query(kv.config.Num + 1)   // query for next config
		// fmt.Printf("Srv %v-%v lagging [C: %v, Q:%v]... calling proposeAdvance(%v).\n",
		// 	       kv.gid, kv.me, kv.config.Num, qconfig.Num, nextConfig.Num)
		kv.proposeAdvance(nextConfig)             	   // propose advance op to the replica group
	} else {
		// fmt.Printf("Srv %v-%v server up-to-date.\n", kv.gid, kv.me)
	}
	kv.mu.Unlock()
}

// tell the server to shut itself down.
// please don't change these two functions.
func (kv *ShardKV) kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.l.Close()
	kv.px.Kill()
}

// call this to find out if the server is dead.
func (kv *ShardKV) isdead() bool {
	return atomic.LoadInt32(&kv.dead) != 0
}

// please do not change these two functions.
func (kv *ShardKV) Setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&kv.unreliable, 1)
	} else {
		atomic.StoreInt32(&kv.unreliable, 0)
	}
}

func (kv *ShardKV) isunreliable() bool {
	return atomic.LoadInt32(&kv.unreliable) != 0
}

func StartServer(gid int64, shardmasters []string,
	servers []string, me int) *ShardKV {

	gob.Register(Op{})

	kv := new(ShardKV)

	kv.me        = me
	kv.gid       = gid
	kv.sm        = shardmaster.MakeClerk(shardmasters)
	kv.servers   = servers
	kv.config    = kv.sm.Query(-1)
	kv.data      = make(map[string]string)
	kv.reads     = make(map[int64]GetReply)
	kv.writes    = make(map[int64]PutAppendReply)
	kv.receives  = make(map[ShardId]ReceiveShardReply)
	kv.top       = 0
	kv.next      = 0

	// Don't call Join().

	rpcs := rpc.NewServer()
	rpcs.Register(kv)

	kv.px = paxos.Make(servers, me, rpcs)

	os.Remove(servers[me])
	l, e := net.Listen("unix", servers[me])
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	kv.l = l

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for kv.isdead() == false {
			conn, err := kv.l.Accept()
			if err == nil && kv.isdead() == false {
				if kv.isunreliable() && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if kv.isunreliable() && (rand.Int63()%1000) < 200 {
					// process the request but force discard of reply.
					c1 := conn.(*net.UnixConn)
					f, _ := c1.File()
					err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
					if err != nil {
						fmt.Printf("shutdown: %v\n", err)
					}
					go rpcs.ServeConn(conn)
				} else {
					go rpcs.ServeConn(conn)
				}
			} else if err == nil {
				conn.Close()
			}
			if err != nil && kv.isdead() == false {
				fmt.Printf("ShardKV(%v) accept: %v\n", me, err.Error())
				kv.kill()
			}
		}
	}()

	go func() {
		for kv.isdead() == false {
			kv.tick()
			time.Sleep(250 * time.Millisecond)
		}
	}()

	return kv
}
