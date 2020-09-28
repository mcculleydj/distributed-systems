package kvpaxos

import "net"
import "fmt"
import "net/rpc"
import "log"
import "paxos"
import "sync"
import "sync/atomic"
import "os"
import "syscall"
// import "encoding/gob"
import "math/rand"
import "time"
import "strconv"

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

//
// think of Op{} as a log entry
// this is the structure we choose as a proposal for the log
// this is the interface{} passed to Start as the parameter v
// we will need some sort of No-Op for lagging servers to catch up
//

type Op struct {
	Xid   int64  // transaction id necessary for at-most-once semantics
	Ty    string // operation type (Get, Put, or Append)
	Key   string // key
	Value string // value
}

//
// Future work: caching all RW replies strikes me as very memory
// consuming. Is there a better way to handle this?
//

type KVPaxos struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       int32                    // for testing
	unreliable int32                    // for testing
	px         *paxos.Paxos             // Paxos peer running on this server

	// Your definitions here.
	data	   map[string]string        // kv store
	reads      map[int64]GetReply       // cache of replies to gets in the log
	writes     map[int64]PutAppendReply // cache of replies to put/appends in the log
	top        int                      // the log is complete for all seq < top
}

//
// PrintLog() function for use in testing
// Requires a GetLen() function in paxos.go
//

// func (kv *KVPaxos) PrintLog() {
// 	fmt.Printf("Current Paxos log for SERVER-%v:\n", kv.me)
// 	for i := 0; i < kv.px.GetLen(); i++ {
// 		fmt.Printf("%v: %v\n", i, kv.px.GetInstance(i))
// 	}
// }

//
// propose(Op{}, int) -> Op{}, int
// propose an Op and wait for a decision on the next seq number
//
func (kv *KVPaxos) propose(proposal Op, seq int) (Op, int) {
	
	// make proposal by calling Start()
	me := kv.me
	kv.px.Start(seq, proposal)
	
	// listen by repeatedly calling Status()

	time_out := 10 * time.Millisecond

	// while loop that waits for consensus:
	for !kv.isdead() {
	    status, op := kv.px.Status(seq)
	    if status == paxos.Decided {	        
	        // must assert interface{} is of type Op{}
	        return op.(Op), seq         
	    }

	    time.Sleep(time_out)

		// had to comment this out to pass TestPartition	    
	    //   if time_out < 10 * time.Second {
	    //      time_out *= 2
	    //   }
	}

	panicString := "KV Server-" + strconv.Itoa(me) + " non-responsive..."
	panic(panicString)

	// Question: What happens if kv dies?
	// Question: What happens if kv.px dies? 
}

// logRead(Op, int) -> bool
// stores the read op in kv.reads
// should only store a reply if the entire log 
// up to this seq number has been applied to the data

func (kv *KVPaxos) logRead(op Op, seq int) bool {
	
	if seq == kv.top {
		val, ok := kv.data[op.Key]
		var reply GetReply

		if !ok {
			reply.Err = ErrNoKey
		} else {
			reply.Seq = seq
			reply.Xid = op.Xid
			reply.Value = val
			reply.Err = OK
		}
		
		kv.reads[op.Xid] = reply
		
		// Question: Is this the right place for Done()?
		kv.px.Done(seq)
		kv.top += 1

		return true
	}

	return false
}

// applyAndLogWrite(Op, int) -> bool
// applies and stores the write op in kv.writes
// should only store a reply if the entire log 
// up to this seq number has been applied to the data

func (kv *KVPaxos) applyAndLogWrite(op Op, seq int) bool {

	if seq == kv.top {
		var reply PutAppendReply

		if op.Ty == "Put" {
			kv.data[op.Key] = op.Value
		} else if op.Ty == "Append" {
			kv.data[op.Key] += op.Value
		}

		reply.Seq = seq
		reply.Xid = op.Xid
		reply.Err = OK
		kv.writes[op.Xid] = reply
		
		// Question: Is this the right place for Done?
		kv.px.Done(seq)
		kv.top += 1

		return true
	}

	return false
}

//
// Get RPC Handler
//
func (kv *KVPaxos) Get(args *GetArgs, reply *GetReply) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	xid  := args.Xid
	key  := args.Key
	xidAcks := args.AckXid

	// Question: We delete "stale" data here but,
	// how do we know that NO client will ever ask for it
	// again, just because one client acks the xid?

	for _, id := range xidAcks {
		delete(kv.reads, id)
	}

	// if: xid made it into the shared log, return the stored reply

	if _, ok := kv.reads[xid]; ok {
		reply.Seq = kv.reads[xid].Seq
		reply.Xid = kv.reads[xid].Xid
		reply.Err = kv.reads[xid].Err
		reply.Value = kv.reads[xid].Value
		return nil
	}

	// else: xid not found, create a Get proposal for kv.px

	proposal := Op { xid,
	                 "Get",
	                 key,
	                 "" }

	// put the proposal on the wire and wait for a response
	// note: kv.top will keep trying the next available seq #
    // Get() is called in an indefinite loop by the client

	decided_op, seq := kv.propose(proposal, kv.top)

	// cache and apply decision

	var ok bool

	if decided_op.Ty == "Get" {
		ok = kv.logRead(decided_op, seq)
	} else if decided_op.Ty == "Put" || decided_op.Ty == "Append" {
		ok = kv.applyAndLogWrite(decided_op, seq)
	}

	// if seq != top, server is behind
	// make empty proposals using Op{} to catch up

	for !ok {
		decided_op, seq = kv.propose(Op{}, kv.top)
		if decided_op.Ty == "Get" {
			ok = kv.logRead(decided_op, seq)
		} else if decided_op.Ty == "Put" || decided_op.Ty == "Append" {
			ok = kv.applyAndLogWrite(decided_op, seq)
		}
	}

	return nil
}

//
// PutAppend RPC Handler
//
func (kv *KVPaxos) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	xid := args.Xid
	key := args.Key
	val := args.Value
	op  := args.Op          // either "Put" or "Append" not to be confused with Op{}
	xidAcks := args.AckXid

	// forget stale data (see question under Get())

	for _, id := range xidAcks {
		delete(kv.writes, id)
	}

	// if: xid made it into the shared log, return the stored reply

	if _, ok := kv.writes[xid]; ok {		
		reply.Seq = kv.writes[xid].Seq
		reply.Xid = kv.writes[xid].Xid
		reply.Err = kv.writes[xid].Err
		return nil
	}

	// else: xid not found, create a PutAppend proposal for kv.px

	proposal := Op { xid,
		    	     op,
		    	     key,
		    	     val }

	// put proposal on the wire and wait for a response  	     

	decided_op, seq := kv.propose(proposal, kv.top)

	// cache and apply decision

	var ok bool

	if decided_op.Ty == "Get" {
		ok = kv.logRead(decided_op, seq)
	} else if decided_op.Ty == "Put" || decided_op.Ty == "Append" {
		ok = kv.applyAndLogWrite(decided_op, seq)
	}

	// if seq != top, server is behind
	// make empty proposals using Op{} to catch up

	for !ok {
		decided_op, seq = kv.propose(Op{}, kv.top)
		if decided_op.Ty == "Get" {
			ok = kv.logRead(decided_op, seq)
		} else if decided_op.Ty == "Put" || decided_op.Ty == "Append" {
			ok = kv.applyAndLogWrite(decided_op, seq)
		}
	}

	return nil
}

// tell the server to shut itself down
// please do not change these two functions
func (kv *KVPaxos) kill() {
	DPrintf("Kill(%d): die\n", kv.me)
	atomic.StoreInt32(&kv.dead, 1)
	kv.l.Close()
	kv.px.Kill()
}

// call this to find out if the server is dead
func (kv *KVPaxos) isdead() bool {
	return atomic.LoadInt32(&kv.dead) != 0
}

// please do not change these two functions
func (kv *KVPaxos) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&kv.unreliable, 1)
	} else {
		atomic.StoreInt32(&kv.unreliable, 0)
	}
}

func (kv *KVPaxos) isunreliable() bool {
	return atomic.LoadInt32(&kv.unreliable) != 0
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
//
func StartServer(servers []string, me int) *KVPaxos {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall
	// gob.Register(Op{})

	// Question: What does gob.Register(Op{}) do for us exactly?
	// It doesn't appear that my code uses this feature, but maybe
	// it does use it under the hood.

	kv := new(KVPaxos)
	kv.me = me
	kv.data = make(map[string]string)
	kv.reads = make(map[int64]GetReply)
	kv.writes = make(map[int64]PutAppendReply)
	kv.top = 0

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
				fmt.Printf("KVPaxos(%v) accept: %v\n", me, err.Error())
				kv.kill()
			}
		}
	}()

	return kv
}
