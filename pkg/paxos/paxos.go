package paxos

import "net"
import "net/rpc"
import "log"
import "os"
import "syscall"
import "sync"
import "sync/atomic"
import "fmt"
import "math/rand"

type Fate int

const (
    Decided   Fate = iota + 1
    Pending                     // not yet decided.
    Forgotten                   // decided but forgotten.
)

// Credit to Dan Schatzberg for the ProposalId{} struct
// and associated functions, with the exception of Next()

type ProposalId struct {
    Proposal int
    Me       int
}

type Instance struct {
    Np      ProposalId  // highest prepare seen
    Na      ProposalId  // highest accept seen
    Va      interface{} // highest accept value
    Fate    Fate        // fate of the instance
}

//
// Future work: Replace the map data structure with
// a slice, because for a sequential log it is more
// appropriate and could allow for localized locking
//

type Paxos struct {
    mu         sync.Mutex
    l          net.Listener
    dead       int32            // for testing
    unreliable int32            // for testing
    rpcCount   int32            // for testing
    peers      []string         // ports of known paxos peers
    me         int              // index into peers[]
    Instances  map[int]Instance // log of events (stored as a hash)
    done       map[int]int      // knowledge of other server's latest Done()
    maxSeq     int              // highest completed sequence #
}

//
// RPC packets:
//

type PrepareArgs struct {
    Seq int
    N   ProposalId
}

type PrepareReply struct {
    Na   ProposalId             // reply with instance.na         
    Va   interface{}            // reply with instance.va
    Done int                    // piggyback Done
    Ok   bool                   // accept or reject
}

type AcceptArgs struct {
    Seq int                          
    N   ProposalId                     
    V   interface{}
}

type AcceptReply struct {
    N    ProposalId             // reply with either args.N or instance.np
    Done int                    // piggyback Done
    Ok   bool                   // accept or reject
}

type DecideArgs struct {
    Seq int                
    N   ProposalId              // decided on proposal number
    Va  interface{}             // decided on value
}

type DecideReply struct {
    Done int                    // piggyback Done
    Ok   bool                   // accepted
}

//
// ProposalId functions
//

func (pi *ProposalId) Greater(other ProposalId) bool {
    return pi.Proposal > other.Proposal ||
        (pi.Proposal == other.Proposal && pi.Me > other.Me)
}

func (pi *ProposalId) Equal(other ProposalId) bool {
    return pi.Proposal == other.Proposal && pi.Me == other.Me
}

func (pi *ProposalId) Geq(other ProposalId) bool {
    return pi.Greater(other) || pi.Equal(other)
}

func (pi *ProposalId) Next(base ProposalId) ProposalId {
    return ProposalId{base.Proposal + 1, pi.Me}
}

func NullProposal() ProposalId {
    return ProposalId { -1, -1 }
}

//
// Return the instance mapped to this seq #
// If one does not exist instantiate a new instance
//

func (px *Paxos) getInstance(seq int) Instance {
    px.mu.Lock()
    defer px.mu.Unlock()

    if _, ok := px.Instances[seq]; !ok {
        px.Instances[seq] = Instance { NullProposal(), 
                                       NullProposal(), 
                                       nil, 
                                       Pending }
        if seq > px.maxSeq {
            px.maxSeq = seq
        }
    }

    px.freeMemory()

    return px.Instances[seq]
}

//
// Forget all seq # data if all peers have flagged it as done
// Note: Done() is a client call to the peer
// Whenever we acquire a lock for px, we can call freeMemory()
//

func (px *Paxos) freeMemory() {
    // Assertion: px is already locked by the callee

    // reproduction of Min() without requesting a lock
    // Question: Can I do this without duplciating code?
    min := px.done[px.me]
    for i := 0; i < len(px.done); i++ {
        if px.done[i] < min {
            min = px.done[i]
        }
    }
    min += 1

    for i, _ := range px.Instances {
        if i < min {
            delete(px.Instances, i)
        }
    }
}

func call(srv string, name string, args interface{}, reply interface{}) bool {
    c, err := rpc.Dial("unix", srv)
    if err != nil {
        err1 := err.(*net.OpError)
        if err1.Err != syscall.ENOENT && err1.Err != syscall.ECONNREFUSED {
        //    fmt.Printf("paxos Dial() failed: %v\n", err1)
        }
        return false
    }
    defer c.Close()

    err = c.Call(name, args, reply)
    if err == nil {
        return true
    }

    fmt.Println(err)
    return false
}

//
// Prepare RPC
//
func (px *Paxos) Prepare(args *PrepareArgs, reply *PrepareReply) error {
    px.mu.Lock()
    defer px.mu.Unlock()

    // if px.rejoinSeq > arg.Seq {  // this simple?
    //     return nil
    // }

    // duplicated getInstance() because of locking
    if _, ok := px.Instances[args.Seq]; !ok {
        px.Instances[args.Seq] = Instance{ NullProposal(), 
                                           NullProposal(), 
                                           nil, 
                                           Pending }
        if args.Seq > px.maxSeq {
            px.maxSeq = args.Seq
        }
    }

    instance := px.Instances[args.Seq]

    // follows the pseudocode for the Paxos algorithm
    if args.N.Greater(instance.Np) {
        px.Instances[args.Seq] = Instance{ args.N, 
                                           instance.Na, 
                                           instance.Va, 
                                           instance.Fate }
        reply.Na = instance.Na
        reply.Va = instance.Va
        reply.Ok = true
    } else {
        reply.Ok = false
    }

    reply.Done = px.done[px.me]

    px.freeMemory()

    return nil
}

//
// Accept RPC
//
func (px *Paxos) Accept(args *AcceptArgs, reply *AcceptReply) error {
    px.mu.Lock()
    defer px.mu.Unlock()

    // if px.rejoinSeq > args.Seq && args.V.Xid != px.xid {
    //     return nil
    // }

    // duplicated getInstance() because of locking
    if _, ok := px.Instances[args.Seq]; !ok {
        px.Instances[args.Seq] = Instance{ NullProposal(), 
                                           NullProposal(), 
                                           nil, 
                                           Pending }
        if args.Seq > px.maxSeq {
            px.maxSeq = args.Seq
        }
    }

    instance := px.Instances[args.Seq]

    // follows the pseudocode for the Paxos algorithm
    if args.N.Geq(instance.Np) {
        px.Instances[args.Seq] = Instance{ args.N,
                                           args.N, 
                                           args.V, 
                                           instance.Fate }
        reply.N  = args.N
        reply.Ok = true
    } else {
        reply.N  = instance.Np
        reply.Ok = false
    }

    reply.Done = px.done[px.me]

    px.freeMemory()

    return nil
}

//
// Decide RPC
//
func (px *Paxos) Decide(args *DecideArgs, reply *DecideReply) error {
    px.mu.Lock()
    defer px.mu.Unlock()

    // if px.rejoinSeq > args.Seq && args.Va.Xid != px.xid {
    //     return nil
    // }

    if args.Seq > px.maxSeq {
        px.maxSeq = args.Seq
    }

    // store the decided value for this seq #
    px.Instances[args.Seq] = Instance{ args.N, 
                                       args.N, 
                                       args.Va, 
                                       Decided }

    reply.Ok = true
    reply.Done = px.done[px.me]

    // if args.Va.Xid == xid {
    //     px.rejoinSeq = args.Seq
    //     px.restricted = false
    // }

    px.freeMemory()

    return nil
}

//
// send<RPC>() makes a distinction between calling the functioning locally
// and sending an RPC to a remote peer
//
func (px *Paxos) sendPrepare(to int, args PrepareArgs, reply *PrepareReply) bool {
    if to == px.me {
        if err := px.Prepare(&args, reply); err == nil {
            return true
        } else {
            return false
        }
    }
    return call(px.peers[to], "Paxos.Prepare", args, reply)
}

func (px *Paxos) sendAccept(to int, args AcceptArgs, reply *AcceptReply) bool {
    if to == px.me {
        if err := px.Accept(&args, reply); err == nil {
            return true
        } else {
            return false
        }
    }
    return call(px.peers[to], "Paxos.Accept", args, reply)
}

func (px *Paxos) sendDecide(to int, args DecideArgs, reply *DecideReply) bool {
    if to == px.me {
        if err := px.Decide(&args, reply); err == nil {
            return true
        } else {
            return false
        }
    }
    return call(px.peers[to], "Paxos.Decide", args, reply)
}

//
// Start() is the client request handler
// It launches a thread for each (seq, value) proposed
//
func (px *Paxos) Start(seq int, v interface{}) {
    
    // if consensus on this seq number is no longer
    // desired from the client, ignore this request
    if seq < px.Min() {
        return
    }

    // access state up front
    px.mu.Lock()
    
    doneMap := make(map[int]int) // local copy of the done map
    for k, v := range px.done {  
        doneMap[k] = v
    }

    px.freeMemory()
    
    px.mu.Unlock()

    // Question could you optimize the initial proposal number?

    go func(seq int, v interface{}) {       // launch thread
        np := ProposalId { 0, px.me }       // initial proposal            
        instance := px.getInstance(seq)     // retrieve instance from log

        // while loop driving consensus:
        for !px.isdead() && instance.Fate == Pending {
            na := NullProposal()            // highest na seen
            va := v                         // va associated with (na, va)
            count := 0                      // count prepare_oks

            // follows the pseudocode for the Paxos algorithm
            for i := 0; i < len(px.peers); i++ {
                args := PrepareArgs{seq, np}
                var reply PrepareReply
                if px.sendPrepare(i, args, &reply) && reply.Ok {
                    doneMap[i] = reply.Done
                    count += 1
                    if reply.Na.Greater(na) {
                        na = reply.Na
                        va = reply.Va
                    }
                }
            }

            // did not gain a majority response during prepare
            // check instance for consensus and try again with
            // the next highest proposal id
            if count < 1 + len(px.peers) / 2 {
                instance = px.getInstance(seq)
                if na.Greater(np) {
                    np = np.Next(na)
                } else {
                    np = np.Next(np)
                }
                continue // return to top of for loop
            }

            count = 0                       // count accept_oks
            
            // follows the pseudocode for the Paxos algorithm
            for i := 0; i < len(px.peers); i++ {
                args := AcceptArgs{seq, np, va}
                var reply AcceptReply
                if px.sendAccept(i, args, &reply) && reply.Ok {
                    doneMap[i] = reply.Done
                    count += 1
                }
            }

            // did not gain a majority response during accept
            // check instance for consensus and try again with
            // the next highest proposal id
            if count < 1 + len(px.peers) / 2 {
                instance = px.getInstance(seq)
                if na.Greater(np) {
                    np = np.Next(na)
                } else {
                    np = np.Next(np)
                }
                continue // return to top of for loop
            }

            // consensus achieved, sendDecide()
            for i := 0; i < len(px.peers); i++ {
                args := DecideArgs{seq, np, va}
                var reply DecideReply
                if px.sendDecide(i, args, &reply) && reply.Ok {
                    doneMap[i] = reply.Done
                }
            }

            // Question: Should we retry if sendDecide() fails?

            break // consesus achieved break out of the while loop
        }

        // update done pointer to address the local updated copy
        px.mu.Lock()
        
        px.done = doneMap
        px.freeMemory()
        
        px.mu.Unlock()

        // Question: Should I check for higher value in px.done?
        // Local copy's done[i] may be less than the global max. 

    }(seq, v)
}

//
// Application call to tell px_server that it no 
// longer needs values from this seq or less
//
func (px *Paxos) Done(seq int) {
    px.mu.Lock()
    defer px.mu.Unlock()

    px.done[px.me] = seq
    px.freeMemory()
}

//
// Return the max seq # seen thus far
//
func (px *Paxos) Max() int {
    return px.maxSeq
}

//
// One more than the minimum seq # in the done map
//
func (px *Paxos) Min() int {
    px.mu.Lock()
    defer px.mu.Unlock()

    min := px.done[px.me]
    for i := 0; i < len(px.done); i++ {
        if px.done[i] < min {
            min = px.done[i]
        }
    }

    // Question: Why not freeMemory() here?

    return min + 1
}

//
// Client call to learn the (fate, value) for a particular seq #
//
func (px *Paxos) Status(seq int) (Fate, interface{}) {
    
    if seq < px.Min() {
        return Forgotten, nil
    }

    instance := px.getInstance(seq)
    return instance.Fate, instance.Va
}

//
// tell the peer to shut itself down for testing
// please do not change these two functions
//
func (px *Paxos) Kill() {
    atomic.StoreInt32(&px.dead, 1)
    if px.l != nil {
        px.l.Close()
    }
}

//
// has this peer been asked to shut down
//
func (px *Paxos) isdead() bool {
    return atomic.LoadInt32(&px.dead) != 0
}

// please do not change these two functions
func (px *Paxos) setunreliable(what bool) {
    if what {
        atomic.StoreInt32(&px.unreliable, 1)
    } else {
        atomic.StoreInt32(&px.unreliable, 0)
    }
}

func (px *Paxos) isunreliable() bool {
    return atomic.LoadInt32(&px.unreliable) != 0
}

//
// the application wants to create a paxos peer
// the ports of all the paxos peers (including this one) are in peers[]
// this server's port is peers[me]
//
func Make(peers []string, me int, rpcs *rpc.Server) *Paxos {
    px := &Paxos{}
    px.peers = peers
    px.me = me
    px.Instances = make(map[int]Instance)
    px.done = make(map[int]int)
    px.maxSeq = -1
    
    // instantiate all done[i] at -1
    for i, _ := range px.peers {
        px.done[i] = -1
    }

    if rpcs != nil {
        // caller will create socket &c
        rpcs.Register(px)
    } else {
        rpcs = rpc.NewServer()
        rpcs.Register(px)

        // prepare to receive connections from clients.
        // change "unix" to "tcp" to use over a network.
        os.Remove(peers[me]) // only needed for "unix"
        l, e := net.Listen("unix", peers[me])
        if e != nil {
            log.Fatal("listen error: ", e)
        }
        px.l = l

        // please do not change any of the following code,
        // or do anything to subvert it.

        // create a thread to accept RPC connections
        go func() {
            for px.isdead() == false {
                conn, err := px.l.Accept()
                if err == nil && px.isdead() == false {
                    if px.isunreliable() && (rand.Int63()%1000) < 100 {
                        // discard the request.
                        conn.Close()
                    } else if px.isunreliable() && (rand.Int63()%1000) < 200 {
                        // process the request but force discard of reply.
                        c1 := conn.(*net.UnixConn)
                        f, _ := c1.File()
                        err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
                        if err != nil {
                            fmt.Printf("shutdown: %v\n", err)
                        }
                        atomic.AddInt32(&px.rpcCount, 1)
                        go rpcs.ServeConn(conn)
                    } else {
                        atomic.AddInt32(&px.rpcCount, 1)
                        go rpcs.ServeConn(conn)
                    }
                } else if err == nil {
                    conn.Close()
                }
                if err != nil && px.isdead() == false {
                    fmt.Printf("Paxos(%v) accept: %v\n", me, err.Error())
                }
            }
        }()
    }

    return px
}