package shardmaster

import (
	"encoding/gob"
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/rpc"
	"os"
	"paxos"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

// import "strconv"

type ShardMaster struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       int32 // for testing
	unreliable int32 // for testing
	px         *paxos.Paxos
	configs    []Config        // store of all past configurations
	version    int             // index into configs
	top        int             // top of the log
	configMap  map[int64][]int // better data structure GID -> [shard]
}

// Op{} is a the interface used for Paxos proposals
type Op struct {
	Ty      string
	GID     int64
	Servers []string
	Shard   int
	Num     int
}

// constructor for Op{}
func makeOp(ty string, GID int64, servers []string, shard int, num int) Op {
	op := Op{}
	if ty == "Join" {
		op.Ty = "Join"
		op.GID = GID
		op.Servers = servers
	} else if ty == "Leave" {
		op.Ty = "Leave"
		op.GID = GID
	} else if ty == "Move" {
		op.Ty = "Move"
		op.GID = GID
		op.Shard = shard
	} else if ty == "Query" {
		op.Ty = "Query"
		op.Num = num
	}
	return op
}

//
// propose(Op, seq) was taken directly from kvpaxos code
//
func (sm *ShardMaster) propose(proposal Op, seq int) (Op, int) {

	// make proposal by calling Start()

	sm.px.Start(seq, proposal)

	time_out := 25 * time.Millisecond // not 100% on this.. maybe randomize it

	// while loop that waits for consensus:
	for !sm.isdead() {
		status, op := sm.px.Status(seq) // listen by repeatedly calling Status()
		if status == paxos.Decided {
			// must assert interface{} is of type Op{}
			return op.(Op), seq
		}

		time.Sleep(time_out)
	}

	return Op{}, -1

	// me := sm.me
	// panicString := "SM Server-" + strconv.Itoa(me) + " non-responsive..."
	// panic(panicString)
}

//
// RPC handlers follow the same pattern as kvpaxos code
//
func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	// fmt.Printf("Join(%d, %v) RPC arrived @ server #%d.\n", args.GID, args.Servers, sm.me)

	proposal := makeOp("Join", args.GID, args.Servers, 0, 0)
	decided_op, seq := sm.propose(proposal, sm.top)

	// fmt.Printf("Server #%d proposed Join(%d) for seq #%d and Paxos decided: %v\n\n", sm.me, args.GID, seq, decided_op)

	sm.executeLog(decided_op, seq)

	if decided_op.Ty == "Join" && decided_op.GID == args.GID {
		return nil
	} else {
		return fmt.Errorf("NAK - %v\n", proposal)
	}
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	// fmt.Printf("Leave(%d) RPC arrived @ server #%d.\n", args.GID, sm.me)

	proposal := makeOp("Leave", args.GID, make([]string, 0), 0, 0)
	decided_op, seq := sm.propose(proposal, sm.top)

	// fmt.Printf("Server #%d proposed Leave(%d) for seq #%d and Paxos decided: %v\n\n", sm.me, args.GID, seq, decided_op)

	sm.executeLog(decided_op, seq)

	if decided_op.Ty == "Leave" && decided_op.GID == args.GID {
		return nil
	} else {
		return fmt.Errorf("NAK - %v\n", proposal)
	}
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	// fmt.Printf("Move(%d, %d) RPC arrived @ server #%d.\n", args.Shard, args.GID, sm.me)

	proposal := makeOp("Move", args.GID, make([]string, 0), args.Shard, 0)
	decided_op, seq := sm.propose(proposal, sm.top)

	// fmt.Printf("Server #%d proposed Move(%d, %d) for seq #%d and Paxos decided: %v\n\n", sm.me, args.GID, args.Shard, seq, decided_op)

	sm.executeLog(decided_op, seq)

	if decided_op.Ty == "Move" && decided_op.GID == args.GID && decided_op.Shard == args.Shard {
		return nil
	} else {
		return fmt.Errorf("NAK - %v\n", proposal)
	}
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	// fmt.Printf("Query(%d) RPC arrived @ server #%d.\n", args.Num, sm.me)

	proposal := makeOp("Query", 0, make([]string, 0), 0, args.Num)
	decided_op, seq := sm.propose(proposal, sm.top)

	// fmt.Printf("Server #%d proposed Query(%d) for seq #%d and Paxos decided: %v\n\n", sm.me, args.Num, seq, decided_op)

	sm.executeLog(decided_op, seq)

	if decided_op.Ty == "Query" && decided_op.Num == args.Num {
		if args.Num == -1 || args.Num > len(sm.configs)-1 {
			reply.Config = sm.configs[len(sm.configs)-1]
		} else {
			reply.Config = sm.configs[args.Num]
		}
		return nil
	} else {
		return fmt.Errorf("NAK - %v\n", proposal)
	}
}

//
// Functions to handle local execution on the configuration state
//
func (sm *ShardMaster) join(op Op, seq int) {

	// fmt.Printf("Server #%d executing join(%v, %v).\n", sm.me, op, seq)

	// unpack Op
	newGID := op.GID
	servers := op.Servers

	// unpack current config
	config := sm.configs[sm.version]
	num := config.Num
	shards := config.Shards // creates a local copy of the array
	groups := config.Groups // groups is a reference variable (not a copy)

	// check for membership, assuming every set of servers is non-empty
	if len(groups[newGID]) != 0 {
		// fmt.Printf("GID #%v is already a member skipping execution and reporting complete.\n", newGID)
		sm.px.Done(seq)
		sm.top += 1
		return
	}

	// fmt.Printf("GID #%d failed membership check, adding group.\n", newGID)

	// instantiate a new groups map and copy old one
	newGroups := make(map[int64][]string)
	for GID, svrs := range groups {
		newGroups[GID] = svrs
	}

	newGroups[newGID] = servers                    // update newGroups
	sm.configMap[newGID] = make([]int, 0, NShards) // update configMap

	// fmt.Println("configMap prior to balancing:")
	// printShardMap(sm.configMap)
	// printGroupMap(groups)

	spg := NShards / len(newGroups) // shards per group

	balance(sm.configMap, spg) // balance shard loading

	// fmt.Println("configMap after balancing:")
	// printShardMap(sm.configMap)
	// printGroupMap(newGroups)

	// update shards array according to the balanced configMap
	for GID, shds := range sm.configMap {
		for _, shd := range shds {
			shards[shd] = GID
		}
	}

	// increment config.Num and version
	num++
	sm.version++

	// add latest configuration to the config log
	newConfig := Config{num, shards, newGroups}
	sm.configs = append(sm.configs, newConfig)

	// fmt.Printf("New configuration for server #%d after join: ", sm.me)
	// fmt.Println(newConfig)

	sm.px.Done(seq)
	sm.top += 1
	return
}

func (sm *ShardMaster) leave(op Op, seq int) {

	// fmt.Printf("Server #%d executing leave(%v, %v).\n", sm.me, op, seq)

	// unpack Op
	oldGID := op.GID

	// unpack current config
	config := sm.configs[sm.version]
	num := config.Num
	shards := config.Shards // creates a local copy of the array
	groups := config.Groups // groups is a reference variable (not a copy)

	// check for membership; assumes the set of servers is non-empty
	if len(groups[oldGID]) == 0 {
		// fmt.Printf("GID #%v is not a member skipping execution and reporting complete.\n", oldGID)
		sm.px.Done(seq)
		sm.top += 1
		return
	}

	// fmt.Printf("GID #%d is a member, removing group.\n", oldGID)

	// instantiate a new groups map and copy old one
	newGroups := make(map[int64][]string)
	for GID, svrs := range groups {
		newGroups[GID] = svrs
	}

	delete(newGroups, oldGID) // remove the departing GID

	// update the configMap by unassigning shards and deleting oldGID
	for _, s := range sm.configMap[oldGID] {
		sm.configMap[0] = append(sm.configMap[0], s)
	}

	delete(sm.configMap, oldGID)

	// fmt.Println("configMap prior to balancing:")
	// printShardMap(sm.configMap)
	// printGroupMap(groups)

	spg := NShards / len(newGroups)

	balance(sm.configMap, spg)

	// fmt.Println("configMap after balancing:")
	// printShardMap(sm.configMap)
	// printGroupMap(newGroups)

	// update shards array
	for GID, shds := range sm.configMap {
		for _, shd := range shds {
			shards[shd] = GID
		}
	}

	// increment config.Num and version
	num++
	sm.version++

	// add latest configuration to the config log
	newConfig := Config{num, shards, newGroups}
	sm.configs = append(sm.configs, newConfig)

	// fmt.Printf("New configuration for server #%d after leave: ", sm.me)
	// fmt.Println(newConfig)

	sm.px.Done(seq)
	sm.top += 1
}

func (sm *ShardMaster) move(op Op, seq int) {

	// unpack Op
	GID := op.GID
	shard := op.Shard

	// unpack current config
	config := sm.configs[sm.version]
	num := config.Num
	shards := config.Shards // creates a local copy of the array
	groups := config.Groups // groups is a reference variable (not a copy)

	// increment config.Num and version
	num++
	sm.version++

	oldGID := shards[shard]
	shards[shard] = GID

	// fmt.Println("configMap before the move:")
	// printShardMap(sm.configMap)

	sm.configMap[oldGID] = remove(sm.configMap[oldGID], shard)
	sm.configMap[GID] = append(sm.configMap[GID], shard)

	// fmt.Println("configMap after the move:")
	// printShardMap(sm.configMap)

	// instantiate a new groups map and copy old one
	newGroups := make(map[int64][]string)
	for GID, svrs := range groups {
		newGroups[GID] = svrs
	}

	// add latest configuration to the config log
	newConfig := Config{num, shards, newGroups}
	sm.configs = append(sm.configs, newConfig)

	// fmt.Printf("New configuration for server #%d after leave: ", sm.me)
	// fmt.Println(newConfig)

	sm.px.Done(seq)
	sm.top += 1
}

// func printShardMap(configMap map[int64][]int) {
// 	for GID, shards := range configMap {
// 		fmt.Printf("%v -> %v\n", GID, shards)
// 	}
// }

// func printGroupMap(groups map[int64][]string) {
// 	for GID, servers := range groups {
// 		fmt.Printf("%v -> %v\n", GID, servers)
// 	}
// }

func balance(configMap map[int64][]int, spg int) {
	// if spg == 0 { spg = 1 } // crashes basic // passes unreliable

	for GID, shards := range configMap {
		if GID != 0 {
			n := len(shards)
			for n == 0 || n < spg {
				maxGID := findMax(configMap)
				shardsMax := configMap[maxGID]
				if maxGID != 0 && n == 0 && len(shardsMax) == 1 {
					break // no need to swap one for one
				}
				configMap[GID] = append(configMap[GID], shardsMax[0])
				configMap[maxGID] = configMap[maxGID][1:]
				n++
			}
		}
	}
}

func findMax(configMap map[int64][]int) int64 {
	max := -1
	var maxGID int64
	for GID, shards := range configMap {
		if GID == 0 && len(shards) > 0 {
			return 0
		}
		if len(shards) > max {
			maxGID = GID
			max = len(shards)
		}
	}
	return maxGID
}

func remove(shards []int, shard int) []int {
	var index int
	for i, s := range shards {
		if s == shard {
			index = i
		}
	}

	if index == len(shards)-1 {
		return shards[:index]
	} else {
		return append(shards[:index], shards[index+1:]...)
	}
}

func (sm *ShardMaster) executeLog(decided_op Op, seq int) {

	// fmt.Printf("Server #%v executing seq #%v: %v.\n", sm.me, seq, decided_op)

	status, op := sm.px.Status(seq)
	if status == paxos.Decided {
		decided_op := op.(Op)
		if decided_op.Ty == "Join" {
			sm.join(decided_op, sm.top)
		} else if decided_op.Ty == "Leave" {
			sm.leave(decided_op, sm.top)
		} else if decided_op.Ty == "Move" {
			sm.move(decided_op, sm.top)
		} else {
			sm.top++
		}
	} else {
		return // maybe you should execute something?
	}

	// fmt.Println()
}

// please don't change these two functions.
func (sm *ShardMaster) Kill() {
	atomic.StoreInt32(&sm.dead, 1)
	sm.l.Close()
	sm.px.Kill()
}

// call this to find out if the server is dead.
func (sm *ShardMaster) isdead() bool {
	return atomic.LoadInt32(&sm.dead) != 0
}

// please do not change these two functions.
func (sm *ShardMaster) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&sm.unreliable, 1)
	} else {
		atomic.StoreInt32(&sm.unreliable, 0)
	}
}

func (sm *ShardMaster) isunreliable() bool {
	return atomic.LoadInt32(&sm.unreliable) != 0
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
func StartServer(servers []string, me int) *ShardMaster {
	sm := new(ShardMaster)
	sm.me = me

	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int64][]string{}
	sm.version = 0
	sm.top = 0
	sm.configMap = make(map[int64][]int)

	// initially assign all shards to GID 0
	for i := 0; i < NShards; i++ {
		sm.configMap[0] = append(sm.configMap[0], i)
	}

	rpcs := rpc.NewServer()

	gob.Register(Op{})
	rpcs.Register(sm)
	sm.px = paxos.Make(servers, me, rpcs)

	os.Remove(servers[me])
	l, e := net.Listen("unix", servers[me])
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	sm.l = l

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for sm.isdead() == false {
			conn, err := sm.l.Accept()
			if err == nil && sm.isdead() == false {
				if sm.isunreliable() && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if sm.isunreliable() && (rand.Int63()%1000) < 200 {
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
			if err != nil && sm.isdead() == false {
				fmt.Printf("ShardMaster(%v) accept: %v\n", me, err.Error())
				sm.Kill()
			}
		}
	}()

	return sm
}
