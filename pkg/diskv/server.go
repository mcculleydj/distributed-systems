package diskv

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
import "io/ioutil"
import "bytes"
import "encoding/base32"
import "strconv"

const Debug = 0 													// set > 0 to log DPrintf statements

func DPrintf(format string, a ...interface{}) (n int, err error) { 	// why are there return types?
	if Debug > 0 { 													// if Debug is 'on'
		log.Printf(format, a...)    								// format: "% % % " a: variadic []interface{}
	}
	return 															// why not return (int, error)?
}

type DisKV struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       int32 							// for testing
	unreliable int32 							// for testing
	sm         *shardmaster.Clerk
	px         *paxos.Paxos
	dir        string                           // each replica has its own directory

	// added state:

	gid        int64                            // replica group ID
	config	   shardmaster.Config               // current config
	data	   map[string]string            	// kv store
	reads      map[int64]GetReply            	// cached replies to gets in the log
	writes     map[int64]PutAppendReply         // cached replies to put/appends in the log
	receives   map[ShardId]ReceiveShardReply 	// cached replies to receives in the log
	requests   map[int64]RequestLogReply        // cached replies to requests in the log 
	top        int                              // the log is complete for all seq < top
	next       int                              // working on reaching this config
	execLog    map[int]Op						// execution log seq -> Op
	ckpt 	   Ckpt 							// initial state when starting hot
}

type Op struct {
	Xid    int64                      	// transaction id necessary for at-most-once semantics
	Ty     string                    	// operation type (Get, Put, Append, Advance, Receive, or Request)
	Key    string                    	// key   (for reads/writes)
	Value  string                    	// value (for writes)
	Sid    ShardId 				    	// shard id (a key for Receive ops)
	Data   map[string]string        	// transfer shard data
	Reads  map[int64]GetReply       	// transfer reads cache
	Writes map[int64]PutAppendReply 	// transfer writes cache
	Config shardmaster.Config       	// next config
	Me     int                       	// which server is requesting the log
	Next   int 							// for Checkpoint ops, to handle a checkpoint mid-advance
}

type Ckpt struct {
	Data   map[string]string 			// datastore at the moment of hot start
	Top    int 							// kv.top at the moment of hot start
	Config shardmaster.Config 			// kv.config at the moment of hot start
	Next   int 							// kv.next at the moment of hot start
}

// kv.shardDir(shard) checks if a directory exists for this shard
// if not, it creates one uniquely named for this shard
// if directory creation fails it logs the error

func (kv *DisKV) shardDir(shard int) string {
	d := kv.dir + "/shard-" + strconv.Itoa(shard) + "/"		// directory name
	if _, err := os.Stat(d); err != nil { 					// if err exists, need to Mkdir
		if err := os.Mkdir(d, 0777); err != nil {           //    Mkdir w/ permissions 777; if Mkdir fails
			log.Fatalf("Mkdir(%v): %v", d, err)             //       log error 
		}
	}
	return d 												// return path
}

// cannot use keys in file names directly,
// since they might contain troublesome characters like '/'
// base32-encode the key to get a file name
// base32 rather than base64 b/c Mac has case-insensitive file names

func (kv *DisKV) encodeKey(key string) string {
	return base32.StdEncoding.EncodeToString([]byte(key))
}

func (kv *DisKV) decodeKey(filename string) (string, error) {
	key, err := base32.StdEncoding.DecodeString(filename)
	return string(key), err
}

// kv.fileGet(shard, key) reads the content of the file associated with this key
// returns the content as a string along with an error code

func (kv *DisKV) fileGet(shard int, key string) (string, error) {
	fullname := kv.shardDir(shard) + "/key-" + kv.encodeKey(key) 	// build the full path string
	content, err := ioutil.ReadFile(fullname) 						// returns a byte stream
	return string(content), err                                     // return the byte stream cast as a string
}

// kv.filePut(shard, key, content) replaces the content of a key's file
// uses os.Rename() to make the replacement atomic with respect to crashes
// returns an error code

func (kv *DisKV) filePut(shard int, key string, content string) error {
	fullname := kv.shardDir(shard) + "/key-" + kv.encodeKey(key) 				// build the full path string
	tempname := kv.shardDir(shard) + "/temp-" + kv.encodeKey(key) 				// create a temporary write file
	if err := ioutil.WriteFile(tempname, []byte(content), 0666); err != nil { 	// write new content the temp file w/ permissions 666
		return err 																// if error: return
	}
	if err := os.Rename(tempname, fullname); err != nil { 						// overwrite old file with the temp file 
		return err 																// if error: return
	}
	return nil 																	// successful call... return nil
}

// kv.fileAppend(shard, key content) adds the content to a key's file
// uses os.Rename() to make the append atomic with respect to crashes
// returns an error code

func (kv *DisKV) fileAppend(shard int, key string, content string) error {													
	fullname := kv.shardDir(shard) + "/key-" + kv.encodeKey(key) 				// build the full path string
	tempname := kv.shardDir(shard) + "/temp-" + kv.encodeKey(key) 				// create a temporary write file
	if value, err := kv.fileGet(shard, key); err != nil {						// retrieve existing value
		return err 																// if error: return
	} else {						    	
		content = value + content												// concatenate and overwrite content
	}
	if err := ioutil.WriteFile(tempname, []byte(content), 0666); err != nil { 	// write new content the temp file w/ permissions 666
		return err 																// if error: return
	}
	if err := os.Rename(tempname, fullname); err != nil { 						// overwrite old file with the temp file 
		return err 																// if error: return
	}
	return nil 																	// successful call... return nil
}

// kv.fileReadShard(shard) scans all the key files in a shard directory
// return a pointer to a map that houses the kv store for this shard

func (kv *DisKV) fileReadShard(shard int) map[string]string {
	m := map[string]string{} 													// instantiate a new map
	d := kv.shardDir(shard)														// obtain path for this shard
	var key string 																// instantiate key
	var content string															// instantiate content	
	var err error																// instantiate err
	files, err := ioutil.ReadDir(d) 											// ReadDir returns ([]os.FileInfo, error)
	if err != nil { 															// if ReadDir fails:
		log.Fatalf("fileReadShard could not read %v: %v", d, err) 				//    log error
	}
	for _, fi := range files { 													// for file in files:
		n1 := fi.Name() 														//    set n1 = file.Name()
		if n1[0:4] == "key-" {  	 											//    if this file is a key file:
			if key, err = kv.decodeKey(n1[4:]); err != nil {					//       decode the key
				log.Fatalf("fileReadShard bad file name %v: %v", n1, err)       //       if decodeKey() fails: log error
			} else if content, err = kv.fileGet(shard, key); err != nil {       //       get the value associated with this key
				log.Fatalf("fileReadShard fileGet failed for %v: %v", key, err) //       if fileGet() fails: log error
			} else {
			    m[key] = content 										        //       else: map key -> value
			}
		}
	}
	return m 																	// return map
}

// kv.fileReplaceShard(shard, m) overwrites this shard's directory
// with the kv store contained in m

func (kv *DisKV) fileReplaceShard(shard int, m map[string]string) {
	d := kv.shardDir(shard) 											// retrieve shard directory path
	os.RemoveAll(d) 													// remove all existing files from shard
	for k, v := range m { 												// for kvs in m
		err := kv.filePut(shard, k, v) 									//    add kv to the persistant store
		if err != nil {
			log.Fatalf("fileReplaceShard filePut failed for %v: %v", shard, err)
		}
	}
}

// encodeState() and decodeState() use Go's gob library
// to store the server's state as a string on disk
// at the moment, only kv.top is stored

func (kv *DisKV) encodeState() string {
	w := new(bytes.Buffer)                		// instantiate a new buffer
	e := gob.NewEncoder(w) 						// instantiate a new encoder with w as a target
  	e.Encode(kv.top)	 						// encode state
  	e.Encode(kv.config) 						
  	e.Encode(kv.next)
	return string(w.Bytes())               		// return buffer cast as a string
}

func decodeState(buf string) (int, shardmaster.Config, int) {
	r := bytes.NewBuffer([]byte(buf)) 			// assign buffer to r
	d := gob.NewDecoder(r) 						// instantiate a new decoder
	var top int									// instantiate return variables
	var config shardmaster.Config 				
	var next int 								 
	d.Decode(&top) 								// decode state
	d.Decode(&config) 							
	d.Decode(&next) 							
	return top, config, next					// return state
}

// kv.saveState() saves the server's state
// to a file named state in the server's directory

func (kv *DisKV) saveState() error {
	state := kv.encodeState()     										// encode top as a string
	path  := kv.dir + "/state" 											// define the path
	temp  := kv.dir + "/tempstate" 										// define the temp path
	if err := ioutil.WriteFile(temp, []byte(state), 0666); err != nil { // write new content the temp path
		return err 														// if error: return
	}
	if err := os.Rename(temp, path); err != nil { 						// perform swap (atomicity)
		return err 														// if error: return
	}
	return nil 															// successful op... return nil
}

// kv.retrieveLog() retrieves the log stored on disk

func (kv *DisKV) retrieveState() (int, shardmaster.Config, int, error) {
	path := kv.dir + "/state" 									// definte the path
	s, err := ioutil.ReadFile(path) 							// read state from file
	if err != nil { 											// if: an error occurs while reading file
		return 0, shardmaster.Config{}, 0, err					// return a default zero state vector
	}
	top, config, next := decodeState(string(s)) 				// decode state
	return top, config, next, nil 								// return decoded state
}

// RPC Handlers:

func (kv *DisKV) Get(args *GetArgs, reply *GetReply) error {

	// lock and defer unlock

	kv.mu.Lock()
	fmt.Printf("Get(%v) RPC arrived @ Svr %v-%v... locking.\n",
		       args.Key, kv.gid, kv.me)
	defer func() {
		fmt.Printf("Get(%v) RPC handled by Svr %v-%v... unlocking.\n",
			       args.Key, kv.gid, kv.me)
		kv.mu.Unlock()
	}()

	// if this xid was logged, return the stored reply

	if r, ok := kv.reads[args.Xid]; ok { 										
		fmt.Printf("\tSrv %v-%v FOUND Get(%v) in cache... sending value: %v.\n", 
		           kv.gid, kv.me, args.Key, r.Value)
		reply.Value = r.Value
		reply.Err   = r.Err
		return nil
	}

	// if the client config num is not the same as the server's reject the request

	if args.ConfigNum != kv.config.Num {
		fmt.Printf("Srv %v-%v is NOT IN SYNC with client, rejecting Get(%v) RPC.\n",
			       kv.gid, kv.me, args.Key)
		fmt.Printf("Srv %v-%v on configuration %v... client trying to Get for configuration %v.\n",
			       kv.gid, kv.me, kv.config.Num, args.ConfigNum)
		reply.Err = ErrConfigSync
		return nil
	}

	// kv.next != kv.config.Num means the server is executing applyAdvance()

	if kv.next != kv.config.Num {
		fmt.Printf("Srv %v-%v is busy with a configuration change, rejecting Get(%v) RPC.\n",
			       kv.gid, kv.me, args.Key)
		reply.Err = ErrConfigSync
		return nil
	}

	fmt.Printf("Get(%v) is not in the cache... Srv %v-%v making a Get proposal for seq %v.\n",
		       args.Key, kv.gid, kv.me, kv.top)
	
	reply.Err = ErrNotInCache

	// create a Get proposal for kv.px

	proposal := Op{}
	proposal.Xid = args.Xid
	proposal.Ty  = "Get"
	proposal.Key = args.Key

	// put the proposal on the wire

	kv.px.Start(kv.top, proposal)
	
	return nil
}

func (kv *DisKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {

	// lock and defer unlock

	kv.mu.Lock()
	fmt.Printf("%v(%v, %v) RPC arrived @ Svr %v-%v... locking.\n", 
		       args.Op, args.Key, args.Value, kv.gid, kv.me)
	defer func() {
		fmt.Printf("%v(%v, %v) RPC handled by Srv %v-%v... unlocking.\n", 
			       args.Op, args.Key, args.Value, kv.gid, kv.me)
		kv.mu.Unlock()
	}()

	// if this xid was logged, return the stored reply

	if r, ok := kv.writes[args.Xid]; ok {		
		fmt.Printf("\tSrv %v-%v FOUND %v(%v, %v) in cache...\n", 
			       kv.gid, kv.me, args.Op, args.Key, args.Value)
		reply.Err = r.Err
		return nil
	}

	// if the client config num is not the same as the server's reject the request

	if args.ConfigNum != kv.config.Num {
		fmt.Printf("Srv %v-%v is NOT IN SYNC with client, rejecting %v(%v, %v) RPC.\n", 
			       kv.gid, kv.me, args.Op, args.Key, args.Value)
		fmt.Printf("Srv %v-%v on configuration %v... client trying to PutAppend for configuration %v.\n", 
			       kv.gid, kv.me, kv.config.Num, args.ConfigNum)
		reply.Err = ErrConfigSync
		return nil
	}

	// kv.next != kv.config.Num means the server is executing applyAdvance()

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

	proposal := Op{}
	proposal.Xid   = args.Xid
	proposal.Ty    = args.Op
	proposal.Key   = args.Key
	proposal.Value = args.Value

	// put the proposal on the wire

	kv.px.Start(kv.top, proposal)

	return nil
}

func (kv *DisKV) ReceiveShard(args *ReceiveShardArgs, reply *ReceiveShardReply) error {

	// lock and defer unlock

	kv.mu.Lock()
	fmt.Printf("ReceiveShard(%v) RPC arrived @ Srv %v-%v... locking.\n", 
		       args.Sid, kv.gid, kv.me)
	defer func() {
		fmt.Printf("ReceiveShard(%v) RPC handled by Srv %v-%v... unlocking.\n", 
			       args.Sid, kv.gid, kv.me)
		kv.mu.Unlock()
	}()

	fmt.Printf("ReceiveShard: Srv %v-%v's config data [C: %v, N: %v].\n", 
		       kv.gid, kv.me, kv.config.Num, kv.next)

	// if this sid is logged, return the stored reply

	if r, ok := kv.receives[args.Sid]; ok {
		fmt.Printf("\tSrv %v-%v FOUND ReceiveShard(%v) in cache...\n",
			       kv.gid, kv.me, args.Sid)
		reply.Err = r.Err
		return nil
	}

	// if server is not executing applyAdvance() OR
	// the server's config is not in sync with the sid being sent
	// reject this ReceiveShard RPC with ErrConfigSync

	if kv.next != kv.config.Num + 1 || kv.config.Num != args.Sid.ConfigNum {
		fmt.Printf("Srv %v-%v NOT IN SYNC [C: %v, N: %v]... rejecting ReceiveShard(%v).\n", 
			       kv.gid, kv.me, kv.config.Num, kv.next, args.Sid)
		reply.Err = ErrConfigSync
		return nil
	}

	fmt.Printf("ReceiveShard(%v) was not in cache... Srv %v-%v making a Receive proposal for seq %v.\n", 
		       args.Sid, kv.gid, kv.me, kv.top)
	reply.Err = ErrNotInCache

	// create a Receive proposal for kv.px

	proposal := Op{}
	proposal.Ty     = "Receive"
	proposal.Sid    = args.Sid
	proposal.Data   = args.Data
	proposal.Reads  = args.Reads
	proposal.Writes = args.Writes

	// put proposal on the wire

	kv.px.Start(kv.top, proposal)
	
	return nil
}

func (kv *DisKV) RequestLog(args *RequestLogArgs, reply *RequestLogReply) error {

	// lock and defer unlock

	kv.mu.Lock()
	fmt.Printf("RequestLog(%v) RPC arrived @ Srv %v-%v... locking.\n", args.Xid, kv.gid, kv.me)
	defer func() {
		fmt.Printf("RequestLog(%v) RPC handled by Srv %v-%v... unlocking.\n", args.Xid, kv.gid, kv.me)
		kv.mu.Unlock()
	}()

	// if this xid was logged, return the stored reply

	if r, ok := kv.requests[args.Xid]; ok {		
		fmt.Printf("\tSrv %v-%v FOUND RequestLog(%v) in cache...\n", 
			       kv.gid, kv.me, args.Xid)
		reply.Err = r.Err
		reply.Log = r.Log
		reply.Ckpt = r.Ckpt 
		return nil
	}

	fmt.Printf("RequestLog(%v) is not in the cache... Srv %v-%v making a Request proposal for seq %v.\n",
		       args.Xid, kv.gid, kv.me, kv.top)
	reply.Err = ErrNotInCache

	// create a Request proposal for kv.px

	proposal := Op{}
	proposal.Xid = args.Xid
	proposal.Ty  = "Request"
	proposal.Me  = args.Me	         

	// put proposal on the wire

	kv.px.Start(kv.top, proposal)

	return nil
}

// execute Op [Get, Put/Append, Receive, Advance, Request, or Checkpoint]

func (kv *DisKV) execute(op Op, seq int) {

	kv.execLog[seq] = op 	// store the op in the execution log

	// print some information about this execute call

	ty := op.Ty

	if ty == "Get" {
		fmt.Printf("\tSrv %v-%v EXECUTING(op: %v(%v), seq: %v).\n",
			       kv.gid, kv.me, ty, op.Key, seq)
	} else if ty == "Put" || ty == "Append" {
		fmt.Printf("\tSrv %v-%v EXECUTING(op: %v(%v, %v), seq: %v).\n",
			       kv.gid, kv.me, ty, op.Key, op.Value, seq)
	} else if ty == "Receive" {
		fmt.Printf("\tSrv %v-%v EXECUTING(op: %v(%v), seq: %v).\n",
			       kv.gid, kv.me, ty, op.Sid, seq)
	} else if ty == "Advance" {
		fmt.Printf("\tSrv %v-%v EXECUTING(op: %v(%v), seq: %v).\n",
			       kv.gid, kv.me, ty, op.Config.Num, seq)
	} else if ty == "Request" {
		fmt.Printf("\tSrv %v-%v EXECUTING(op: %v(%v), seq: %v).\n",
			       kv.gid, kv.me, ty, op.Xid, seq)
	} else if ty == "Checkpoint" {
		fmt.Printf("\tSrv %v-%v EXECUTING(op: %v, seq: %v).\n",
			       kv.gid, kv.me, ty, seq)
	} else if ty == "" {
		fmt.Printf("\tSrv %v-%v EXECUTING(No-Op, seq: %v).\n",
			       kv.gid, kv.me, seq)
		kv.top += 1
		kv.saveState()
		return
	}

	// apply writes, cache replies, or execute checkpoints

	if ty == "Get" {
		kv.logRead(op, seq)
	} else if ty == "Put" || ty == "Append" {
		kv.applyAndLogWrite(op, seq)
	} else if ty == "Receive" {
		kv.applyAndLogReceive(op, seq)
	} else if ty == "Advance" {
		kv.applyAdvance(op, seq)
	} else if ty == "Request" {
		kv.logRequest(op, seq)
	} else if ty == "Checkpoint" {
		kv.applyCheckpoint(op, seq)
	}

	kv.saveState() 			// save state after every op, because client acks are not bundled
}

// execution functions:
// - alter the database in the case of writes
// - build and cache the reply for use by RPC handlers
// - handle checkpoint operations

func (kv *DisKV) logRead(op Op, seq int) {
	var reply GetReply 										// instantiate GetReply

	value, ok := kv.data[op.Key] 							// obtain the value from the datastore

	if !ok { 												// if key does not exist
		reply.Err = ErrNoKey 								//    reply with error: ErrNoKey
	} else { 												// else key exists
		reply.Value = value 								//    reply with value
		reply.Err = OK 										//    reply with OK
	}
	
	kv.reads[op.Xid] = reply 								// cache reply
	if kv.px != nil { 										// if not executing a cold start
		kv.px.Done(seq) 									//    call px.Done()
	}
	kv.top += 1 											// increment top
}

func (kv *DisKV) applyAndLogWrite(op Op, seq int) {
	var reply PutAppendReply 								// instantiate PutAppendReply
	var err error 											// instantiate error for disk IO
	
	shard := key2shard(op.Key) 								// retreive shard containing this key
	_, isNewKey := kv.data[op.Key] 							// find out if this key already exists

	if op.Ty == "Put" || !isNewKey { 						// if op is a Put or key DNE
		kv.data[op.Key] = op.Value 							//    set value for this key
		err = kv.filePut(shard, op.Key, op.Value)   		//    persistently store (key, value)
	} else if op.Ty == "Append" { 							// else if op is an Append (and key exists)
		kv.data[op.Key] += op.Value 						//    set old value + new value for this key
		err = kv.fileAppend(shard, op.Key, op.Value) 		//    persistently store (key, value)
	}

	if  err != nil {										// if there was an issue with the disk IO
		log.Fatalf("%v - Srv %v-%v PutAppend(%v, %v)\n", 	//    log the error
			       err, kv.gid, kv.me, op.Key, op.Value)
	}

	reply.Err = OK 											// reply with OK
	kv.writes[op.Xid] = reply 								// cache reply
	if kv.px != nil { 										// if not executing a cold start
		kv.px.Done(seq) 									//    call px.Done()
	}
	kv.top += 1 											// increment top
}

func (kv *DisKV) applyAndLogReceive(op Op, seq int) {
	var reply ReceiveShardReply 							// instantiate ReceiveShardReply
	var err error 											// instantiate disk IO error

	// shard := op.Sid.Shard
	// kv.fileReplaceShard(shard, op.Data) 					// update the database on disk

	for k, v := range op.Data { 							// add to or overwrite existing database in memory
		kv.data[k] = v
		shard := key2shard(k)
		err = kv.filePut(shard, k, v)
		if  err != nil {
			log.Fatalf("%v - Srv %v-%v ReceiveShard(%v)\n",
				       err, kv.gid, kv.me, op.Sid)
		}
	}

	for k, v := range op.Reads { 							// update R/W caches
		kv.reads[k] = v
	}

	for k, v := range op.Writes {
		kv.writes[k] = v
	}

	reply.Err = OK 											// reply with OK					
	kv.receives[op.Sid] = reply 							// cache reply
	if kv.px != nil { 										// if not executing a cold start
		kv.px.Done(seq) 									//    call px.Done()
	}
	kv.top += 1 											// increment top
}

// Can you handle crashes in the middle of an applyAdvance()?
// I highly doubt it...

func (kv *DisKV) applyAdvance(op Op, seq int) {

	currentConfig := kv.config // make a local copy of the current config
	nextConfig    := op.Config // make a local copy of the next config 

	// it should be the case that the currentConfig.Num == kv.next
	// because the server will not tick again until applyAdvance returns
	// in other words execution of Advance ops cannot step on each other

	fmt.Printf("Srv %v-%v trying to advance to config %v... Config Data [Curr: %v, Next: %v].\n",
		       kv.gid, kv.me, op.Config.Num, currentConfig.Num, kv.next)

	// if nextConfig.Num is 1 ahead of kv.next
	// then increment kv.top so that Receive ops can populate the log beyond this Advance op
	// and increment kv.next to signal to clients
	// that this server is attempting to advance to the next config
	
	if kv.next == nextConfig.Num - 1 { 	// should always be the case
		kv.next += 1
		kv.top  += 1
	} else { 							// protocol is not functioning as anticipated
		fmt.Printf("\n\tSrv %v-%v tried to execute applyAdvance where kv.next != nextConfig.Num - 1.\n", 
			       kv.gid, kv.me)
		return
	}

	currentShards := currentConfig.Shards    // Shards is a 10 element array such that
	nextShards    := nextConfig.Shards       // the int64 stored at index i is the GID
        								     // which owns shard i (maps shard -> GID)

	toSend    := make([]ShardId, 0, NShards) // place holder slice
	toReceive := make([]ShardId, 0, NShards) // place holder slice

	for i := 0; i < NShards; i++ { 												// for i in range 0 to N-1
		if currentShards[i] == kv.gid && nextShards[i] != kv.gid { 				// if this server's group owns shard i 
																				// under the current config, but not the next
			fmt.Printf("Srv %v-%v must send shard %v to group %v.\n",  				
				       kv.gid, kv.me, ShardId{kv.config.Num, i}, nextShards[i])
			
			toSend = append(toSend, ShardId{currentConfig.Num, i}) 				// construct a ShardID corresponding to 
		}																		// this shard and append it to toSend
	}

	for i := 0; i < NShards; i++ { 																// for i in range 0 to N-1
		if currentShards[i] != 0 && currentShards[i] != kv.gid && nextShards[i] == kv.gid { 	// if shard i is assigned to a group 
																								// and under the current config
			fmt.Printf("Srv %v-%v must receive shard %v from group %v.\n",  					// this group does not own shard i, 
				       kv.gid, kv.me, ShardId{kv.config.Num, i}, currentShards[i]) 				// but it does own it in the next config
			
			toReceive = append(toReceive, ShardId{currentConfig.Num, i}) 						// construct a ShardID corresponding to this shard 
		}																						// and append it to toReceive
	}

	needShards := len(toReceive) > 0  	// a flag; if the server needs shards then no need to call Done
	count      := 0  					// count tracks how many times the server executes the following while loop before advancing

	for len(toSend) > 0 || len(toReceive) > 0 { 	// while there are still shards to send or shards to receive:

		// send phase:

		for i := 0; i < len(toSend); i++ { 														// for i in range 0 to len(toSend)-1:
			servers, exists := nextConfig.Groups[nextShards[toSend[i].Shard]] 					// retreive list of servers for the receiving group
			if exists { 																		// if the GID exsits in Groups
				if ok := kv.sendShard(toSend[i], servers, nextShards[toSend[i].Shard]); ok { 	// attempt to sendShard() and if successful:
					toSend = append(toSend[0:i], toSend[i+1:]...) 								// remove that sid from toSend
				}
			} else {
				fmt.Printf("\n\tSrv %v-%v tried to send a shard to an INVALID group...\n",
					       kv.gid, kv.me)
			}
		}
		
		// unlock and sleep to allow receipt of incoming shards

		fmt.Printf("Srv %v-%v idling to receive shards... toSend: %v, toReceive: %v.\n",
			       kv.gid, kv.me, toSend, toReceive)
		
		kv.mu.Unlock()
		time.Sleep(time.Duration(100 + rand.Intn(75)) * time.Millisecond) // this duration is critical
		kv.mu.Lock()

		// receive phase:

		// look Receive ops at the top of the log

		status, op := kv.px.Status(kv.top) 		// check status of next op in log
		// fmt.Printf("\n\t\t%v - %v...\n", op, status)
		if status == paxos.Decided { 			// if decided
	    	decided_op := op.(Op) 				// wrap Paxos op in DisKV Op
	    	if decided_op.Ty == "Receive" { 	// if op type is Receive
	    		kv.execute(decided_op, kv.top) 	// then execute the Receive
	    	}									// in order to move forward on applyAdvance
		}

		// update toReceive:

		for i := 0; i < len(toReceive); i++ { 							// for i in range 0 to len(toReceive)-1:
			if _, ok := kv.receives[toReceive[i]]; ok {					// check the cache for this sid
				toReceive = append(toReceive[0:i], toReceive[i+1:]...)	// if shard was received
			} 															// then remove it from toReceive
		}

		// fmt.Printf("Srv %v-%v finished checking cache for missing shards... updated toReceive: %v.\n",
		// 	       kv.gid, kv.me, toReceive)

		if len(toSend) > 0 || len(toReceive) > 0 { 		// if loop will need to repeat
														
			fmt.Printf("Srv %v-%v iterating applyAdvance loop. Count: %v... toSend: %v, toReceive: %v.\n", 
				       kv.gid, kv.me, count, toSend, toReceive)

			count += 1 									// increment count
		}
	} // end while loop; configuration successfully advanced

	kv.config = nextConfig 								// update kv.config

	fmt.Printf("\tSrv %v-%v has ADVANCED to configuration %v.\n",  	
		       kv.gid, kv.me, kv.config.Num) 	
	
	if !needShards && kv.px != nil {    // if there were no shards to receive
		kv.px.Done(seq)  				// then Advance will be the only op and the server needs to call Done()
	} 									// otherwise applyAndLogReceive() will call Done() for later seq numbers
}

// sendShard(sid, servers, gid) returns a boolean
// true:  successful transfer ... false: unsuccessful transfer
// helper function for applyAdvance, which delivers shards to other servers using RPC

func (kv *DisKV) sendShard(sid ShardId, servers []string, gid int64) bool {

	fmt.Printf("Srv %v-%v attempting to send shard %v to group %v.\n", kv.gid, kv.me, sid, gid)

	args := &ReceiveShardArgs{} 						// pointer to a ReceiveShardArgs struct
	args.Sid = sid 										// populate args with this sid and
	args.Data = make(map[string]string) 				// instantiate new maps for Data, Reads, and Writes
	args.Reads = make(map[int64]GetReply)
	args.Writes = make(map[int64]PutAppendReply)
	reply := &ReceiveShardReply{} 						// point to a ReceiveShardReply struct

	// package shard data, reads, and writes
	for k, v := range kv.data {
		if key2shard(k) == sid.Shard { 					// if this kv pair is stored on this shard:
			args.Data[k] = v 							// then store it in the kv data being sent 
		}
	}

	for k, v := range kv.reads { 						// package cached replies
		args.Reads[k] = v
	}

	for k, v := range kv.writes {
		args.Writes[k] = v
	}

	for _, s := range servers { 							// for each server in servers:
	
		ok := call(s, "DisKV.ReceiveShard", args, reply) 	// attempt RPC

		// reply.Err should not be an empty string, if the RPC succeeds

		if ok && reply.Err == "" {
			fmt.Printf("\nRecieveShard(%v) reply empty...\n", args.Sid)
		}

		if ok && reply.Err == OK {
			fmt.Printf("Srv %v-%v's attempt to send shard %v to group %v was successful.\n", 
				       kv.gid, kv.me, sid, gid)
			return true
		}
	}

	fmt.Printf("Srv %v-%v's attempt to send shard %v to group %v failed.\n",
		       kv.gid, kv.me, sid, gid)
	return false
}

// execution of a Request Op{} will cause the live server to
// transfer all of its state as of this seq # to the restarting server
// the restarting server will become a clone of the live server and 
// begin to participate fully as a client server and Paxos peer

func (kv *DisKV) logRequest(op Op, seq int) {
	var reply RequestLogReply					// instantiate a RequestLogReply

	log := make(map[int]Op) 					// instantiate a new log map

	for k, v := range kv.execLog { 				// populate log with ops in execLog
		log[k] = v
	}

	reply.Ckpt = kv.ckpt 						// build reply
	reply.Log = log 							// log may not be the same for each server
	reply.Err = OK 								// but, it had better produce the same database
	
	kv.requests[op.Xid] = reply 				// cache reply
	if kv.px != nil { 							// if not executing a cold restart
		kv.px.Done(seq) 						//    call px.Done()
	}
	kv.top += 1 								// increment top
}

// during hot restart:
// sequence numbers [0:kv.top-2] are populated with decided no-ops
// sequence number [kv.top-1] is populated with a decided Checkpoint op
// the op contains the data and state recovered from disk

// the aim is to prevent servers that have lost their Paxos logs from allowing
// other servers to claim sequence numbers that have already been decided, but lost

// the lagging server still needs some way of catching up
// the idea is to step through decided no-ops until reaching the Checkpoint op
// update state according to the Checkpoint and then rely on existing Paxos logs

func (kv *DisKV) applyCheckpoint(op Op, seq int) {

	kv.data = make(map[string]string) 						// re-initialize the existing database

	for k, v := range op.Data { 	  						// for (k, v) in the checkpoint data
		kv.data[k] = v 										// overwrite or add (k, v) to this server's data
		shard := key2shard(k) 								// persistently store
		err := kv.filePut(shard, k, v)
		if  err != nil {
			log.Fatalf("%v - Srv %v-%v Checkpoint.\n",
				       err, kv.gid, kv.me)
		}
	}

	kv.next = op.Next 										// update state
	kv.config = op.Config
	kv.ckpt = Ckpt { op.Data, kv.top, kv.config, kv.next } 	// overwrite kv.ckpt

	kv.execLog = make(map[int]Op) 							// re-initialize execLog

	if kv.px != nil {
		kv.px.Done(seq)
	}
	kv.top += 1 											// increment top
}

// proposeAdvance() puts an Advance op on the wire
// tick() will call proposeAdvance() whenever
// the server's config is behind the shardmaster's config

func (kv *DisKV) proposeAdvance(nextConfig shardmaster.Config) {

	fmt.Printf("Srv %v-%v proposing advance to configuration %v...\n",
		       kv.gid, kv.me, nextConfig.Num)

	proposal := Op{}
	proposal.Ty     = "Advance"
	proposal.Config = nextConfig
	kv.px.Start(kv.top, proposal)
}


// ask the shardmaster if there's a new configuration
// if so, re-configure

func (kv *DisKV) tick() {
	kv.mu.Lock()

	if kv.isdead() { 	// if server is dead
		return 			// ignore this tick
	}

	// if Decided, execute the operation at the top of the log

	status, op := kv.px.Status(kv.top) 		
	if status == paxos.Decided {
	    decided_op := op.(Op)
    	kv.execute(decided_op, kv.top) 
	}

	qconfig := kv.sm.Query(-1)                         // query the SM

	if kv.config.Num < qconfig.Num {			       // if this server is behind
		nextConfig := kv.sm.Query(kv.config.Num + 1)   // query for next config
		kv.proposeAdvance(nextConfig)             	   // propose advance op to the replica group
	}

	kv.mu.Unlock()
}

// kv.requestLog() is called by a restarting cold server
// the purpose of this function is to petition the other servers
// via RPC to add a Request op to their Paxos log
// requesting server remains offline until this succeeds
// only one server's cached reply will be used to restore the offline server

func (kv *DisKV) requestLog(servers []string) (map[int]Op, Ckpt) {

	args := &RequestLogArgs{nrand(), kv.me}							// instantiate args pointer	
	others := append(servers[0:kv.me], servers[kv.me+1:]...)		// a list of the other servers

	for {															// loop until RequestLog RPC succeeds
		for _, s := range others { 									// for each server
			reply := &RequestLogReply{}								//    instantiate reply pointer
			ok := call(s, "DisKV.RequestLog", args, reply)			//    send RPC
			if ok && reply.Err == OK {								//    if RPC succeeds
				return reply.Log, reply.Ckpt 						//       return log, ckpt
			}
		}

		time.Sleep(40 * time.Millisecond)							// sleep - slightly shorter than other RPC pause
	}
}

// Assignment Instructions - Please don't change the next four functions:
// - kv.kill()
// - kv.isdead()
// - kv.Setunreliable()
// - kv.isunreliable()

// tell the server to shut itself down

func (kv *DisKV) kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.l.Close()
	kv.px.Kill()
}

// call isdead() to find out if the server is dead

func (kv *DisKV) isdead() bool {
	return atomic.LoadInt32(&kv.dead) != 0
}

// make communication unreliable

func (kv *DisKV) Setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&kv.unreliable, 1)
	} else {
		atomic.StoreInt32(&kv.unreliable, 0)
	}
}

// call isunreliable() to find out if the server is unreliable

func (kv *DisKV) isunreliable() bool {
	return atomic.LoadInt32(&kv.unreliable) != 0
}

// StartServer() instantiates a DisKV server
// servers can either be new or restarting

func StartServer(gid int64, shardmasters []string, servers []string,
	             me int, dir string, restart bool) *DisKV {

	gob.Register(Op{}) // https://golang.org/pkg/encoding/gob/#Register

	kv := new(DisKV)   // instantiate a new DisKV structure

	// assign the fields

	kv.me 		= me 									// index in []servers					
	kv.gid 		= gid 									// group ID
	kv.dir 		= dir 									// directory for persistant storage
	kv.config   = shardmaster.Config{} 					// assign an empty Config {0, [0...], map[]}
	kv.data     = make(map[string]string)				// instantiate a data map
	kv.reads    = make(map[int64]GetReply) 				// instantiate a reads map
	kv.writes   = make(map[int64]PutAppendReply) 		// instantiate a writes map
	kv.receives = make(map[ShardId]ReceiveShardReply) 	// instantiate a receives map
	kv.requests = make(map[int64]RequestLogReply)		// instantiate a requests map
	kv.top      = 0 									// pointer to the top of the Paxos log
	kv.next     = 0 									// flag for reconfigure (why not boolean?)
	kv.execLog  = make(map[int]Op) 						// instantiate a map seq number -> executed op
	kv.ckpt     = Ckpt{} 								// instantiate a nil checkpoint

	rpcs := rpc.NewServer() // https://golang.org/pkg/net/rpc/#NewServer
	rpcs.Register(kv)       // https://golang.org/pkg/net/rpc/#Server.Register

	kv.sm = shardmaster.MakeClerk(shardmasters) 		// start a shardmaster client
	kv.px = nil											// initial value for Paxos client
														// if kv.px is nil during execution 
														// then server is restarting

	if restart {												// if server is restarting

		if _, err := os.Stat(kv.dir + "/state"); err != nil { 	//    if the disk was lost 

			fmt.Printf("\tSrv %v-%v LOST DISK, restarting cold...\n", 
				       kv.gid, kv.me)

			serversCopy := make([]string, len(servers), len(servers))

			for i, s := range servers { 					// deep copy to avoid passing a reference variable
				serversCopy[i] = s
			}

			log, ckpt := kv.requestLog(serversCopy) 		// get a copy of the log (requires a live majority)

			// update state variables to match received checkpoint

			kv.ckpt   = ckpt
			kv.config = ckpt.Config
			kv.top    = ckpt.Top
			kv.next   = ckpt.Next

			if ckpt.Data == nil { 							// a check against null pointer error
				ckpt.Data = make(map[string]string) 		// instantitate a new map if necessary
			}

			for k, v := range ckpt.Data { 					// update the database
				kv.data[k] = v 
			}

			for k, v := range kv.data { 					// persistently store
					shard := key2shard(k)
					kv.filePut(shard, k, v)
			}

			for i := ckpt.Top; i < ckpt.Top + len(log); i++ {		// while there are still ops in the log
				if log[i].Ty != "Request" { 						//    if not a Request op
					kv.execute(log[i], i)							//       execute the op
				} else { 											//    else is a Request op
					kv.execute(Op{}, i) 	 						//       execute a no-op
				}
			}

			kv.px = paxos.Make(servers, me, rpcs) 					// start a Paxos client

			// populate Paxos instance log with the decided ops

			for i := ckpt.Top; i < ckpt.Top + len(log); i++ {
				kv.px.Instances[i] = paxos.Instance { paxos.ProposalId{}, paxos.ProposalId{}, log[i], paxos.Decided }
			}

		} else { 												// else the disk was found

			fmt.Printf("\tSrv %v-%v FOUND DISK, restarting hot...\n", 
				       kv.gid, kv.me)

			// restore the database stored on the disk

			for shard := 0; shard < NShards; shard++ {
				data := kv.fileReadShard(shard)
				for k, v := range data {
					kv.data[k] = v
				}
			}

			// restore state stored on the disk

			var err error 									     	// instantiate an error variable

			top, config, next, err := kv.retrieveState() 			// get state from disk

			if err != nil { 										// if unable to obtain state
				log.Fatalf("%v - retrieveState failed for Srv %v-%v.\n", err, kv.gid, me)
			}

			// update state

			kv.top = top
			kv.config = config
			kv.next = next

			// make a deep copy of the database to avoid issues with reference variables

			ckptData := make(map[string]string)

			for k, v := range kv.data {
				ckptData[k] = v
			}

			kv.ckpt = Ckpt { ckptData, top, config, next }  // set checkpoint

			kv.px = paxos.Make(servers, me, rpcs) 			// start a Paxos client

			// populate the log with no-ops up to the last entry
			for i := 0; i < top - 1; i++ {
				kv.px.Instances[i] = paxos.Instance { paxos.ProposalId{}, paxos.ProposalId{}, Op{}, paxos.Decided }
			}

			// make the last entry a checkpoint entry to replace the database
			ckptOp := Op{}
			ckptOp.Ty = "Checkpoint"
			ckptOp.Data = ckptData
			ckptOp.Config = config
			ckptOp.Next = next

			kv.px.Instances[top - 1] = paxos.Instance { paxos.ProposalId{}, paxos.ProposalId{}, ckptOp, paxos.Decided }
		}

	} else { 											// else fresh start (!restart)
		kv.px = paxos.Make(servers, me, rpcs) 			//    start a Paxos client
	}

	// log.SetOutput(os.Stdout) 						// ???

	// *** need an explanation for the following block:

	os.Remove(servers[me])
	l, e := net.Listen("unix", servers[me])
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	kv.l = l

	// please do not change any of the following code, or do anything to subvert it

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
				fmt.Printf("DisKV(%v) accept: %v\n", me, err.Error())
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

// EOF