package pbservice

import "net"
import "fmt"
import "net/rpc"
import "log"
import "time"
import "viewservice"
import "sync"
import "sync/atomic"
import "os"
import "syscall"
import "math/rand"

type PBServer struct {
	mu         			  sync.Mutex
	l          			  net.Listener
	dead       			  int32
	unreliable 			  int32
	me         			  string
	vs         			  *viewservice.Clerk
	view 				  viewservice.View
	isPrimary			  bool
	isBackup			  bool
	kvs					  map[string]string
	putAppendsProcessed   map[int64]bool
	// getReplies  		  map[int64]string
}

func (pb *PBServer) GetFromBackup(args *GetFromBackupArgs, reply *GetReply) error {
	pb.mu.Lock()
	defer pb.mu.Unlock()

	if !pb.isBackup {
		reply.Err = ErrWrongServer
		return nil
	}

	if pb.view.Viewnum != args.View.Viewnum {
		reply.Err = ErrWrongServer
		return nil
	}

	// if v, ok := pb.getReplies[args.Id]; ok { 
	// 	reply.Value = v
	//  	reply.Err = OK
	//  	return nil
	// }

	if v, ok := pb.kvs[args.Key]; ok {
		// pb.getReplies[args.Id] = reply.Value
		reply.Value = v
		reply.Err = OK
	} else {
		reply.Err = ErrNoKey
	}

	return nil
}

func (pb *PBServer) Get(args *GetArgs, reply *GetReply) error {

	pb.mu.Lock()
	defer pb.mu.Unlock()

	if !pb.isPrimary {
		reply.Err = ErrWrongServer
		return nil
	}

	// if v, ok := pb.getReplies[args.Id]; ok { 
	// 	reply.Value = v
	// 	reply.Err = OK
	// 	return nil
	// }

	if pb.view.Backup != "" {
		args2 := GetFromBackupArgs{args.Id, args.Key, pb.view}
		var replyFromBackup GetReply
		
		if call(pb.view.Backup, "PBServer.GetFromBackup", args2, &replyFromBackup) {
			if replyFromBackup.Err == OK {
				// if pb.kvs[args.Key] == replyFromBackup.Value {
					// pb.getReplies[args.Id] = replyFromBackup.Value
					reply.Err = OK
					reply.Value = replyFromBackup.Value
				// } else {
				// 	fmt.Println("Backup had a different value for Get(k)")
				// }
			}
		}
	} else if v, ok := pb.kvs[args.Key]; ok {
		reply.Value = v
		reply.Err = OK
		// pb.getReplies[args.Id] = v
	} else if _, ok := pb.kvs[args.Key]; !ok {
		reply.Err = ErrNoKey
	}
	
	return nil
}

func (pb *PBServer) PutAppendToBackup(args *PutAppendToBackupArgs, reply *PutAppendReply) error {
	pb.mu.Lock()
	defer pb.mu.Unlock()

	if !pb.isBackup {
		reply.Err = ErrWrongServer
		return nil
	}

	if args.View.Viewnum != pb.view.Viewnum {
		reply.Err = ErrWrongServer
		return nil
	}

	if pb.putAppendsProcessed[args.Id] { 
		reply.Err = OK
		return nil
	}

	if args.Op == "Put" {
		pb.kvs[args.Key] = args.Value
		reply.Err = OK
		pb.putAppendsProcessed[args.Id] = true
	} else if args.Op == "Append" {
		pb.kvs[args.Key] += args.Value
		reply.Err = OK
		pb.putAppendsProcessed[args.Id] = true
	}

	return nil
}	

func (pb *PBServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	pb.mu.Lock()
	defer pb.mu.Unlock()

	if !pb.isPrimary {
		reply.Err = ErrWrongServer
		return nil
	}

	if pb.putAppendsProcessed[args.Id] { 
		reply.Err = OK
		return nil
	}

	args2 := PutAppendToBackupArgs{args.Id, args.Key, args.Value, args.Op, pb.view}
	var replyFromBackup PutAppendReply

	if args.Op == "Put" {
		if pb.view.Backup != "" {
			if call(pb.view.Backup, "PBServer.PutAppendToBackup", args2, &replyFromBackup) {
				if replyFromBackup.Err == OK {
					pb.kvs[args.Key] = args.Value
					pb.putAppendsProcessed[args.Id] = true
					reply.Err = OK
				}
			}
		} else {
			pb.kvs[args.Key] = args.Value
			pb.putAppendsProcessed[args.Id] = true
			reply.Err = OK
		}
	} else if args.Op == "Append" {
		if pb.view.Backup != "" {
			if call(pb.view.Backup, "PBServer.PutAppendToBackup", args2, &replyFromBackup) {
				if replyFromBackup.Err == OK {
					pb.kvs[args.Key] += args.Value
					pb.putAppendsProcessed[args.Id] = true
					reply.Err = OK
				}
			}
		} else {
			pb.kvs[args.Key] += args.Value
			pb.putAppendsProcessed[args.Id] = true
			reply.Err = OK
		}
	}

	return nil
}

func (pb *PBServer) InitializeBackup(args *InitializeBackupArgs, reply *InitializeBackupReply) error {
	pb.mu.Lock()
	defer pb.mu.Unlock()

	if pb.view.Backup != pb.me {
		reply.Err = ErrWrongServer
	} else {
		pb.isBackup = true
		pb.kvs = args.Kvs
		reply.Err = OK
	}

	return nil
}

func (pb *PBServer) initializeBackup(backup string) {
	// primary is locked by tick

	args := InitializeBackupArgs{pb.kvs}
	var reply InitializeBackupReply

	for reply.Err != OK {
		call(backup, "PBServer.InitializeBackup", args, &reply)
	}

	return
}

// ping the viewserver periodically.
// if view changed:
//   transition to new view.
//   manage transfer of state from primary to new backup.

func (pb *PBServer) tick() {
	// lock server requesting view
	pb.mu.Lock()
	defer pb.mu.Unlock()

	view, err := pb.vs.Ping(pb.view.Viewnum)
	
	if err != nil {
		return
	}

	// if you are the current primary and you discover a new backup
	// transfer state before doing anything else

	if view.Backup != pb.view.Backup && view.Backup != "" && pb.me == view.Primary {
		pb.initializeBackup(view.Backup)
	}

	pb.isPrimary = (view.Primary == pb.me)


	if view.Backup != pb.me {
		pb.isBackup = false
	}

	pb.view = view

	return
}

// tell the server to shut itself down.
// please do not change these two functions.
func (pb *PBServer) kill() {
	atomic.StoreInt32(&pb.dead, 1)
	pb.l.Close()
}

// call this to find out if the server is dead.
func (pb *PBServer) isdead() bool {
	return atomic.LoadInt32(&pb.dead) != 0
}

// please do not change these two functions.
func (pb *PBServer) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&pb.unreliable, 1)
	} else {
		atomic.StoreInt32(&pb.unreliable, 0)
	}
}

func (pb *PBServer) isunreliable() bool {
	return atomic.LoadInt32(&pb.unreliable) != 0
}

func StartServer(vshost string, me string) *PBServer {
	pb := new(PBServer)
	pb.me = me
	pb.vs = viewservice.MakeClerk(me, vshost)
	pb.view = viewservice.View{}
	pb.kvs = make(map[string]string)
	pb.isPrimary = false
	pb.isBackup = false
	pb.putAppendsProcessed = make(map[int64]bool)
	// pb.getReplies = make(map[int64]string)

	rpcs := rpc.NewServer()
	rpcs.Register(pb)

	os.Remove(pb.me)
	l, e := net.Listen("unix", pb.me)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	pb.l = l

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for pb.isdead() == false {
			conn, err := pb.l.Accept()
			if err == nil && pb.isdead() == false {
				if pb.isunreliable() && (rand.Int63()%1000) < 100 {
					// discard the request.
					// fmt.Println("discard the request\n")
					conn.Close()
				} else if pb.isunreliable() && (rand.Int63()%1000) < 200 {
					// process the request but force discard of reply.
					// fmt.Println("process the request but force discard of reply\n")
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
			if err != nil && pb.isdead() == false {
				fmt.Printf("PBServer(%v) accept: %v\n", me, err.Error())
				pb.kill()
			}
		}
	}()

	go func() {
		for pb.isdead() == false {
			pb.tick()
			time.Sleep(viewservice.PingInterval)
		}
	}()

	return pb
}
