package viewservice

import "net"            // Listener
import "net/rpc"        // NewServer(); Register(); ServeConn()
import "log"            // Fatal()
import "time"           // Time; Now(); Sub(); Sleep()
import "sync"           // Mutex; Lock(); Unlock()
import "fmt"            // Printf()
import "os"             // Remove()
import "sync/atomic"    // StoreInt32(); AddInt32(); LoadInt32()

type ViewServer struct {
    mu               sync.Mutex              // locking
    l                net.Listener            // RPC listner
    dead             int32                   // for testing
    rpccount         int32                   // for testing
    me               string                  // host addr
    view             View                    // current view
    lastPing         map[string]time.Time    // ping tracking
    viewAcked        bool                    // ack flag
}

// server Ping RPC handler.
func (vs *ViewServer) Ping(args *PingArgs, reply *PingReply) error {
    
    // lock view server
    vs.mu.Lock()
    
    // record (vck, time) of the incoming ping
    vs.lastPing[args.Me] = time.Now()
    
    // update viewAcked
    if args.Me == vs.view.Primary && args.Viewnum == vs.view.Viewnum {
        vs.viewAcked = true
    }

    // only update view if current view was acked
    if vs.viewAcked {

        // niether a primary or backup exists
        if vs.view.Primary == "" && vs.view.Backup == "" {
            
            // assign primary
            vs.view.Primary = args.Me
            
            vs.view.Viewnum += 1
            vs.viewAcked = false

        // ping is not from primary and a backup does not exist
        } else if vs.view.Primary != args.Me && vs.view.Backup == "" {
            
            // assign new backup
            vs.view.Backup = args.Me
            
            vs.view.Viewnum += 1
            vs.viewAcked = false

        // primary crashes
        } else if vs.view.Primary == args.Me && args.Viewnum == 0 {

            vs.view.Primary = ""

            // promote backup if able
            if vs.view.Backup != "" {
                vs.view.Primary = vs.view.Backup
                vs.view.Backup = ""

                // search for new backup
                for server, t := range vs.lastPing {
                    if (time.Now().Sub(t)) < (DeadPings * PingInterval) {
                        if server != args.Me && server != vs.view.Primary {
                            vs.view.Backup = server
                        }
                    }
                }
            }
            
            vs.view.Viewnum += 1
            vs.viewAcked = false
        
        // backup crashes
        } else if vs.view.Backup == args.Me && args.Viewnum == 0 {

            vs.view.Backup = ""
            
            // search for a new backup
            for server, t := range vs.lastPing {
                if (time.Now().Sub(t)) < (DeadPings * PingInterval) {
                    if server != args.Me && server != vs.view.Primary {
                        vs.view.Backup = server
                    }
                }
            }

            vs.view.Viewnum += 1
            vs.viewAcked = false

        }
    }

    // reply with current or updated view
    reply.View = vs.view

    // unlock view server
    vs.mu.Unlock()

    return nil
}

// server Get() RPC handler.
func (vs *ViewServer) Get(args *GetArgs, reply *GetReply) error {
    
    // lock view server
    vs.mu.Lock()

    // reply with current view
    reply.View = vs.view
    
    // unlock view server
    vs.mu.Unlock()

    return nil
}

// tick() is called once per PingInterval;
// it should notice if servers have died or recovered
// and change the view accordingly.

func (vs *ViewServer) tick() {

    // lock view server
    vs.mu.Lock()

    // check each (server, time)
    for server, t := range vs.lastPing {

        // if server timed out
        if (time.Now().Sub(t)) > (DeadPings * PingInterval) {

            // primary timed out and view can change
            if server == vs.view.Primary && vs.viewAcked {

                oldPrimary := server
                vs.view.Primary = ""

                // promote backup if able
                if vs.view.Backup != "" {
                    vs.view.Primary = vs.view.Backup
                    vs.view.Backup = ""

                    // search for new backup
                    for replServer, replT := range vs.lastPing {
                        if (time.Now().Sub(replT)) < (DeadPings * 2 * PingInterval) {
                            if replServer != vs.view.Primary && replServer != oldPrimary {
                                vs.view.Backup = replServer
                            }
                        }
                    }
                }

                vs.view.Viewnum += 1
                vs.viewAcked = false
            
            // backup timed out and view can change
            } else if server == vs.view.Backup && vs.viewAcked {

                oldBackup := server
                vs.view.Backup = ""

                // search for a new backup
                for replServer, replT := range vs.lastPing {
                    if (time.Now().Sub(replT)) < (DeadPings * 2 * PingInterval) {
                        if replServer != vs.view.Primary && replServer != oldBackup {
                            vs.view.Backup = replServer
                        }
                    }
                }
                
                vs.view.Viewnum += 1
                vs.viewAcked = false
            }
        }
    }

    // unlock view server
    vs.mu.Unlock()
}

// tell the server to shut itself down.
// for testing.
// please don't change these two functions.
func (vs *ViewServer) Kill() {
    atomic.StoreInt32(&vs.dead, 1)
    vs.l.Close()
}

// has this server been asked to shut down?
func (vs *ViewServer) isdead() bool {
    return atomic.LoadInt32(&vs.dead) != 0
}

// please don't change this function.
func (vs *ViewServer) GetRPCCount() int32 {
    return atomic.LoadInt32(&vs.rpccount)
}

func StartServer(me string) *ViewServer {
    vs := new(ViewServer)
    vs.me = me
    vs.view = View{}
    vs.lastPing = make(map[string]time.Time)
    vs.viewAcked = true // starts as true for to register first primary
    
    // tell net/rpc about our RPC server and handlers.
    rpcs := rpc.NewServer()
    rpcs.Register(vs)

    // prepare to receive connections from clients.
    // change "unix" to "tcp" to use over a network.
    os.Remove(vs.me) // only needed for "unix"
    l, e := net.Listen("unix", vs.me)
    if e != nil {
        log.Fatal("listen error: ", e)
    }
    vs.l = l

    // please don't change any of the following code,
    // or do anything to subvert it.

    // create a thread to accept RPC connections from clients.
    go func() {
        for vs.isdead() == false {
            conn, err := vs.l.Accept()
            if err == nil && vs.isdead() == false {
                atomic.AddInt32(&vs.rpccount, 1)
                go rpcs.ServeConn(conn)
            } else if err == nil {
                conn.Close()
            }
            if err != nil && vs.isdead() == false {
                fmt.Printf("ViewServer(%v) accept: %v\n", me, err.Error())
                vs.Kill()
            }
        }
    }()

    // create a thread to call tick() periodically.
    go func() {
        for vs.isdead() == false {
            vs.tick()
            time.Sleep(PingInterval)
        }
    }()

    return vs
}
