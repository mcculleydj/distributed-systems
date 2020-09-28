package mapreduce

import "container/list"
import "fmt"
import "strconv"
import "strings"

// WorkerInfo is not used in this implementation
type WorkerInfo struct {   
    address string
}

// Clean up all workers by sending a Shutdown RPC to each one of them
// Collect the number of jobs each worker has performed.
func (mr *MapReduce) KillWorkers(n int, prefix string) *list.List {
    
    workers := list.New()
    replies := list.New()
    for i := 0; i <= n; i++ {
        workers.PushBack(prefix + strconv.Itoa(i))
    }

    /* provided implentation:
       l := list.New()
       for _, w := range mr.Workers {
    */
    
    for e := workers.Front(); e != nil; e = e.Next() {
        DPrintf("DoWork: shutdown %s\n", e.Value)
        args := &ShutdownArgs{}
        var reply ShutdownReply
        
        /* provided implementation:
           ok := call(w.address, "Worker.Shutdown", args, &reply)
        */
        
        ok := call(e.Value.(string), "Worker.Shutdown", args, &reply)
        if ok == false {
        
        /* provided implementation:
           fmt.Printf("DoWork: RPC %s shutdown error\n", w.address)
        */
        
            fmt.Printf("DoWork: RPC %s shutdown error\n", e.Value)
        } else {
            fmt.Printf("DoWork: RPC %s shutdown successful\n", e.Value)
            replies.PushBack(reply.Njobs)
        }
    }
    return replies
}

func (mr *MapReduce) RunMaster() *list.List {

    // global variables used as parameters to KillWorkers()
    var latestWorkerNum int
    var prefix string
    
    // channel initialization
    mapJobs     := make(chan int, mr.nMap)
    reduceJobs  := make(chan int, mr.nReduce)
    idleWorkers := make(chan string)
    
    // worker RPC to complete a map task:
    doMap := func(worker string, jobNumber int) bool {
        args := DoJobArgs{mr.file, Map, jobNumber, mr.nReduce}
        var reply DoJobReply
        return call(worker, "Worker.DoJob", args, &reply)
    }

    // worker RPC to complete a reduce task:
    doReduce := func(worker string, jobNumber int) bool {
        args := DoJobArgs{mr.file, Reduce, jobNumber, mr.nMap}
        var reply DoJobReply
        return call(worker, "Worker.DoJob", args, &reply)
    }

    // listen for an available worker and try to assign the job until complete
    assign := func(jobNumber int, t JobType) {
        for {
            var worker string
            var done bool
            
            select {
            case worker = <-idleWorkers:
            case worker = <-mr.registerChannel:
                
                /*
                // this approach causes runtime errors
                // in the many failures test:

                mr.Workers[worker] = &WorkerInfo{worker}
                
                // what if I use .new()?

                info := new(WorkerInfo)
                info.address = worker
                mr.Workers[worker] = info
                
                // unfortunately, this also results in errors
                // I really need to dissect what is happening
                
                // final thought: stop storing pointers
                // also changed mapreduce.go 
                // (changed mapreduce.go back when this didn't fix problem)
                
                info := WorkerInfo{worker}
                mr.Workers[worker] = info
                */
                
                // new idea: 
                // simply build a list of seen workers at the end w/in KillWorkers()
                
                // print workers as they come off the mr.registerChannel 
                fmt.Println("Registering worker: " + worker)

                // get the prefix before the worker number
                split := strings.SplitAfter(worker, "worker")
                prefix = split[0]
                
                // get the worker number as a string
                workerNum := split[1]
                
                // convert string to an int for use in a loop
                thisWorkerNum, _ := strconv.Atoi(workerNum)
                
                // update latestWorkerNum as required
                if thisWorkerNum > latestWorkerNum {
                    latestWorkerNum = thisWorkerNum
                }
            }
            
            // assign a map or reduce job as appropriate
            if t == Map {
                done = doMap(worker, jobNumber)
            } else {
                done = doReduce(worker, jobNumber)
            }
            
            // if successful return the worker to the pool
            // place the job on the completed jobs queue to unblock
            if done {
                if t == Map {
                    mapJobs <- jobNumber
                } else {
                    reduceJobs <- jobNumber
                }
                idleWorkers <- worker
                return
            } else {
                // RPC failed so worker is considered failed:
                fmt.Printf("Worker failed: %s\n", worker)
                // delete(mr.Workers, worker)
            }
            
            /*
            should we remove the worker from the map if it fails? probably...
            delete(Workers, worker) would accomplish this
            still workers can fail, without detection because they execute their
            10th and final RPC, without executing an 11th before 
            KillWorkers() calls the shutdown
            
            are we responsible for cleaning up all workers activated by test_test.go?
            if so would need to update Workers ahead of call to KillWorkers()
            */
        }
    }

    // create a thread for each map job
    for j := 0; j < mr.nMap; j++ {
        go assign(j, Map)
    }

    // block until all map jobs are complete
    for i := 0; i < mr.nMap; i++{
        <- mapJobs
    }

    // create a thread for each reduce job  
    for j := 0; j < mr.nReduce; j++ {
        go assign(j, Reduce)
    }

    // block until all reduce jobs are complete
    for i := 0; i < mr.nReduce; i++{
        <- reduceJobs
    }

    // report the ID of the last worker assigned
    fmt.Printf("The last worker assigned work was: %v\n", latestWorkerNum)
    
    // modified to use locally generated parameters
    // instead of the Workers map in the mapreduce struct
    return mr.KillWorkers(latestWorkerNum, prefix)
}
