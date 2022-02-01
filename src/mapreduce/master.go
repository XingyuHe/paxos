package mapreduce

import (
	"container/list"
	"fmt"
	"log"
)

type WorkerInfo struct {
    address string
	id int
    // You can add definitions here.
}


// Clean up all workers by sending a Shutdown RPC to each one of them Collect
// the number of jobs each work has performed.
func (mr *MapReduce) KillWorkers() *list.List {
    l := list.New()
    for _, w := range mr.Workers {
        DPrintf("DoWork: shutdown %s\n", w.address)
        args := &ShutdownArgs{}
        var reply ShutdownReply;
        ok := call(w.address, "Worker.Shutdown", args, &reply)
        if ok == false {
            fmt.Printf("DoWork: RPC %s shutdown error\n", w.address)
        } else {
            l.PushBack(reply.Njobs)
        }
    }
    return l
}

/* 

return only when all of the amp and reduce tasks have been
executed


Read from new worker's infromation by reading mr.registerChannel

Modify the MapReduce struct to keep track of any additional state (e.g., the 
set of available workers), and initialize this additional state in the 
InitMapReduce() function.

Wiat for a worker to finish before it can hand out more jobs
- use channel to do that

Add worker info into mr.workers

Solution: 
1. Know whether a worker is ready
	a. know this by waiting for mr.registerChannel
	b. the channel also contains the name of the worker
2. Once it is ready send an rpc to that worker with arguments
	a. what function to call? 
		wk.Map = MapFunc
		wk.Reduce = ReduceFunc

	b. How does Master know which worker to send the signal to? 
		mr.registerChannel
3. If the rpc call succeeds, then update the worker information
	a. How do I know that the worker already finishes executing? 

4. How to distribute the task? 
	a. read files from MapReduce.Split()

5. Do I need to wait for all workers to send you a signal? Where does worker send a signal
	a. Wait when calling mapfn, 
	b. Use .Done to check if the workers have finished

6. Show I use synchronous or asynchronous call
	a. try asynchronous first


Actual design: 
	1. RunMaster
		1. wait for a signal in a go routine
		2. process texts in a loop
			1. open files
			2. call map function asynchronously
			3. save divCall
		3. wait for .Done in a loop
		4. 
*/

type RunJobArgs struct {
	jobIdx int
	finish chan int
	fail chan *RunJobArgs
	mr *MapReduce
	jobType string
}

func runJob (args *RunJobArgs) {
	worker_name := <- args.mr.registerChannel
	log.Printf("[RunMaster]: worker name %s", worker_name)
	log.Printf("[RunMaster]: %s JobNumber %d", args.jobType, args.jobIdx)

	numOtherPhase := args.mr.nReduce
	if args.jobType == "Reduce" {
		numOtherPhase = args.mr.nMap
	}
		
	var reply DoJobReply
	log.Printf("[RunMaster]: Go Routine: mapJobNumber %d", args.jobIdx)
	callArgs := &DoJobArgs{
		File: args.mr.file,
		Operation: JobType(args.jobType),
		JobNumber: args.jobIdx,
		NumOtherPhase: numOtherPhase,
	} // TODO: need to calculate number of other jobs
	log.Printf("[RunMaster]: args JobNumber %d\n", callArgs.JobNumber)

	ok := call(worker_name, "Worker.DoJob", callArgs, &reply)
	if !ok {
		fmt.Printf("[RunMaster] Register: RPC %s register error\n", worker_name)
		args.fail <- args
		return 
	}
	log.Printf("[RunMaster]: Finish %s JobNumber %d\n", args.jobType, args.jobIdx)
	args.finish <- 1
	args.mr.registerChannel<- worker_name
}

func (mr *MapReduce) RunMaster() *list.List {

	log.Printf("[RunMaster]: filename %s", mr.file)
	mapFinish := make(chan int, mr.nMap)
	reduceFinish := make(chan int, mr.nReduce)

	jobFail := make(chan *RunJobArgs)

	go func() {
		for {
			jobArgs := <- jobFail
			go runJob(jobArgs)
		}
	}()

		
	// map
	for jobIdx := 0; jobIdx < mr.nMap; jobIdx++{
		jobArgs := &RunJobArgs{jobIdx, mapFinish, jobFail, mr, "Map"}
		go runJob(jobArgs)
	}

	finishCnt := 0
	log.Printf("[RunMaster]: nMap %d\n", mr.nMap)
	for i := 0; i < mr.nMap; i++ {
		finishCnt += <- mapFinish
		log.Printf("[RunMaster]: map finishCnt %d\n", finishCnt)
	}
	close(mapFinish)


	// reduce
	for jobIdx := 0; jobIdx < mr.nReduce; jobIdx++{
		jobArgs := &RunJobArgs{jobIdx, reduceFinish, jobFail, mr, "Reduce"}
		go runJob(jobArgs)
	}

	finishCnt = 0
	log.Printf("[RunMaster]: nReduce %d\n", mr.nReduce)
	for i := 0; i < mr.nReduce; i++ {
		finishCnt += <- reduceFinish 
		log.Printf("[RunMaster]: reduce finishCnt %d\n", finishCnt)
	}
	close(reduceFinish)
	return mr.KillWorkers()
}
