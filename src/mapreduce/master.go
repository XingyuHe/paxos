package mapreduce

import (
	"container/list"
	"fmt"
	"log"
	"net/rpc"
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

func (mr *MapReduce) RunMaster() *list.List {

	id := 0
	log.Printf("[RunMaster]: filename %s", mr.file)

	mapJobNumber := mr.nMap - 1 
	mapJobNumberSem := make(chan int, 1)

	reduceJobNumber := mr.nReduce - 1 
	reduceJobNumberSem := make(chan int, 1)

	// wait for map to finish
	mapCall := make([]chan *rpc.Call, mr.nMap)
	for i := range mapCall {
		mapCall[i] = make(chan *rpc.Call, 10)
	}

	mapFinish := make(chan int)

	go func() {
		for jobIdx := 0; jobIdx < mr.nMap; jobIdx++ {
			<- mapCall[jobIdx]
		}
		mapFinish <- 1
	}()


	// wait for reduce to finish
	reduceCall := make([]chan *rpc.Call, mr.nMap)
	for i := range reduceCall {
		reduceCall[i] = make(chan *rpc.Call, 10)
	}

	reduceFinish := make(chan int)

	go func() {
		for jobIdx := 0; jobIdx < mr.nMap; jobIdx++ {
			<- reduceCall[jobIdx]
		}
		reduceFinish <- 1
	}()

	for {
		// Your code here
		worker_name := <- mr.registerChannel
		log.Printf("[RunMaster]: receive signal from worker %s", worker_name)

		// initialize worker state
		mr.Workers[worker_name] = &WorkerInfo{address: worker_name, 
												id: id}

		client, _ := rpc.Dial("unix", worker_name)
		// error(err) // TODO: not sure if this line is necessary
		defer client.Close()


		log.Printf("[RunMaster]: successfully dial worker server %s", worker_name)
		
		go func() {
			// map

			for {

				mapJobNumberSem <- 1

				log.Printf("[RunMaster]: map loop: mapJobNumber %d", mapJobNumber)
				
				if (mapJobNumber < 0) {
					<- mapJobNumberSem
					log.Printf("[RunMaster]: map loop: no more map jobs left")
					break
				}
				currJobNumber := mapJobNumber
				mapJobNumber--
				<- mapJobNumberSem

				var reply DoJobReply
				args := &DoJobArgs{
					File: mr.file,
					Operation: "Map",
					JobNumber: currJobNumber,
					NumOtherPhase: mr.nReduce,
				} // TODO: need to calculate number of other jobs

				mapCall = append(mapCall, make(chan *rpc.Call, 10))
				client.Go("Worker.DoJob", args, &reply, mapCall[len(mapCall) - 1])
			}

			<- mapFinish
			// reduce
			for {

				reduceJobNumberSem <- 1
				if (reduceJobNumber < 0) {
					<- reduceJobNumberSem
					break
				}
				currJobNumber := reduceJobNumber
				reduceJobNumber--
				<- reduceJobNumberSem

				var reply DoJobReply
				args := &DoJobArgs{
					File: mr.file,
					Operation: "Reduce",
					JobNumber: currJobNumber,
					NumOtherPhase: mr.nMap,
				} // TODO: need to calculate number of other jobs
				client.Go("Worker.DoJob", args, &reply, reduceCall[currJobNumber])
			}

		}()



	}
	<- reduceFinish
	mr.DoneChannel <- true
	return mr.KillWorkers()

}
