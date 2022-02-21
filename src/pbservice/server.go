package pbservice

import (
	// "errors"
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/rpc"
	"os"
	"strconv"
	"sync"
	"syscall"
	"time"
	"viewservice"
)

//import "strconv"

// Debugging
const Debug = 0
const PutCompleteMask = 1
const ForwardCompleteMask = 2

func DPrintf(format string, a ...interface{}) (n int, err error) {
  if Debug > 0 {
    n, err = fmt.Printf(format, a...)
  }
  return
}

type PBServer struct {
  l net.Listener
  dead bool // for testing
  unreliable bool // for testing
  me string
  vs *viewservice.Clerk
  done sync.WaitGroup
  finish chan interface{}
  // Your declarations here.
	lock sync.Mutex

	data map[string]string
	putReplyLog map[int64]PutReply
	getReplyLog map[int64]GetReply
	view viewservice.View
	opCount int64
}

func (pb *PBServer) getPrimary() string {
	return pb.view.Primary
}
func (pb *PBServer) getBackup() string {
	return pb.view.Backup
}
func (pb *PBServer) isPrimary() bool {
	return pb.getPrimary() == pb.me
}
func (pb *PBServer) isBackup() bool {
	return pb.getBackup() == pb.me
}
func (pb *PBServer) hasBackup() bool {
	return pb.getBackup() != ""
}
func (pb *PBServer) isIdle() bool {
	return !pb.isBackup() && !pb.isPrimary()
}
func (pb *PBServer) getPutDataVal(args *PutArgs) string {
	if !args.DoHash {
		return args.Value
	} else {
		oldVal := ""
		oldVal, _ = pb.data[args.Key]

		hashRes := strconv.Itoa(int(hash(oldVal + args.Value)))

		// log.Printf("[getPutDataVal]: %s + %s = %s", oldVal, args.Value, hashRes)

		return hashRes
	}
}
func (pb *PBServer) putData(args *PutArgs) {

	pb.data[args.Key] = pb.getPutDataVal(args)
	// log.Printf("[PutData]: finished %s, %s", args.Key, args.Value)
}
func (pb *PBServer) getData(key string) string {
	return pb.data[key]
}

// bits
func (pb *PBServer) GetCompleteBits() {

}

// printing
func (pb *PBServer) printfData() {
	for k, v := range pb.data {
		log.Printf("\t(%s, %s)", k, v)
	}
	log.Printf("\t[PrintfData] end")
}

/*
protocol:
1. send the call to back up
2. wait for backup to reply
3. if backup replies with error
	1. return that error to client
	2. return value
*/

func (pb *PBServer) ForwardPut(args *ForwardPutArgs, reply *PutReply) error {
	pb.lock.Lock(); defer pb.lock.Unlock()
	// log.Printf("[ForwardPut]: (%s, %s) at server %s, %d", args.Key, args.Value, pb.me, args.PutID)

	if !pb.isBackup() {
		// log.Printf("[ForwardPut]: not back up at server %s, %d", pb.me, args.PutID)
		reply.Err = ErrWrongServer
		return nil
	}

	reply.PreviousValue = args.CorrectPreVal
	pb.data[args.Key] = args.Value
	// log.Printf("[ForwardPut]: putdata completed, %d", args.PutID)
	pb.putReplyLog[args.PutID] = *reply

	return nil
}


func (pb *PBServer) Put(args *PutArgs, reply *PutReply) error {
  // Your code here.

	pb.lock.Lock(); defer pb.lock.Unlock()
	// log.Printf("[Put]: at server %s, id; %d", pb.me, args.PutID)
	// args.Printf()
	// pb.view.Printf()

	if !pb.isPrimary() {
		// log.Printf("[Put]: at server %s, not primary, id: %d", pb.me, args.PutID)
		reply.Err = ErrWrongServer
		return nil
	}

	if prevReply, ok := pb.putReplyLog[args.PutID]; ok {
		*reply = prevReply
		return nil
	}

	backupReply := &PutReply{}
	// do forward call if pb has backup
	if pb.hasBackup() {
		// pb.view.Printf()

		backupArgs := &ForwardPutArgs{Key: args.Key,
																	Value: pb.getPutDataVal(args),
																	CorrectPreVal: pb.getData(args.Key),
																	PutID: args.PutID}
		ok := call(pb.getBackup(), "PBServer.ForwardPut", backupArgs, backupReply)

		if backupReply.Err == ErrWrongServer || !ok {
			if ok {
				// log.Printf("[Put]: forwardPut wrong server, %d", args.PutID)
			} else {
				// log.Printf("[Put]: forwardPut call failed, %d", args.PutID)
			}
			reply.Err = ErrWrongServer
			return nil // TODO: if the backup doesn't respond, the primary thinks it's dead
		}

		if backupReply.PreviousValue != pb.getData(args.Key) {
			// log.Printf("[Put]: forwardPut yields different value, backup reply: %v, primary PreVal: %v,  %d", backupReply, pb.getData(args.Key), args.PutID)
		}
		// log.Printf("[Put]: finishing forward put, %d", args.PutID)
	}

	// if backup succeeds || no back up
	if backupReply.Err == "" {
		reply.PreviousValue = pb.getData(args.Key)
		pb.putData(args)
		pb.putReplyLog[args.PutID] = *reply
		// log.Printf("[Put]: finished putting data, %d", args.PutID)
	}
	return nil
}

func (pb *PBServer) ForwardGet(args *GetArgs, reply *GetReply) error {
	pb.lock.Lock(); defer pb.lock.Unlock()
	// log.Printf("[ForwardGet]: server: %s", viewservice.GetCleanName(pb.me))
	// pb.printfData()

	if pb.isBackup() {
		return nil
	} else {
		reply.Err = ErrWrongServer
		return nil
	}
}

func (pb *PBServer) Get(args *GetArgs, reply *GetReply) error {
  // Your code here.
	pb.lock.Lock(); defer pb.lock.Unlock()
	// log.Printf("[Get]: server: %s", viewservice.GetCleanName(pb.me))

	if !pb.isPrimary() {
		reply.Err = ErrWrongServer
		return nil
	}

	// pb.printfData()
	backupReply := &GetReply{}
	if pb.hasBackup() {
		ok := call(pb.getBackup(), "PBServer.ForwardGet", args, backupReply)

		if backupReply.Err == ErrWrongServer || !ok {
			// backup receives and not responds || backup not receives
			reply.Err = ErrWrongServer
			return nil
		}

		if backupReply.Value != pb.getData(args.Key) {
			// log.Printf("[Get]: server: %s does not yield the same value from backup, %v", viewservice.GetCleanName(pb.me), args.GetID)
		}
	}

	if backupReply.Err == "" {
		// backup receive and responds || no backup
		reply.Value = pb.getData(args.Key)
		return nil
	} else {
		// backup receive and responds with error
		reply.Err = ErrWrongServer
		return nil
	}
}


func (pb *PBServer) Transfer(args *TransferArgs, reply *TransferReply) error {
	pb.lock.Lock(); defer pb.lock.Unlock()
	// log.Printf("[Replicate]: incoming data")

	pb.delete()

	for key, value := range args.Data {
		pb.data[key] = value
	}
	for key, value := range args.GetReplyLog {
		pb.getReplyLog[key] = value
	}
	for key, value := range args.PutReplyLog {
		pb.putReplyLog[key] = value
	}

	return nil
}

func (pb *PBServer) delete() {
	for key, _ := range pb.data {
		delete(pb.data, key)
	}
	for key, _ := range pb.getReplyLog {
		delete(pb.getReplyLog, key)
	}
	for key, _ := range pb.putReplyLog {
		delete(pb.putReplyLog, key)
	}
}

func (pb *PBServer) assignNewView(newView *viewservice.View) {
	pb.view = *newView
}

// ping the viewserver periodically.
// 2 important events
// 1. assigned as primary
// 2. assigned as backup
func (pb *PBServer) tick() {
  // Your code here.
	pb.lock.Lock(); defer pb.lock.Unlock()
	newView, _ := pb.vs.Ping(pb.view.Viewnum)

	// log.Printf("server tick: %s", viewservice.GetCleanName(pb.me))

	// idle to backup: copy all the things from primary
	if newView.Primary != pb.me && newView.Backup != pb.me {
		pb.delete()

	} else {

		if newView.Primary == pb.me {
			if newView.Backup != pb.getBackup() || newView.Viewnum != pb.view.Viewnum {
				// log.Printf("[tick]: Transferring data from server %s to server %s", pb.me, newView.Backup)
				args := &TransferArgs{}
				reply := &TransferReply{}

				args.Data = make(map[string]string)
				args.GetReplyLog = make(map[int64]GetReply)
				args.PutReplyLog = make(map[int64]PutReply)

				for key, val := range pb.data {
					args.Data[key] = val
				}
				for key, val := range pb.getReplyLog {
					args.GetReplyLog[key] = val
				}
				for key, val := range pb.putReplyLog {
					args.PutReplyLog[key] = val
				}
				// pb.printfData()

				ok := call(newView.Backup, "PBServer.Transfer", args, reply)
				if !ok {
					newView.Backup = ""
					// log.Printf("[tick]: Transfer call failed")
				}

				// log.Printf("[tick]: Newview %v", newView)

			}
		}
	}

	pb.assignNewView(&newView)
}


// tell the server to shut itself down.
// please do not change this function.
func (pb *PBServer) kill() {
  pb.dead = true
  pb.l.Close()
}


func StartServer(vshost string, me string) *PBServer {
  pb := new(PBServer)
  pb.me = me
  pb.vs = viewservice.MakeClerk(me, vshost)
  pb.finish = make(chan interface{})
  // Your pb.* initializations here.
	pb.data = make(map[string]string)
	pb.putReplyLog = make(map[int64]PutReply)
	pb.opCount = 0

  rpcs := rpc.NewServer()
  rpcs.Register(pb)

  os.Remove(pb.me)
  l, e := net.Listen("unix", pb.me);
  if e != nil {
    log.Fatal("listen error: ", e);
  }
  pb.l = l

  // please do not change any of the following code,
  // or do anything to subvert it.

  go func() {
    for pb.dead == false {
      conn, err := pb.l.Accept()
      if err == nil && pb.dead == false {
        if pb.unreliable && (rand.Int63() % 1000) < 100 {
          // discard the request.
					// log.Printf("[StartServer] discard the request, server: %s", pb.me)
          conn.Close()
        } else if pb.unreliable && (rand.Int63() % 1000) < 200 {
          // process the request but force discard of reply.
					// log.Printf("[StartServer] process the request but force discard of reply,kserver: %s ", pb.me)
          c1 := conn.(*net.UnixConn)
          f, _ := c1.File()
          err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
          if err != nil {
            fmt.Printf("shutdown: %v\n", err)
          }
          pb.done.Add(1)
          go func() {
            rpcs.ServeConn(conn)
            pb.done.Done()
          }()
        } else {
          pb.done.Add(1)
          go func() {
            rpcs.ServeConn(conn)
            pb.done.Done()
          }()
        }
      } else if err == nil {
        conn.Close()
      }
      if err != nil && pb.dead == false {
        fmt.Printf("PBServer(%v) accept: %v\n", me, err.Error())
        pb.kill()
      }
    }
    DPrintf("%s: wait until all request are done\n", pb.me)
    pb.done.Wait()
    // If you have an additional thread in your solution, you could
    // have it read to the finish channel to hear when to terminate.
    close(pb.finish)
  }()

  pb.done.Add(1)
  go func() {
    for pb.dead == false {
      pb.tick()
      time.Sleep(viewservice.PingInterval)
    }
    pb.done.Done()
  }()

  return pb
}
