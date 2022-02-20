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
<<<<<<< HEAD
	putReplyLogLock sync.RWMutex

	getReplyLog map[int64]GetReply
	getReplyLogLock sync.RWMutex

=======
	getReplyLog map[int64]GetReply
>>>>>>> HW2-PARTB-REDO
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
func (pb *PBServer) putData(key string, val string, doHash bool) {

	if !doHash {
		pb.data[key] = val
	} else {
		oldVal := ""
		oldVal, _ = pb.data[key]

		hashRes := strconv.Itoa(int(hash(oldVal + val)))

		log.Printf("[PutData]: %s + %s = %s", oldVal, val, hashRes)

		pb.data[key] = hashRes
	}
}
func (pb *PBServer) getData(key string) string {
	return pb.data[key]
}

// bits
func (pb *PBServer) GetCompleteBits() {

}

// printing
<<<<<<< HEAD
func (pb *PBServer) PrintfData() {
	log.Printf("\t[PrintfData] start")
=======
func (pb *PBServer) printfData() {
>>>>>>> HW2-PARTB-REDO
	for k, v := range pb.data {
		log.Printf("\t(%s, %s)", k, v)
	}
	log.Printf("\t[PrintfData] end")
}

<<<<<<< HEAD

/* 
protocol: 
=======
/*
protocol:
>>>>>>> HW2-PARTB-REDO
1. send the call to back up
2. wait for backup to reply
3. if backup replies with error
	1. return that error to client
	2. return value
*/

func (pb *PBServer) ForwardPut(args *PutArgs, reply *PutReply) error {
<<<<<<< HEAD
	log.Printf("[ForwardPut]: (%s, %s) at server %s", args.Key, args.Value, pb.me)
	pb.putReplyLogLock.Lock(); defer pb.putReplyLogLock.Unlock()
	pb.dataLock.Lock(); defer pb.dataLock.Unlock()
	pb.viewLock.RLock(); defer pb.viewLock.RUnlock()

	if pb.IsBackup() {

		if prevReply, ok := pb.putReplyLog[args.PutID]; ok {
			*reply = prevReply
			return nil
		}

		reply.PreviousValue = pb.GetData(args.Key)
		pb.PutData(args.Key, args.Value, args.DoHash)
		log.Printf("\t[ForwardPut]: putdata completed")
=======
	pb.lock.Lock(); defer pb.lock.Unlock()
	log.Printf("[ForwardPut]: (%s, %s) at server %s, %d", args.Key, args.Value, pb.me, args.PutID)

	if !pb.isBackup() {
		log.Printf("[ForwardPut]: not back up at server %s, %d", pb.me, args.PutID)
		reply.Err = ErrWrongServer
>>>>>>> HW2-PARTB-REDO
		return nil
	}

	if prevReply, ok := pb.putReplyLog[args.PutID]; ok {
		log.Printf("[ForwardPut]: put executed %s, %d", pb.me, args.PutID)
		*reply = prevReply
		return nil
	}

	reply.PreviousValue = pb.getData(args.Key)
	pb.putData(args.Key, args.Value, args.DoHash)
	log.Printf("[ForwardPut]: putdata completed")
	pb.putReplyLog[args.PutID] = *reply
	return nil
}


func (pb *PBServer) Put(args *PutArgs, reply *PutReply) error {
  // Your code here.

	pb.lock.Lock(); defer pb.lock.Unlock()
	log.Printf("[Put]: at server %s, id; %d", pb.me, args.PutID)
	args.Printf()

<<<<<<< HEAD
	pb.putReplyLogLock.Lock(); defer pb.putReplyLogLock.Unlock()
	pb.dataLock.Lock(); defer pb.dataLock.Unlock()
	pb.viewLock.RLock(); defer pb.viewLock.RUnlock()

	if !pb.IsPrimary() {
		reply.Err = Err("wrong primary")
		return fmt.Errorf("server %s is not a primary server", pb.me)
=======
	if !pb.isPrimary() {
		log.Printf("[Put]: at server %s, not primary, id: %d", pb.me, args.PutID)
		reply.Err = ErrWrongServer
		return nil
>>>>>>> HW2-PARTB-REDO
	}

	if prevReply, ok := pb.putReplyLog[args.PutID]; ok {
		*reply = prevReply
		return nil
	}

<<<<<<< HEAD
=======

>>>>>>> HW2-PARTB-REDO
	backupReply := &PutReply{}
	// do forward call if pb has backup
	if pb.hasBackup() {
		pb.view.Printf()

		ok := call(pb.getBackup(), "PBServer.ForwardPut", args, backupReply)

		if backupReply.Err == ErrWrongServer || !ok {
			reply.Err = ErrWrongServer
			return nil // TODO: if the backup doesn't respond, the primary thinks it's dead
		}

		if backupReply.PreviousValue != pb.getData(args.Key) {
			log.Printf("[Put]: forwardPut yields different value, %d", args.PutID)
		}
		log.Printf("[Put]: finishing forward put, %d", args.PutID)
	}
<<<<<<< HEAD
	log.Printf("\t[Put]: finishing forward put")
=======
>>>>>>> HW2-PARTB-REDO

	// if backup succeeds || no back up
	if backupReply.Err == "" {
		reply.PreviousValue = pb.getData(args.Key)
		pb.putData(args.Key, args.Value, args.DoHash)
		pb.putReplyLog[args.PutID] = *reply
<<<<<<< HEAD
		log.Printf("\t[Put]: finished putting data")
=======
		log.Printf("[Put]: finished putting data, %d", args.PutID)
>>>>>>> HW2-PARTB-REDO
	}
	return nil
}

func (pb *PBServer) ForwardGet(args *GetArgs, reply *GetReply) error {
	pb.lock.Lock(); defer pb.lock.Unlock()
	log.Printf("[ForwardGet]: server: %s", viewservice.GetCleanName(pb.me))
<<<<<<< HEAD
	log.Printf("\t[ForwardGet] args: %v\n", args)
	pb.dataLock.RLock(); defer pb.dataLock.RUnlock()
	pb.PrintfData()

	if pb.IsBackup() {
		reply.Value = pb.GetData(args.Key)
=======

	if pb.isBackup() {
>>>>>>> HW2-PARTB-REDO
		return nil
	} else {
		reply.Err = ErrWrongServer
		return nil
	}
}

func (pb *PBServer) Get(args *GetArgs, reply *GetReply) error {
  // Your code here.
	pb.lock.Lock(); defer pb.lock.Unlock()
	log.Printf("[Get]: server: %s", viewservice.GetCleanName(pb.me))
<<<<<<< HEAD
	log.Printf("\t[Get] args: %v\n", args)
	pb.dataLock.RLock(); defer pb.dataLock.RUnlock()
	pb.viewLock.RLock(); defer pb.viewLock.RUnlock()
	pb.getReplyLogLock.Lock(); defer pb.getReplyLogLock.Unlock()
	pb.PrintfData()

	if !pb.IsPrimary() {
		reply.Err = Err("wrong primary")
		return fmt.Errorf("server %s is not a primary server", pb.me)
	}

	if prevReply, ok := pb.getReplyLog[args.GetID]; ok {
		*reply = prevReply 
		return nil
	}

	backupReply := &GetReply{}
	if pb.HasBackup() {
		ok := call(pb.GetBackup(), "PBServer.ForwardGet", args, backupReply)
		if !ok {
			return fmt.Errorf("server %s not respond", pb.GetBackup())
		} 
	}
	
	currData := pb.GetData(args.Key)
	if backupReply.Err == "" && currData == backupReply.Value {
		reply.Value = currData
=======

	if !pb.isPrimary() {
		reply.Err = ErrWrongServer
		return nil
	}

	pb.printfData()
	backupReply := &GetReply{}
	if pb.hasBackup() {
		ok := call(pb.getBackup(), "PBServer.ForwardGet", args, backupReply)

		if backupReply.Err == ErrWrongServer || !ok {
			// backup receives and not responds || backup not receives
			reply.Err = ErrWrongServer
			return nil
		}
	}

	if backupReply.Err == "" {
		// backup receive and responds || no backup
		reply.Value = pb.getData(args.Key)
>>>>>>> HW2-PARTB-REDO
		return nil
	} else {
		// backup receive and responds with error
		reply.Err = ErrWrongServer
		return nil
	}
}


<<<<<<< HEAD
func (pb *PBServer) Replicate(args *ReplicateArgs, reply *ReplicateReply) error {
	log.Printf("[Replicate]: source server: %s", viewservice.GetCleanName(pb.me))
	pb.dataLock.RLock(); defer pb.dataLock.RUnlock()
=======
func (pb *PBServer) Transfer(args *TransferArgs, reply *TransferReply) error {
	pb.lock.Lock(); defer pb.lock.Unlock()
	log.Printf("[Replicate]: incoming data")
>>>>>>> HW2-PARTB-REDO

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

func (pb *PBServer) assignNewView(newView viewservice.View) {
	pb.view = newView
}

// ping the viewserver periodically.
// 2 important events
// 1. assigned as primary
// 2. assigned as backup
func (pb *PBServer) tick() {
  // Your code here.
<<<<<<< HEAD
	pb.dataLock.Lock(); defer pb.dataLock.Unlock()
	pb.viewLock.Lock(); defer pb.viewLock.Unlock()
	newView, _ := pb.vs.Ping(pb.view.Viewnum)


	// idle to backup: copy all the things from primary
	if pb.IdleToBackup(&newView) || pb.BackupToBackup(&newView) {
		log.Printf("[tick] at server %s", viewservice.GetCleanName(pb.me))
		log.Printf("\t[tick]: Transferred data")
		args := &ReplicateArgs{}
		reply := &ReplicateReply{}
		reply.Data = make(map[string]string)
		call(newView.Primary, "PBServer.Replicate", args, reply)
		pb.DeleteData()

		for key, val := range reply.Data {
			pb.data[key] = val
		}
		pb.PrintfData()

	} else if pb.IsBackup() && newView.Primary == pb.me {
		log.Printf("[tick] at server %s", viewservice.GetCleanName(pb.me))
		// backup to primary
	} else if pb.IsPrimary() && newView.Primary != pb.me {
		log.Printf("[tick] at server %s", viewservice.GetCleanName(pb.me))
		// primary to idle
		pb.DeleteData()
=======
	pb.lock.Lock(); defer pb.lock.Unlock()
	newView, _ := pb.vs.Ping(pb.view.Viewnum)
	defer pb.assignNewView(newView)

	log.Printf("server tick: %s", viewservice.GetCleanName(pb.me))

	// idle to backup: copy all the things from primary
	if newView.Primary != pb.me && newView.Backup != pb.me {
		pb.delete()

	} else {

		if pb.isPrimary() && newView.Primary == pb.me {
			if newView.Backup != pb.view.Backup {
				log.Printf("[tick]: Transferring data from server %s", pb.me)
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
				pb.printfData()

				ok := call(newView.Backup, "PBServer.Transfer", args, reply)
				if !ok {
					newView.Backup = ""
					log.Printf("[tick]: Transfer call failed")
					return;
				}
			}
		}
>>>>>>> HW2-PARTB-REDO
	}
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
<<<<<<< HEAD
					log.Printf("[StartServer]: discard request, unrealiable")
=======
					log.Printf("[StartServer] discard the request, server: %s", pb.me)
>>>>>>> HW2-PARTB-REDO
          conn.Close()
        } else if pb.unreliable && (rand.Int63() % 1000) < 200 {
          // process the request but force discard of reply.
					log.Printf("[StartServer] process the request but force discard of reply,kserver: %s ", pb.me)
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
