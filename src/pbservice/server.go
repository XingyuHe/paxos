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
	data map[string]string
	dataLock sync.RWMutex

	putReplyLog map[int64]PutReply
	putReplyLogLock sync.RWMutex

	getReplyLog map[int64]GetReply
	getReplyLogLock sync.RWMutex

	view viewservice.View
	viewLock sync.RWMutex
}

func (pb *PBServer) GetPrimary() string {
	return pb.view.Primary
}
func (pb *PBServer) GetBackup() string {
	return pb.view.Backup
}
func (pb *PBServer) IsPrimary() bool {
	return pb.GetPrimary() == pb.me
}
func (pb *PBServer) IsBackup() bool {
	return pb.GetBackup() == pb.me
}
func (pb *PBServer) HasBackup() bool {
	return pb.GetBackup() != ""
}
func (pb *PBServer) IsIdle() bool {
	return !pb.IsBackup() && !pb.IsPrimary()
}
func (pb *PBServer) PutData(key string, val string, doHash bool) {

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
func (pb *PBServer) GetData(key string) string {
	return pb.data[key]
}

// bits
func (pb *PBServer) GetCompleteBits() {

}

// printing
func (pb *PBServer) PrintfData() {
	log.Printf("\t[PrintfData] start")
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

func (pb *PBServer) ForwardPut(args *PutArgs, reply *PutReply) error {
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
		return nil
	} else {
		reply.Err = Err(fmt.Sprintf("server %s is not a backup", pb.me))
		return fmt.Errorf("server %s is not a backup", pb.me)
	}
}


func (pb *PBServer) TryCall(srv string, rpcname string,
          args interface{}, reply interface{}, done chan bool) {
  ok := call(srv, rpcname, args, reply)
	if ok {
		done <- true
	} 
}

func (pb *PBServer) Put(args *PutArgs, reply *PutReply) error {
  // Your code here.
	log.Printf("[Put]: at server %s", pb.me)
	args.Printf()

	pb.putReplyLogLock.Lock(); defer pb.putReplyLogLock.Unlock()
	pb.dataLock.Lock(); defer pb.dataLock.Unlock()
	pb.viewLock.RLock(); defer pb.viewLock.RUnlock()

	if !pb.IsPrimary() {
		reply.Err = Err("wrong primary")
		return fmt.Errorf("server %s is not a primary server", pb.me)
	}

	if prevReply, ok := pb.putReplyLog[args.PutID]; ok {
		*reply = prevReply
		return nil
	}

	backupReply := &PutReply{}
	// do forward call if pb has backup
	if pb.HasBackup() {
		pb.viewLock.RLock(); defer pb.viewLock.RUnlock()
		pb.view.Printf()

		ok := call(pb.GetBackup(), "PBServer.ForwardPut", args, backupReply)

		if backupReply.Err != "" || !ok {
			reply.Err = Err("fail to call backup")
			return fmt.Errorf("fail to call backup") // TODO: if the backup doesn't respond, the primary thinks it's dead
		}
	}
	log.Printf("\t[Put]: finishing forward put")

	// if backup succeeds || no back up
	if backupReply.Err == "" {
		reply.PreviousValue = pb.GetData(args.Key)
		pb.PutData(args.Key, args.Value, args.DoHash)
		pb.putReplyLog[args.PutID] = *reply
		log.Printf("\t[Put]: finished putting data")
	}
	return nil
}

func (pb *PBServer) ForwardGet(args *GetArgs, reply *GetReply) error {
	log.Printf("[ForwardGet]: server: %s", viewservice.GetCleanName(pb.me))
	log.Printf("\t[ForwardGet] args: %v\n", args)
	pb.dataLock.RLock(); defer pb.dataLock.RUnlock()
	pb.PrintfData()

	if pb.IsBackup() {
		reply.Value = pb.GetData(args.Key)
		return nil
	} else {
		reply.Err = Err(fmt.Sprintf("Server %s is not a backup", pb.me))
		return fmt.Errorf("server %s is not a backup", pb.me)
	}
}

func (pb *PBServer) Get(args *GetArgs, reply *GetReply) error {
  // Your code here.
	log.Printf("[Get]: server: %s", viewservice.GetCleanName(pb.me))
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
		return nil
	} else {
		reply.Err = Err(fmt.Sprintf("Backup of server %s changed", pb.me))
		return fmt.Errorf("backup of server %s changed", pb.me)
	}
}


func (pb *PBServer) Replicate(args *ReplicateArgs, reply *ReplicateReply) error {
	log.Printf("[Replicate]: source server: %s", viewservice.GetCleanName(pb.me))
	pb.dataLock.RLock(); defer pb.dataLock.RUnlock()

	pb.PrintfData()
	reply.Data = make(map[string]string)
	for key, value := range pb.data {
		reply.Data[key] = value
	}
	return nil
}

func (pb *PBServer) IdleToBackup(newView *viewservice.View) bool {
	return pb.IsIdle() && newView.Backup == pb.me 
}

func (pb *PBServer) BackupToBackup(newView *viewservice.View) bool {
	return pb.IsBackup() && newView.Backup == pb.me && newView.Viewnum > pb.view.Viewnum
}

func (pb *PBServer) DeleteData() {
	for key, _ := range pb.data {
		delete(pb.data, key)
	}
}

// ping the viewserver periodically.
// 2 important events
// 1. assigned as primary
// 2. assigned as backup
func (pb *PBServer) tick() {
  // Your code here.
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
	}
	pb.view = newView
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
					log.Printf("[StartServer]: discard request, unrealiable")
          conn.Close()
        } else if pb.unreliable && (rand.Int63() % 1000) < 200 {
          // process the request but force discard of reply.
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
