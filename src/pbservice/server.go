package pbservice

import (
	// "errors"
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/rpc"
	"os"
	"sync"
	"syscall"
	"time"
	"viewservice"
)

//import "strconv"

// Debugging
const Debug = 0

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
	view viewservice.View
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
func (pb *PBServer) PutData(key string, val string) {
	pb.data[key] = val
}
func (pb *PBServer) GetData(key string) string {
	return pb.data[key]
}

// printing
func (pb *PBServer) PrintfData() {
	for k, v := range pb.data {
		log.Printf("\t(%s, %s)", k, v)
	}
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
	log.Printf("[ForwardPut]: (%s, %s)", args.Key, args.Value)
	if pb.IsBackup() {
		reply.PreviousValue = pb.GetData(args.Key)
		pb.PutData(args.Key, args.Value)
		return nil
	} else {
		reply.Err = Err(fmt.Sprintf("server %s is not a backup", pb.me))
		return fmt.Errorf("server %s is not a backup", pb.me)
	}
}

func (pb *PBServer) Put(args *PutArgs, reply *PutReply) error {
  // Your code here.
	backupReply := &PutReply{}
	if pb.HasBackup() {
		ok := call(pb.GetBackup(), "PBServer.ForwardPut", args, backupReply)
		if !ok {
			return fmt.Errorf("server %s not respond", pb.GetBackup())
		}
	}
	if backupReply.Err == "" {
		reply.PreviousValue = pb.GetData(args.Key)
		pb.PutData(args.Key, args.Value)
		return nil
	} else {
		reply.Err = Err(fmt.Sprintf("Backup of server %s changed", pb.me))
		return fmt.Errorf("backup of server %s changed", pb.me)
	}
}

func (pb *PBServer) ForwardGet(args *GetArgs, reply *GetReply) error {
	if pb.IsBackup() {
		return nil
	} else {
		reply.Err = Err(fmt.Sprintf("Server %s is not a backup", pb.me))
		return fmt.Errorf("server %s is not a backup", pb.me)
	}
}

func (pb *PBServer) Get(args *GetArgs, reply *GetReply) error {
  // Your code here.
	backupReply := &GetReply{}
	if pb.HasBackup() {
		ok := call(pb.GetBackup(), "PBServer.ForwardGet", args, backupReply)
		if !ok {
			return fmt.Errorf("server %s not respond", pb.GetBackup())
		} 
	}
	
	if backupReply.Err == "" {
		reply.Value = pb.GetData(args.Key)
		return nil
	} else {
		reply.Err = Err(fmt.Sprintf("Backup of server %s changed", pb.me))
		return fmt.Errorf("backup of server %s changed", pb.me)
	}
}


func (pb *PBServer) Replicate(args *ReplicateArgs, reply *ReplicateReply) error {
	for key, value := range pb.data {
		reply.data[key] = value
	}
	return nil
}

// ping the viewserver periodically.
// 2 important events
// 1. assigned as primary
// 2. assigned as backup
func (pb *PBServer) tick() {
  // Your code here.
	newView, _ := pb.vs.Ping(pb.view.Viewnum)
	log.Printf("server tick: %s", viewservice.GetCleanName(pb.me))
	pb.view.Printf()
	newView.Printf()

	// idle to backup: copy all the things from primary
	if pb.IsIdle() && newView.Backup == pb.me {
		args := &ReplicateArgs{}
		reply := &ReplicateReply{}
		call(newView.Primary, "PBServer.Replicate", args, reply)
		for key, val := range reply.data {
			pb.data[key] = val
		}

	} else if pb.IsBackup() && newView.Primary == pb.me {
		// backup to primary
	} else if pb.IsPrimary() && newView.Primary != pb.me {
		// primary to idle
		for key, _ := range pb.data {
			delete(pb.data, key)
		}
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
	pb.data["abcd"] = "abdc"
	fmt.Printf("data: %s from %s", pb.data["abcd"], me)

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
