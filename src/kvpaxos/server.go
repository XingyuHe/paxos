package kvpaxos

import (
	"encoding/gob"
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/rpc"
	"os"
	"paxos"
	"sync"
	"syscall"
)

const Debug=0

func DPrintf(format string, a ...interface{}) (n int, err error) {
  if Debug > 0 {
    log.Printf(format, a...)
  }
  return
}


type Op struct {
  // Your definitions here.
  // Field names must start with capital letters,
  // otherwise RPC will break.
  Args interface {} // Get or Put args
  Reply interface {} // Get or Put Reply
}

type KVPaxos struct {
  mu sync.Mutex
  l net.Listener
  me int
  dead bool // for testing
  unreliable bool // for testing
  px *paxos.Paxos

  // Your definitions here.
  kvstore map[string]string
  getCache map[int64]GetReply
  putCache map[int64]PutReply
  doneSeq int
}


func (kv *KVPaxos) Get(args *GetArgs, reply *GetReply) error {
  // Your code here.
  DB := makeDebugger("Get", args.ID, kv.me)
  DB.printf(1, "GetArgs", args.toString())

  kv.mu.Lock(); defer kv.mu.Unlock()

  origReply, ok := kv.findGetCache(args.ID)
  if ok {
    kv.fillGetReply(origReply, reply)
    DB.printf(2, "found in GetCache ", reply.toString())
  } else {

    seq := kv.doneSeq + 1
    candidateOp := kv.builPaxosGetOp(seq, args)
    kv.px.Start(seq, candidateOp)
    DB.printf(3, "finished Start")
    decidedOp := kv.waitForDecision(seq)
    DB.printf(4, "decidedOp ", decidedOp.toString())
    kv.updateStateFromSeq(seq)

    if (decidedOp == candidateOp) {
      *reply = decidedOp.Reply.(GetReply)
      DB.printf(5, "agreed\n\tdecidedOp: ", decidedOp.toString(), "\n\tcandidateOp: ", candidateOp.toString())
    } else {
      DB.printf(5, "not agreed\n\tdecidedOp: ", decidedOp.toString(), "\n\tcandidateOp: ", candidateOp.toString())
    }
  }
  log.Printf("doneSeq: %v server: %v", kv.doneSeq, kv.me)
  kv.printSeqToState()
  kv.printStateSize()
  kv.printState()
  return nil
}

func (kv *KVPaxos) Put(args *PutArgs, reply *PutReply) error {
  // Your code here.
  DB := makeDebugger("Put", args.ID, kv.me)
  DB.printf(1, "PutArgs:", args.toString())

  kv.mu.Lock(); defer kv.mu.Unlock()

  origReply, ok := kv.findPutCache(args.ID)
  if ok {
    kv.fillPutReply(origReply, reply)
    DB.printf(2, "found in PutCache", reply.toString())
  } else {

    seq := kv.doneSeq + 1
    candidateOp := kv.builPaxosPutOp(seq, args)
    kv.px.Start(seq, candidateOp)
    DB.printf(3, "finished Start")
    decidedOp := kv.waitForDecision(seq)
    DB.printf(4, "decidedOp ", decidedOp.toString())
    kv.updateStateFromSeq(seq)

    if (decidedOp == candidateOp) {
      *reply = decidedOp.Reply.(PutReply)
      DB.printf(5, "agreed\n\tdecidedOp: ", decidedOp.toString(), "\n\tcandidateOp: ", candidateOp.toString())
    } else {
      DB.printf(5, "not agreed\n\tdecidedOp: ", decidedOp.toString(), "\n\tcandidateOp: ", candidateOp.toString())
      *reply = kv.buildPutReply(ErrNotAgreed, "")
    }
  }
  log.Printf("doneSeq: %v server: %v", kv.doneSeq, kv.me)
  kv.printSeqToState()
  kv.printStateSize()
  kv.printState()
  return nil
}

// tell the server to shut itself down.
// please do not change this function.
func (kv *KVPaxos) kill() {
  DPrintf("Kill(%d): die\n", kv.me)
  kv.dead = true
  kv.l.Close()
  kv.px.Kill()
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
//
func StartServer(servers []string, me int) *KVPaxos {
  // call gob.Register on structures you want
  // Go's RPC library to marshall/unmarshall.
  gob.Register(Op{})

  kv := new(KVPaxos)
  kv.me = me

  // Your initialization code here.
  kv.kvstore = make(map[string]string)
  kv.getCache = make(map[int64]GetReply)
  kv.putCache = make(map[int64]PutReply)
  kv.doneSeq = -1

  gob.Register(GetArgs{})
  gob.Register(GetReply{})
  gob.Register(PutArgs{})
  gob.Register(PutReply{})


  rpcs := rpc.NewServer()
  rpcs.Register(kv)

  kv.px = paxos.Make(servers, me, rpcs)

  os.Remove(servers[me])
  l, e := net.Listen("unix", servers[me]);
  if e != nil {
    log.Fatal("listen error: ", e);
  }
  kv.l = l


  // please do not change any of the following code,
  // or do anything to subvert it.

  go func() {
    for kv.dead == false {
      conn, err := kv.l.Accept()
      if err == nil && kv.dead == false {
        if kv.unreliable && (rand.Int63() % 1000) < 100 {
          // discard the request.
          conn.Close()
        } else if kv.unreliable && (rand.Int63() % 1000) < 200 {
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
      if err != nil && kv.dead == false {
        fmt.Printf("KVPaxos(%v) accept: %v\n", me, err.Error())
        kv.kill()
      }
    }
  }()

  return kv
}

