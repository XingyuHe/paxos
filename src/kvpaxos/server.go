package kvpaxos

import (
	"encoding/gob"
	"fmt"
	"log"
	"math/rand"
  "time"
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
  Agree interface {} // get agree or put agree
}

/*

The goal is to get rid of kvstore

getCache maps GetID to the corresponding put ID
getPutCache maps PutID to reply
maps GetID to the PutID

keyToCurrPutID:
  key: PutID

keyToPastPutIDToValue:

  alternatively, it can be a map of map
  key: [(PutID1 : value1), (PutID2 : value2), ...]

  in the case of TestDone:
  key1: [(PutID1 : value1), (PutID2 : value2)]
  key2: [(PutID1 : value1), (PutID2 : value2)]
  ....
  key9: [(PutID1 : value1), (PutID2 : value2)]
  key10: [(PutID1 : value1), (PutID2 : value2)]

  in total you have to store 10 x 2 x 1 MB = 20 MB
  since you have 3 servers, then you have to store 60 MB

  30 MB


getIDtoPutID:
  map getID to the putID right before the getID
  putID can then go to state log to get the right value

Get()
  if GetID in getIDtoPutID:
    goto keyToPastPutIDToValue
  else:
    goto keyToPutID
    if no key:
      generate a fake PutID
      update getIDToPutID
    goto keyToPastPutIDToValue
    update getIDtoPutID
    return value

Put()
if PutID in keyToPastPutIDValue:
  return the previous PutIDValue
else
  goto keyToCurrPutID return its value as previous value
  update keyToCurrPutID with a new PutID
  update keyToPastPutIDToValue

*/

type KVPaxos struct {
  mu sync.Mutex
  l net.Listener
  me int
  dead bool // for testing
  unreliable bool // for testing
  px *paxos.Paxos

  // Your definitions here.
  kpv KeyToPastPutIDToValue
  getIDtoPutID map[int64]int64 // get reqeust cache
  // maps the getID to the putID for the given key at the time of the request
  // after paxos, what's current value for key? 
  doneSeq int
}


func (kv *KVPaxos) Get(args *GetArgs, reply *GetReply) error {
  // Your code here.
  DB := makeDebugger("Get", args.ID, kv.me)
  DB.printf(1, "GetArgs", args.toString())

  kv.mu.Lock(); defer kv.mu.Unlock()

  origReply, ok := kv.findGetCache(args.Key, args.ID)
  if ok {
    kv.fillGetReply(origReply, reply)
    DB.printf(2, "found in GetCache ", reply.toString())
  } else {

    seq := kv.doneSeq + 1
    candidateOp := kv.builPaxosGetOp(seq, args)
    kv.px.Start(seq, candidateOp)
    DB.printf(3, "finished Start")
    decidedOp := kv.waitForDecision(seq)
    DB.printf(4, "candidateOp: ", candidateOp.toString())
    DB.printf(4, "decidedOp: ", decidedOp.toString())
    kv.updateStateFromSeq(seq)

    if (decidedOp == candidateOp) {
      agree := decidedOp.Agree.(GetAgree)
      *reply = kv.keyToGetReply(agree.Key, agree.LastPutID)
      DB.printf(5, "agreed\n\tdecidedOp: ", decidedOp.toString(), "\n\tcandidateOp: ", candidateOp.toString())
    } else {
      DB.printf(5, "not agreed\n\tdecidedOp: ", decidedOp.toString(), "\n\tcandidateOp: ", candidateOp.toString())
    }
  }
  log.Printf("doneSeq: %v server: %v", kv.doneSeq, kv.me)
  // kv.printSeqToState()
  // kv.printStateSize()
  // kv.printState()
  return nil
}

func (kv *KVPaxos) Put(args *PutArgs, reply *PutReply) error {
  // Your code here.
  DB := makeDebugger("Put", args.ID, kv.me)
  DB.printf(1, "PutArgs:", args.toString())

  kv.mu.Lock(); defer kv.mu.Unlock()

  origReply, ok := kv.findPutCache(args.Key, args.ID)
  if ok {
    kv.fillPutReply(origReply, reply)
    DB.printf(2, "found in PutCache", reply.toString())
  } else {

    seq := kv.doneSeq + 1
    candidateOp := kv.builPaxosPutOp(seq, args)
    DB.printf(3, "candidateOp ", candidateOp.toString())
    kv.px.Start(seq, candidateOp)
    DB.printf(3, "finished Start")
    decidedOp := kv.waitForDecision(seq)
    DB.printf(4, "decidedOp ", decidedOp.toString())
    kv.updateStateFromSeq(seq)

    if (decidedOp == candidateOp) {
      agree := decidedOp.Agree.(PutAgree)
      *reply = kv.keyToPutReply(agree.Key, agree.PutID)
      DB.printf(5, "agreed\n\tdecidedOp: ", decidedOp.toString(), "\n\tcandidateOp: ", candidateOp.toString())
    } else {
      DB.printf(5, "not agreed\n\tdecidedOp: ", decidedOp.toString(), "\n\tcandidateOp: ", candidateOp.toString())
      *reply = kv.buildPutReply(ErrNotAgreed, "")
    }
  }
  log.Printf("doneSeq: %v server: %v", kv.doneSeq, kv.me)
  // kv.printSeqToState()
  // kv.printStateSize()
  // kv.printState()
  return nil
}

func (kv *KVPaxos) checkUpdateStateFromSeq() {
  DB := makeDebugger("checkUpdateStateFromSeq", 0, kv.me)
  for {
    time.Sleep(10 * time.Millisecond)
    kv.mu.Lock()
    DB.printf(1, "")
    kv.px.Done(kv.doneSeq)
    kv.updateStateFromSeq(kv.doneSeq + 1)
    kv.mu.Unlock()
  }
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
  kv.getIDtoPutID = make(map[int64]int64)
  kv.kpv = MakeKPV()
  kv.doneSeq = -1

  gob.Register(GetArgs{})
  gob.Register(GetReply{})
  gob.Register(PutArgs{})
  gob.Register(PutReply{})
  gob.Register(GetAgree{})
  gob.Register(PutAgree{})

  go kv.checkUpdateStateFromSeq()


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
          log.Printf("[StartServer]: server %v, discard request", kv.me)
          conn.Close()
        } else if kv.unreliable && (rand.Int63() % 1000) < 200 {
          // process the request but force discard of reply.
          log.Printf("[StartServer]: server %v, process the request but force discard of reply", kv.me)
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

