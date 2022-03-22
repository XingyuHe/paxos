package shardkv

import "net"
import "fmt"
import "net/rpc"
import "log"
import "time"
import "paxos"
import "sync"
import "os"
import "syscall"
import "encoding/gob"
import "math/rand"
import "shardmaster"

const Debug=0

func DPrintf(format string, a ...interface{}) (n int, err error) {
        if Debug > 0 {
                log.Printf(format, a...)
        }
        return
}


type Op struct {
  // Your definitions here.
  Agree interface {} // get agree or put agree
  OpID int64
}


type ShardKV struct {
  mu sync.Mutex
  l net.Listener
  me int
  dead bool // for testing
  unreliable bool // for testing
  sm *shardmaster.Clerk
  px *paxos.Paxos

  gid int64 // my replica group ID

  // Your definitions here.
  kpv KeyToPastPutIDToValue
  getIDtoPutID map[int64]int64

  doneSeq int
  config shardmaster.Config
  prepareConfig shardmaster.Config
  shardToKeys map[int][]string

}

func (kv *ShardKV) sendOpPaxosLcl(candidateOp Op) bool {
  DB := makeDebugger("sendOpPaxosLcl", 0, kv.me, kv.gid)
  seq := kv.doneSeq + 1
  kv.px.Start(seq, candidateOp)
  decidedOp := kv.waitForDecision(seq)
  kv.updateStateFromSeq(seq)
  DB.printf(4, "seq: ", seq)
  if (equalOp(&decidedOp, &candidateOp)) {
    DB.printf(5, "agreed\n\tdecidedOp: ", decidedOp.toString(), "\n\tcandidateOp: ", candidateOp.toString())
    return true
  } else {
    DB.printf(6, "not agreed\n\tdecidedOp: ", decidedOp.toString(), "\n\tcandidateOp: ", candidateOp.toString())
    return false
  }
}


func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) error {
  // Your code here.

  kv.mu.Lock(); defer kv.mu.Unlock()

  DB := makeDebugger("Get", args.ID, kv.me, kv.gid)
  DB.printf(1, "GetArgs", args.toString())

  origReply, ok := kv.findGetCache(args.Key, args.ID)
  if ok {
    kv.fillGetReply(origReply, reply)
    DB.printf(2, "found in GetCache ", reply.toString())
  } else {

    candidateOp := kv.builPaxosGetOp(args)
    if (kv.sendOpPaxosLcl(candidateOp)) {
      *reply = kv.opToReply(&candidateOp).(GetReply)
    } else {
      return ErrPaxosFailed
    }
  }
  log.Printf("doneSeq: %v server: %v", kv.doneSeq, kv.me)
  return nil
}

func (kv *ShardKV) Put(args *PutArgs, reply *PutReply) error {
  // Your code here.
  kv.mu.Lock(); defer kv.mu.Unlock()

  DB := makeDebugger("Put", args.ID, kv.me, kv.gid)
  DB.printf(1, "PutArgs:", args.toString())

  origReply, ok := kv.findPutCache(args.Key, args.ID)
  if ok {
    kv.fillPutReply(origReply, reply)
    DB.printf(2, "found in PutCache", reply.toString())
  } else {

    candidateOp := kv.builPaxosPutOp(args)
    if (kv.sendOpPaxosLcl(candidateOp)) {
      *reply = kv.opToReply(&candidateOp).(PutReply)
    } else {
      return ErrPaxosFailed
    }
  }
  log.Printf("doneSeq: %v server: %v", kv.doneSeq, kv.me)
  return nil
}

//
// Ask the shardmaster if there's a new configuration;
// if so, re-configure.
//
// there is a difference between catching up to a new config and
// just discovering a new config
func (kv *ShardKV) tick() {
  kv.mu.Lock(); defer kv.mu.Unlock()
  DB := makeDebugger("tick", genID(), kv.me, kv.gid)
  newConfig := kv.sm.Query(kv.config.Num + 1)
  if (!equalConfig(&newConfig, &kv.config)) {
    DB.printf(1, "newConfig ", newConfig.ToString())
    DB.printf(1, "oldConfig ", kv.config.ToString())

    prepareConfigAgree := kv.buildPaxosPrepareConfigOp(&newConfig).Agree.(PrepareConfigAgree)
    kv.updatePrepareConfig(&prepareConfigAgree)
    // kv.tryPaxosOpTillSuccess(candidateOp)
    // DB.printf(2, "candidateOp", candidateOp.toString())
  }
}
// a potential problem is that a server could very very behind that
// it is looking at the configurations long ago. The updated the

// another potential problem is that a server


// tell the server to shut itself down.
func (kv *ShardKV) kill() {
  kv.dead = true
  kv.l.Close()
  kv.px.Kill()
}

//
// Start a shardkv server.
// gid is the ID of the server's replica group.
// shardmasters[] contains the ports of the
//   servers that implement the shardmaster.
// servers[] contains the ports of the servers
//   in this replica group.
// Me is the index of this server in servers[].
//
func StartServer(gid int64, shardmasters []string,
                 servers []string, me int) *ShardKV {
  gob.Register(Op{})
  gob.Register(PrepareConfigAgree{})
  gob.Register(GetAgree{})
  gob.Register(PutAgree{})
  gob.Register(MoveShardAgree{})
  gob.Register(OrderedDict{})
  gob.Register(CommitConfigAgree{})

  kv := new(ShardKV)
  kv.me = me
  kv.gid = gid
  kv.sm = shardmaster.MakeClerk(shardmasters)

  // Your initialization code here.
  // Don't call Join().
  kv.doneSeq = 0
  kv.getIDtoPutID = make(map[int64]int64)
  kv.kpv = makeKPV()
  kv.config = shardmaster.Config{Num: 0}
  kv.prepareConfig = shardmaster.Config{Num: -1}
  kv.shardToKeys = make(map[int][]string)

  DB := makeDebugger("StartServer", 0, me, gid)

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
          DB.printf(1, "discard the request.")
          conn.Close()
        } else if kv.unreliable && (rand.Int63() % 1000) < 200 {
          // process the request but force discard of reply.
          DB.printf(1, "process the request but force discard of reply.")
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
        fmt.Printf("ShardKV(%v) accept: %v\n", me, err.Error())
        kv.kill()
      }
    }
  }()

  go func() {
    for kv.dead == false {
      kv.tick()
      time.Sleep(250 * time.Millisecond)
    }
  }()

  return kv
}
