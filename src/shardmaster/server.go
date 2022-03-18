package shardmaster

import "net"
import "fmt"
import "net/rpc"
import "log"
import "paxos"
import "sync"
import "os"
import "syscall"
import "encoding/gob"
import "math/rand"

type ShardMaster struct {
  mu sync.Mutex
  l net.Listener
  me int
  dead bool // for testing
  unreliable bool // for testing
  px *paxos.Paxos

  configs []Config // indexed by config num
  doneSeq int
}


type Op struct {
  // Your data here.
  OpID int64
  Agree interface {}
}

func (sm *ShardMaster) sendOpPaxos(candidateOp Op) bool {
  DB := makeDebugger("sendOpPaxos", 0, sm.me)
  seq := sm.doneSeq + 1
  sm.px.Start(seq, candidateOp)
  decidedOp := sm.waitForDecision(seq)
  sm.updateStateFromSeq(seq)
  if (equal(&decidedOp, &candidateOp)) {
    DB.printf(5, "agreed\n\tdecidedOp: ", decidedOp.toString(), "\n\tcandidateOp: ", candidateOp.toString())
    return true
  } else {
    DB.printf(6, "not agreed\n\tdecidedOp: ", decidedOp.toString(), "\n\tcandidateOp: ", candidateOp.toString())
    return false
  }
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) error {
  // Your code here.
  DB := makeDebugger("Join", 0, sm.me)
  DB.printf(1, "JoinArgs", args.toString())

  sm.mu.Lock(); defer sm.mu.Unlock()

  if sm.initialized() {
    lastConfig := sm.configs[len(sm.configs) - 1]
    if _, ok := lastConfig.Groups[args.GID]; ok {
      return nil
    }
  }

  candidateOp := sm.buildJoinOp(args)
  if sm.sendOpPaxos(candidateOp) {
    return nil
  }
  return ErrPaxosFailed
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) error {
  // Your code here.
  DB := makeDebugger("Leave", 0, sm.me)
  DB.printf(1, "LeaveArgs", args.toString())

  sm.mu.Lock(); defer sm.mu.Unlock()

  candidateOp := sm.buildLeaveOp(args)
  if sm.sendOpPaxos(candidateOp) {
    return nil
  }
  return ErrPaxosFailed
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) error {
  // Your code here.
  DB := makeDebugger("Move", 0, sm.me)
  DB.printf(1, "MoveArgs", args.toString())

  sm.mu.Lock(); defer sm.mu.Unlock()

  candidateOp := sm.buildMoveOp(args)
  if sm.sendOpPaxos(candidateOp) {
    return nil
  }
  return ErrPaxosFailed
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) error {
  // Your code here.

  DB := makeDebugger("Query", 0, sm.me)
  DB.printf(1, "QueryArgs", args.toString())

  sm.mu.Lock(); defer sm.mu.Unlock()

  candidateOp := sm.buildQueryOp(args)
  if sm.sendOpPaxos(candidateOp) {
    *reply = sm.buildQueryReply(args.Num)
    return nil
  }
  return ErrPaxosFailed
}

// please don't change this function.
func (sm *ShardMaster) Kill() {
  sm.dead = true
  sm.l.Close()
  sm.px.Kill()
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
func StartServer(servers []string, me int) *ShardMaster {
  gob.Register(Op{})
  gob.Register(QueryArgs{})
  gob.Register(JoinArgs{})
  gob.Register(MoveArgs{})
  gob.Register(LeaveArgs{})

  sm := new(ShardMaster)
  sm.me = me

  sm.configs = make([]Config, 1)
  sm.configs[0].Groups = map[int64][]string{}

  rpcs := rpc.NewServer()
  rpcs.Register(sm)

  sm.px = paxos.Make(servers, me, rpcs)

  os.Remove(servers[me])
  l, e := net.Listen("unix", servers[me]);
  if e != nil {
    log.Fatal("listen error: ", e);
  }
  sm.l = l

  // please do not change any of the following code,
  // or do anything to subvert it.

  go func() {

    for !sm.dead {
      conn, err := sm.l.Accept()
      if err == nil && !sm.dead {
        if sm.unreliable && (rand.Int63() % 1000) < 100 {
          // discard the request.
          conn.Close()
        } else if sm.unreliable && (rand.Int63() % 1000) < 200 {
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
      if err != nil && !sm.dead {
        fmt.Printf("ShardMaster(%v) accept: %v\n", me, err.Error())
        sm.Kill()
      }
    }
  }()

  return sm
}
