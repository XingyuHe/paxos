package shardmaster

import (
	"log"
	"time"
	"math/big"
	"crypto/rand"
)

// changing the state

func (sm *ShardMaster) updateStateFromSeq(seq int) bool {
	DB := makeDebugger("updateStateFromSeq", 0, sm.me)
	DB.printf(1, "doneSeq: ", sm.doneSeq)
	// log.Printf("[updateStateFromSeq] seq: %v", seq)

  // defer kv.printSeqToState()
  // defer kv.printStateSize()
  // defer kv.printState()

	ok, v := sm.px.Status(seq)
	if !ok {
		// Instance not agreed
		DB.printf(2, "Instance not agreed")
		return false
	} else {
		// Paxos instance seq is agreed
		op := v.(Op)
		if (seq != sm.doneSeq + 1) {
			DB.printf(3, "seq skipped")
			log.Fatalf("seq: %v is skipped!", sm.doneSeq + 1)
		}
		// update doneSeq
		sm.doneSeq = seq
		sm.px.Done(seq)

		switch args := op.Agree.(type) {
		case JoinArgs:
			DB.printf(5, "")
			sm.updateJoinArgs(&args)

		case LeaveArgs:
			DB.printf(6, "")
			sm.updateLeaveArgs(&args)

		case MoveArgs:
			DB.printf(7, "")
			sm.updateMoveArgs(&args)

		case QueryArgs:
			DB.printf(8, "")

		default:
			DB.printf(9, "")
		}
		DB.printf(10, "config history: ", sm.allConfigToString())
		return true
	}
}

func (sm *ShardMaster) updateMoveArgs(args *MoveArgs) {
	DB := makeDebugger("updateMoveArgs", 0, sm.me)
	var newConfig Config

	lastConfig := sm.configs[len(sm.configs) - 1]
	var shards [NShards]int64
	for shard, gid := range lastConfig.Shards {
		shards[shard] = gid
	}
	shards[args.Shard] = args.GID

	groups := make(map[int64][]string)
	for gid, servers := range lastConfig.Groups {
		groups[gid] = servers
	}

	newConfig = sm.makeConfig(
		len(sm.configs),
		shards,
		groups)

	sm.configs = append(sm.configs, newConfig)
	DB.printf(3, "newConfig: ", newConfig)
}

func (sm *ShardMaster) updateLeaveArgs(args *LeaveArgs) {

	DB := makeDebugger("updateLeaveArgs", 0, sm.me)
	DB.printf(1, args.toString())

	var newConfig Config

	lastConfig := sm.configs[len(sm.configs) - 1]
	shards := sm.leaveNewShardFromOld(args, &lastConfig)
	DB.printf(2, "shards: ", shards)

	groups := make(map[int64][]string)

	for gid, servers := range lastConfig.Groups {
		groups[gid] = servers
	}
	delete(groups, args.GID)
	DB.printf(2, "groups: ", groups)

	newConfig = sm.makeConfig(
		len(sm.configs),
		shards,
		groups)

	sm.configs = append(sm.configs, newConfig)
	DB.printf(3, "config history: ", sm.allConfigToString())
}

func (sm *ShardMaster) updateJoinArgs(args *JoinArgs) {

	DB := makeDebugger("updateJoinArgs", 0, sm.me)
	DB.printf(1, args.GID)

	// update cache
	// new config
	var newConfig Config
	DB.printf(2, "groups: ", sm.configs[len(sm.configs) - 1].Groups)
	if sm.initialized() {

		lastConfig := sm.configs[len(sm.configs) - 1]
		groups := make(map[int64][]string)

		if _, ok := lastConfig.Groups[args.GID]; ok {
			return
		}

		for gid, servers := range lastConfig.Groups {
			groups[gid] = servers
		}
		groups[args.GID] = args.Servers

		shards := sm.joinNewShardFromOld(args, &lastConfig)

		newConfig = sm.makeConfig(
			len(sm.configs),
			shards,
			groups)

	} else {
		var shards [NShards]int64
		groups := make(map[int64][]string)

		for i, _ := range shards {
			shards[i] = args.GID
		}
		groups[args.GID] = args.Servers

		newConfig = sm.makeConfig(
			len(sm.configs),
			shards,
			groups)
	}

	sm.configs = append(sm.configs, newConfig)
	DB.printf(3, "config history: ", sm.allConfigToString())
}


func (sm *ShardMaster) buildJoinOp(args *JoinArgs) Op {
	v := Op{}
	v.Agree = *args
	v.OpID = genID()
	return v
}

func (sm *ShardMaster) buildLeaveOp(args *LeaveArgs) Op {
	v := Op{}
	v.Agree = *args
	v.OpID = genID()
	return v
}

func (sm *ShardMaster) buildMoveOp(args *MoveArgs) Op {
	v := Op{}
	v.Agree = *args
	v.OpID = genID()
	return v
}

func (sm *ShardMaster) buildQueryOp(args *QueryArgs) Op {
	v := Op{}
	v.Agree = *args
	v.OpID = genID()
	return v
}

func (sm *ShardMaster) buildQueryReply(configIdx int) QueryReply {

	var retReply QueryReply
  if configIdx < 0 || configIdx >= len(sm.configs) {
    retReply.Config = sm.configs[len(sm.configs) - 1]
  } else {
    retReply.Config = sm.configs[configIdx]
  }
	return retReply
}


// this function blocks
func (sm *ShardMaster) waitForDecision(seq int) Op {
	to := 10 * time.Millisecond
	for {
		decided, decidedOp := sm.px.Status(seq)
		if decided {
			return decidedOp.(Op)
		}
		time.Sleep(to)
		if to < 10 * time.Second {
			to *= 2
		}
	}
}



func Min(a int, b int) int {
	if a <= b{
		return a
	}
	return b
}

func Max(a int, b int) int {
	if a >= b{
		return a
	}
	return b
}

func equal(op1 *Op, op2 *Op) bool {
	return op1.OpID == op2.OpID
}

func genID() int64 {
	max := big.NewInt(int64(1) << 62)
  bigx, _ := rand.Int(rand.Reader, max)
  x := bigx.Int64()
  return x
}