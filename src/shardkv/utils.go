package shardkv

import (
	"crypto/rand"
	"log"
	"math/big"
	"shardmaster"
	"strconv"
	"time"
)

// changing the state

func (kv *ShardKV) updateStateFromSeq(seq int) bool {
	DB := makeDebugger("updateStateFromSeq", 0, kv.me, kv.gid)
	DB.printf(1, "doneSeq: ", kv.doneSeq)

	ok, v := kv.px.Status(seq)
	if !ok {
		// Instance not agreed
		DB.printf(2, "Instance not agreed")
		return false
	} else {
		// Paxos instance seq is agreed
		op := v.(Op)
		if (seq != kv.doneSeq + 1) {
			DB.printf(3, "seq skipped")
			log.Fatalf("seq: %v is skipped!", kv.doneSeq + 1)
		}

		kv.doneSeq = seq
		kv.px.Done(seq)

		switch agree := op.Agree.(type) {
		case GetAgree:
			DB.ID = agree.GetID
			DB.printf(5, "")
			kv.updateGet(&agree)

		case PutAgree:
			DB.printf(6, "")
			kv.updatePut(&agree)

		case ConfigAgree:
			DB.printf(7, "")
			kv.updateConfig(&agree)

		case MoveShardAgree:
			DB.printf(8, "")
			kv.updateMoveShards(&agree)

		default:
			DB.printf(9, "")
		}
		return true
	}
}

func (kv *ShardKV) updateMoveShards(agree *MoveShardAgree) {
	if agree.Num > kv.config.Num {
		kv.config = shardmaster.Config{Num: kv.config.Num}
	}
	// kv.config = shardmaster.Config{Num: kv.config.Num}
	DB := makeDebugger("updateMoveShards", genID(), kv.me, kv.gid)
	DB.printf(1, "updated config: ", kv.config.ToString())
}


func (kv *ShardKV) updateConfig(agree *ConfigAgree) {
	DB := makeDebugger("updateConfig", agree.ID, kv.me, kv.gid)
	newConfig := kv.sm.Query(agree.Num)
	oldConfig := kv.config
	DB.printf(1, "agreedNum: ", agree.Num, "new config: ", newConfig.ToString())
	DB.printf(1, "agreedNum: ", agree.Num, "old config: ", kv.config.ToString())
	DB.printf(0, "state before reconfiguration: ", kv.kpv)
	DB.printf(0, "config before reconfiguration: ", kv.config.ToString())
	defer DB.printf(6, "state after reconfiguration: ", kv.kpv)
	defer DB.printf(6, "config after reconfiguration: ", kv.config.ToString())

	if oldConfig.Num >= agree.Num {
		return
	}

	// identify new shards that I need to add
	// identify the keys according to the shards
	newShards := kv.findNewShards(&oldConfig, &newConfig)
	oldGIDToNewShards := make(map[int64][]int)

	for _, newShard := range newShards {
		currGID := kv.config.Shards[newShard]
		oldGIDToNewShards[currGID] = append(oldGIDToNewShards[currGID], newShard)
	}

	DB.printf(2, "gidToNewShards: ", oldGIDToNewShards)
	movingAllOk := true
	movedKPV := makeKPV()

	for oldGID, newShard := range oldGIDToNewShards {
		DB.printf(3, "looking for gid: ", oldGID)
		movingGidOk := false


		servers, ok := oldConfig.Groups[oldGID]

		if !ok {
			break
		}
		args := MoveShardsArgs{Shards: newShard, ID: genID(), Num: agree.Num}
		reply := MoveShardsReply{}

		for _, srv := range servers {
			DB.printf(4, "calling ShardKV.MoveShards on server: ", srv, ", gid: ", oldGID)
			ok := call(srv, "ShardKV.MoveShards", &args, &reply)
			if ok && reply.Err == OK {
				DB.printf(5, "rpc succeed, reply: ", reply)
				// kv.kpv.insertKPV(reply.KPV)
				movedKPV.insertKPVDict(reply.KPV)
				movingGidOk = true
				break
			}
			DB.printf(6, "rpc ok: ", ok)
		}

		if movingGidOk == false {
			movingAllOk = false
			break
		}
	}

	if movingAllOk {
		kv.kpv.insertKPV(&movedKPV)
		kv.config = newConfig
	}
}

func (kv *ShardKV) updateGet(agree *GetAgree) {

	DB := makeDebugger("updateGetCache", agree.GetID, kv.me, kv.gid)
	DB.printf(1, "")
	if !kv.isKeyInShard(agree.Key) {return}
	// update cache
	if _, ok := kv.findGetCache(agree.Key, agree.GetID); !ok {
		DB.printf(2, "GetCache not found", agree.GetID, "new GetReply", kv.keyToGetReply(agree.Key, agree.LastPutID))
		kv.updateGetCacheID(agree)
	} else {
		// only update the first 1 instance for the same get ID
		DB.printf(3, "GetCache already found")
	}
}

func (kv *ShardKV) updatePut(agree *PutAgree) {

	DB := makeDebugger("updatePutCache", agree.PutID, kv.me, kv.gid)
	DB.printf(1, "")

	if !kv.isKeyInShard(agree.Key) {return}
	// update cache
	if _, ok := kv.findPutCache(agree.Key, agree.PutID); !ok {
		DB.printf(2, "PutCache not found ", agree.PutID, " new PutReply ", kv.keyToPutReply(agree.Key, agree.PutID))
		kv.updatePutCacheID(agree)
	} else {
		// only update the first 1 instance for the same put ID
		DB.printf(3, "PutCache already found")
	}
}


func (kv *ShardKV) updateGetCacheID(agree *GetAgree) {
	kv.getIDtoPutID[agree.GetID] = agree.LastPutID
}

func (kv *ShardKV) findGetCache(key string, getID int64) (GetReply, bool) {
	lastPutID, ok := kv.getPutIDBeforeGetID(getID)
	if !ok {
		return GetReply{}, ok
	}
	return kv.keyToGetReply(key, lastPutID), true
}

func (kv *ShardKV) updatePutCacheID(agree *PutAgree) {
	kv.kpv.insert(agree.Key, agree.PutID, agree.Val)
	if (!agree.DoHash) {
		prevPutID, _ := kv.kpv.getPrevPutIDValue(agree.Key, agree.PutID)
		kv.kpv.removeVal(agree.Key, prevPutID)
	}
	shard := key2shard(agree.Key)
	kv.shardToKeys[shard] = append(kv.shardToKeys[shard], agree.Key)
}

func (kv *ShardKV) getPutIDBeforeGetID(getID int64) (int64, bool) {
	lastPutID,ok := kv.getIDtoPutID[getID]
	return lastPutID, ok
}

func (kv *ShardKV) findPutCache(key string, putID int64) (PutReply, bool) {
	if !kv.kpv.containsPutID(key, putID) {
		return PutReply{}, false
	}
	_, prevValue := kv.kpv.getPrevPutIDValue(key, putID)
	return kv.buildPutReply(OK, prevValue), true
}



func (kv *ShardKV) fillGetReply(src GetReply, tgt *GetReply) {
	tgt.Err = src.Err
	tgt.Value = src.Value
}


func (kv *ShardKV) fillPutReply(src PutReply, tgt *PutReply) {
	tgt.Err = src.Err
	tgt.PreviousValue = src.PreviousValue
}

func (kv *ShardKV) tryPaxosOpTillSuccess(candidateOp Op) {
	DB := makeDebugger("sendPaxosOpTillSuccess", candidateOp.OpID, kv.me, kv.gid)
	for {
		if kv.sendOpPaxosLcl(candidateOp) {
			DB.printf(1, "paxos agreed")
			return
		}
		DB.printf(2, "paxos not agreed")
	}
}

// build Op
func (kv *ShardKV) buildPaxosCatchupOp() Op {
	v := Op{}
	v.OpID = genID()
	return v
}
func (kv *ShardKV) buildPaxosMoveShardsOp(args *MoveShardsArgs) Op {
	v := Op{}
	v.OpID = genID()

	v.Agree = MoveShardAgree{Shards: args.Shards, Num: args.Num, ID: args.ID}

	return v
}
func (kv *ShardKV) buildPaxosConfigOp(newConfig *shardmaster.Config) Op {
	v := Op{}
	v.OpID = genID()

	agree := ConfigAgree{}
	agree.Num = newConfig.Num
	agree.ID = v.OpID

	v.Agree = agree
	return v
}

func (kv *ShardKV) builPaxosPutOp(args *PutArgs) Op {
	v := Op{}
	v.OpID = genID()

	agree := PutAgree{}
	agree.Key = args.Key
	if args.DoHash {
		_, preVal := kv.kpv.getLastPutIDVal(args.Key)
		h := hash(preVal + args.Value)
		agree.Val = strconv.Itoa(int(h))
	} else {
		agree.Val = args.Value
	}
	agree.PutID = args.ID
	agree.DoHash = args.DoHash

	v.Agree = agree
	return v
}


func (kv *ShardKV) builPaxosGetOp(args *GetArgs) Op {
	v := Op{}
	v.OpID = genID()

	agree := GetAgree{}
	agree.GetID = args.ID
	agree.Key = args.Key
	agree.LastPutID, _ = kv.kpv.getLastPutIDVal(args.Key)

	v.Agree = agree
	return v
}

func (kv *ShardKV) opToReply(op *Op) interface {} {

	switch agree := op.Agree.(type) {
	case GetAgree:
		return kv.keyToGetReply(agree.Key, agree.LastPutID)
	case PutAgree:
		return kv.keyToPutReply(agree.Key, agree.PutID)
	case MoveShardAgree:
		return kv.shardsToMoveShardsReply(agree.Shards)
	}
	log.Fatal("wrong Op type")
	return nil
}

func (kv *ShardKV) shardsToMoveShardsReply(shards [] int) MoveShardsReply {

	DB := makeDebugger("shardsToMoveShardsReply", 0, kv.me, kv.gid)
	DB.printf(1, "shards: ", shards)
	DB.printf(2, "shardsToKeys: ", kv.shardToKeys)

	ret := MoveShardsReply{Err: OK, KPV: make(map[string]OrderedDict)}

	for _, shard := range shards {
		for _, key := range kv.shardToKeys[shard] {
			newkpvDict := *kv.kpv.getPutIDToVal(key)
			DB.printf(2, "shard: ", shard, "key: ", key, " pastPutIDToVal: ", newkpvDict)
			ret.KPV[key] = newkpvDict
		}
	}
	return ret
}

func (kv *ShardKV) keyToGetReply(key string, lastPutID int64) GetReply {
	if !kv.isKeyInShard(key) {return kv.buildGetReply(ErrWrongGroup, "")}

	value := kv.kpv.getValue(key, lastPutID)
	if value == "" {
		return kv.buildGetReply(ErrNoKey, value)
	}
	return kv.buildGetReply(OK, value)
}


func (kv *ShardKV) keyToPutReply(key string, putID int64) PutReply {
	if !kv.isKeyInShard(key) {return kv.buildPutReply(ErrWrongGroup, "")}

	var preValue string
	if kv.kpv.containsPutID(key, putID) {
		_, preValue = kv.kpv.getPrevPutIDValue(key, putID)
	} else {
		lastPutID, _ := kv.kpv.getLastPutIDVal(key)
		preValue = kv.kpv.getValue(key, lastPutID)
	}
		return kv.buildPutReply(OK, preValue)
}


func buildGetArgs(key string, id int64) GetArgs {
	return GetArgs{Key: key, ID: id}
}

func buildPutArgs(key string, id int64, value string, doHash bool) PutArgs {
	return PutArgs{Key: key, ID: id, Value: value, DoHash: doHash}
}

func (kv *ShardKV) buildGetReply(err Err, value string) GetReply {
	return GetReply{Err: err, Value: value}
}

func (kv *ShardKV) buildPutReply(err Err, previousValue string) PutReply {
	return PutReply{Err: err, PreviousValue: previousValue}
}

// this function blocks
func (kv *ShardKV) waitForDecision(seq int) Op {
	to := 10 * time.Millisecond
	for {
		decided, decidedOp := kv.px.Status(seq)
		if decided {
			return decidedOp.(Op)
		}
		time.Sleep(to)
		if to < 10 * time.Second {
			to *= 2
		}
	}
}




// orderedDict
func (od *OrderedDict) get(putID int64) string {
  return od.Mapping[putID]
}

func (od *OrderedDict) contains(putID int64) bool {
  _, ok := od.Mapping[putID]
	return ok
}

func (od *OrderedDict) insert(putID int64, value string) {
  od.Stack = append(od.Stack, putID)
  od.Mapping[putID] = value
}

func (od *OrderedDict) removeVal(putID int64) {
	if od.contains(putID) {
		od.Mapping[putID] = ""
	}
}

func (od *OrderedDict) getPrevPutIDValue(putID int64) (int64, string) {
  for i := 1; i < len(od.Stack); i++ {
    if putID == od.Stack[i] {
      return od.Stack[i - 1], od.get(od.Stack[i - 1])
    }
  }
  return 0, ""
}

func (od *OrderedDict) empty() bool {
	return len(od.Stack) == 0
}

func (od *OrderedDict) size() int {
	return len(od.Stack)
}

func (od *OrderedDict) getLastPutIDVal() (int64, string) {
	if od.empty() {
		return 0, ""
	} else {
		return od.Stack[len(od.Stack) - 1], od.get(od.Stack[len(od.Stack) - 1])
	}
}

func NewOrderedDict() *OrderedDict {
  ret := &OrderedDict{}
  ret.Stack = make([]int64, 0)
  ret.Mapping = make(map[int64]string)
  return ret
}

// KeyToPastPutIDToValue
func (kpv *KeyToPastPutIDToValue) insert(key string, putID int64, value string) {
	if !kpv.contains(key) {
		kpv.mapping[key] = NewOrderedDict()
	}
	kpv.getPutIDToVal(key).insert(putID, value)
}

func (kpv *KeyToPastPutIDToValue) insertKPVDict(newkpv map[string]OrderedDict) {
	for key, pastPutIDToValue := range newkpv {
		kpv.insertPutIDToVal(key, pastPutIDToValue)
	}
}

func (kpv *KeyToPastPutIDToValue) insertKPV(kpv2 *KeyToPastPutIDToValue) {
	for key, pastPutIDToValue := range kpv2.mapping {
		kpv.mapping[key] = pastPutIDToValue
	}
}

func (kpv *KeyToPastPutIDToValue) insertPutIDToVal(key string, putIDToVal OrderedDict) {
	kpv.mapping[key] = &putIDToVal
}

func (kpv *KeyToPastPutIDToValue) removeVal(key string, putID int64) {
	if !kpv.contains(key) {
		return
	}
	kpv.getPutIDToVal(key).removeVal(putID)
}


func (kpv *KeyToPastPutIDToValue) contains(key string) bool {
	_, ok := kpv.mapping[key]
	return ok
}

func (kpv *KeyToPastPutIDToValue) containsPutID(key string, putID int64) bool {
	if kpv.contains(key) {
		return kpv.getPutIDToVal(key).contains(putID)
	} else {
		return false
	}
}


func (kpv *KeyToPastPutIDToValue) getPutIDToVal(key string) *OrderedDict {
	return kpv.mapping[key]
}

func (kpv *KeyToPastPutIDToValue) getValue(key string, putID int64) string {
	if !kpv.contains(key) {
		return ""
	} else {
		return kpv.getPutIDToVal(key).get(putID)
	}
}

func (kpv *KeyToPastPutIDToValue) getLastPutIDVal(key string) (int64, string) {
	if !kpv.contains(key) {
		return 0, ""
	} else {
		lastPutID, lastVal := kpv.getPutIDToVal(key).getLastPutIDVal()
		return lastPutID, lastVal
	}
}

func (kpv *KeyToPastPutIDToValue) getPrevPutIDValue(key string, putID int64) (int64, string) {
	if !kpv.contains(key) {
		return 0, ""
	} else {
		putID, val := kpv.getPutIDToVal(key).getPrevPutIDValue(putID)
		return putID, val
	}
}

func makeKPV() KeyToPastPutIDToValue {
	ret := KeyToPastPutIDToValue{}
	ret.mapping = make(map[string]*OrderedDict)
	return ret
}

// miscellaneous

func equalOp(op1 *Op, op2 *Op) bool {
	return op1.OpID == op2.OpID
}

func equalConfig(config1 *shardmaster.Config, config2 *shardmaster.Config) bool {
	return config1.Num == config2.Num
}

func genID() int64 {
	max := big.NewInt(int64(1) << 62)
  bigx, _ := rand.Int(rand.Reader, max)
  x := bigx.Int64()
  return x
}