package kvpaxos

import (
	"log"
	"strconv"
	"time"
)

// changing the state

func (kv *KVPaxos) updateStateFromSeq(seq int) bool {
	DB := makeDebugger("updateStateFromSeq", 0, kv.me)
	DB.printf(1, "doneSeq: ", kv.doneSeq)
	log.Printf("[updateStateFromSeq] seq: %v", seq)

  defer kv.printSeqToState()
  defer kv.printStateSize()
  defer kv.printState()

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

		DB.printf(4, "seq skipped")
		// update doneSeq
		kv.doneSeq = seq
		kv.px.Done(seq)

		switch agree := op.Agree.(type) {
		case GetAgree:
			DB.ID = agree.GetID
			DB.printf(5, "")
			kv.updateGetCache(&agree)

		case PutAgree:
			DB.printf(6, "")
			kv.updatePutCache(&agree)
		default:
			DB.printf(7, "")
		}
		return true
	}
}

func (kv *KVPaxos) updateGetCache(agree *GetAgree) {

	DB := makeDebugger("updateGetCache", agree.GetID, kv.me)
	DB.printf(1, "")
	// update cache
	if _, ok := kv.findGetCache(agree.Key, agree.GetID); !ok {
		DB.printf(2, "GetCache not found", agree.GetID, "new GetReply", kv.keyToGetReply(agree.Key, agree.LastPutID))
		kv.updateGetCacheID(agree)
	} else {
		// only update the first 1 instance for the same get ID
		DB.printf(3, "GetCache already found")
	}
}

func (kv *KVPaxos) updatePutCache(agree *PutAgree) {

	DB := makeDebugger("updatePutCache", agree.PutID, kv.me)
	DB.printf(1, "")

	// update cache
	if _, ok := kv.findPutCache(agree.Key, agree.PutID); !ok {
		DB.printf(2, "PutCache not found ", agree.PutID, " new PutReply ", kv.keyToPutReply(agree.Key, agree.PutID))
		kv.updatePutCacheID(agree)
	} else {
		// only update the first 1 instance for the same put ID
		DB.printf(3, "PutCache already found")
	}
}


func (kv *KVPaxos) updateGetCacheID(agree *GetAgree) {
	kv.getIDtoPutID[agree.GetID] = agree.LastPutID
}

func (kv *KVPaxos) findGetCache(key string, getID int64) (GetReply, bool) {
	lastPutID, ok := kv.getPutIDBeforeGetID(getID)
	if !ok {
		return GetReply{}, ok
	}
	return kv.keyToGetReply(key, lastPutID), true
}

func (kv *KVPaxos) updatePutCacheID(agree *PutAgree) {
	kv.kpv.insert(agree.Key, agree.PutID, agree.Val)
	kv.keyToCurrPutID[agree.Key] = agree.PutID
}

func (kv *KVPaxos) getPutIDBeforeGetID(getID int64) (int64, bool) {
	lastPutID,ok := kv.getIDtoPutID[getID]
	return lastPutID, ok
}

func (kv *KVPaxos) findPutCache(key string, putID int64) (PutReply, bool) {
	value := kv.kpv.getValue(key, putID)
	if value == "" {
		return PutReply{}, false
	}
	prevValue := kv.kpv.getPreValue(key, putID)
	return kv.buildPutReply(OK, prevValue), true
}



func (kv *KVPaxos) fillGetReply(src GetReply, tgt *GetReply) {
	tgt.Err = src.Err
	tgt.Value = src.Value
}


func (kv *KVPaxos) fillPutReply(src PutReply, tgt *PutReply) {
	tgt.Err = src.Err
	tgt.PreviousValue = src.PreviousValue
}

//
func (kv *KVPaxos) builPaxosPutOp(seq int, args *PutArgs) Op {
	v := Op{}

	agree := PutAgree{}
	agree.Key = args.Key
	if args.DoHash {
		_, preVal := kv.kpv.getLastPutIDVal(args.Key)
		h := hash(preVal + args.Value)
		agree.Val = strconv.Itoa(int(h))
	} else {
		agree.Val =args.Value
	}
	agree.PutID = args.ID

	v.Agree = agree
	return v
}
func (kv *KVPaxos) builPaxosGetOp(seq int, args *GetArgs) Op {
	v := Op{}

	agree := GetAgree{}
	agree.GetID = args.ID
	agree.Key = args.Key
	agree.LastPutID, _ = kv.kpv.getLastPutIDVal(args.Key)

	v.Agree = agree
	return v
}

func (kv *KVPaxos) keyToGetReply(key string, lastPutID int64) GetReply {
	value := kv.kpv.getValue(key, lastPutID)
	if value == "" {
		return kv.buildGetReply(ErrNoKey, value)
	}
	return kv.buildGetReply(OK, value)
}

func (kv *KVPaxos) keyToPutReply(key string, putID int64) PutReply {
	var preValue string
	if kv.kpv.containsPutID(key, putID) {
		preValue = kv.kpv.getPreValue(key, putID)
	} else {
		preValue = kv.kpv.getValue(key, kv.keyToCurrPutID[key])
	}
		return kv.buildPutReply(OK, preValue)
}


func buildGetArgs(key string, id int64) GetArgs {
	return GetArgs{Key: key, ID: id}
}

func buildPutArgs(key string, id int64, value string, doHash bool) PutArgs {
	return PutArgs{Key: key, ID: id, Value: value, DoHash: doHash}
}

func (kv *KVPaxos) buildGetReply(err Err, value string) GetReply {
	return GetReply{Err: err, Value: value}
}

func (kv *KVPaxos) buildPutReply(err Err, previousValue string) PutReply {
	return PutReply{Err: err, PreviousValue: previousValue}
}

// this function blocks
func (kv *KVPaxos) waitForDecision(seq int) Op {
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
  return od.mapping[putID]
}

func (od *OrderedDict) contains(putID int64) bool {
  _, ok := od.mapping[putID]
	return ok
}

func (od *OrderedDict) insert(putID int64, value string) {
  od.stack = append(od.stack, putID)
  od.mapping[putID] = value
}

func (od *OrderedDict) getPreValue(putID int64) string {
  for i := 1; i < len(od.stack); i++ {
    if putID == od.stack[i] {
      return od.get(od.stack[i - 1])
    }
  }
  return ""
}

func (od *OrderedDict) empty() bool {
	return len(od.stack) == 0
}

func (od *OrderedDict) size() int {
	return len(od.stack)
}

func (od *OrderedDict) getLastPutIDVal() (int64, string) {
	if od.empty() {
		return 0, ""
	} else {
		return od.stack[len(od.stack) - 1], od.get(od.stack[len(od.stack) - 1])
	}
}

func NewOrderedDict() *OrderedDict {
  ret := &OrderedDict{}
  ret.stack = make([]int64, 0)
  ret.mapping = make(map[int64]string)
  return ret
}

// KeyToPastPutIDToValue
func (kpv *KeyToPastPutIDToValue) insert(key string, putID int64, value string) {
	if !kpv.contains(key) {
		kpv.mapping[key] = NewOrderedDict()
	}
	kpv.getPutIDToVal(key).insert(putID, value)
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

func (kpv *KeyToPastPutIDToValue) getPreValue(key string, putID int64) string {
	if !kpv.contains(key) {
		return ""
	} else {
		return kpv.getPutIDToVal(key).getPreValue(putID)
	}
}

func MakeKPV() KeyToPastPutIDToValue {
	ret := KeyToPastPutIDToValue{}
	ret.mapping = make(map[string]*OrderedDict)
	return ret
}