package kvpaxos

import (
	"bytes"
	"fmt"
	"log"
	"strconv"
	"time"
	"unsafe"
)

// changing the state

func (kv *KVPaxos) updateStateFromSeq(seq int) bool {
	DB := makeDebugger("updateStateFromSeq", 0, kv.me)
	DB.printf(1, "")
	ok, v := kv.px.Status(seq)
	if !ok {
		// Instance not agreed
		DB.printf(1, "Instance not agreed")
		return false
	} else {
		// Paxos instance seq is agreed
		op := v.(Op)
		if (seq != kv.doneSeq + 1) {
			DB.printf(1, "seq skipped")
			log.Fatalf("seq: %v is skipped!", kv.doneSeq + 1)
		}

		// update doneSeq
		kv.doneSeq = seq
		kv.px.Done(seq)

		switch args := op.Args.(type) {
		case GetArgs:
			DB.ID = args.ID
			DB.printf(2, "")
			reply := op.Reply.(GetReply)
			kv.updateGetCache(seq, &args, &reply)

		case PutArgs:
			reply := op.Reply.(PutReply)
			DB.printf(3, "")
			kv.updatePutCache(seq, &args, &reply)
			DB.printf(4, "")
			kv.updateKVStore(&args)
		}
		return true
	}
}

func (kv *KVPaxos) updateGetCache(seq int, args *GetArgs, reply *GetReply) {

	DB := makeDebugger("updateGetCache", args.ID, kv.me)
	DB.printf(1, "")
	// update cache
	if _, ok := kv.findGetCache(args.ID); !ok {
		DB.printf(2, "GetCache not found", args.ID, "new GetReply", reply.toString())
		kv.updateGetCacheID(args.ID, *reply)
	} else {
		// only update the first 1 instance for the same get ID
		DB.printf(3, "GetCache already found")
	}
}

func (kv *KVPaxos) updatePutCache(seq int, args *PutArgs, reply *PutReply) {

	DB := makeDebugger("updatePutCache", args.ID, kv.me)
	DB.printf(1, "")

	// update cache
	if _, ok := kv.findPutCache(args.ID); !ok {
		DB.printf(2, "PutCache not found ", args.ID, " new PutReply ", reply.toString())
		kv.updatePutCacheID(args.ID, *reply)
	} else {
		// only update the first 1 instance for the same put ID
		DB.printf(3, "PutCache already found")
	}
}


func (kv *KVPaxos) updateKVStore(args *PutArgs) {

	DB := makeDebugger("updateKVStore", 0, kv.me)
	DB.printf(1, "")
	value := args.Value

	if args.DoHash {
		oldVal := ""
		oldVal, _ = kv.kvstore[args.Key]

		DB.printf(2, "DoHash")
		value = strconv.Itoa(int(hash(oldVal + value)))
	}

	DB.printf(3, "previousValue: ", kv.kvstore[args.Key], " newValue ", value)
	kv.kvstore[args.Key] = value
}


func (kv *KVPaxos) updateGetCacheID(id int64, reply GetReply) {
	kv.getCache[id] = reply
}

func (kv *KVPaxos) findGetCache(id int64) (GetReply, bool) {
	reply, ok := kv.getCache[id]
	return reply, ok
}

func (kv *KVPaxos) updatePutCacheID(id int64, reply PutReply) {
	kv.putCache[id] = reply
}

func (kv *KVPaxos) findPutCache(id int64) (PutReply, bool) {
	reply, ok := kv.putCache[id]
	return reply, ok
}


func (kv *KVPaxos) findKVstore(key string) (string, bool) {
	value, ok := kv.kvstore[key]
	return value, ok
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
	v.Args = *args

	// construct reply
	reply := PutReply{}
	reply.Err = OK
	reply.PreviousValue = kv.kvstore[args.Key]
	v.Reply = reply

	return v
}
func (kv *KVPaxos) builPaxosGetOp(seq int, args *GetArgs) Op {
	v := Op{}
	v.Args = args

	// construct reply
	reply := GetReply{}
	value, ok := kv.findKVstore(args.Key)
	if ok {
		reply.Value = value
		reply.Err = OK
	} else {
		reply.Err = ErrNoKey
	}
	v.Reply = reply

	return v
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

func (op *Op) toString() string {
	var ans bytes.Buffer

	switch args := op.Args.(type) {
	case GetArgs:
		reply := op.Reply.(GetReply)

		ans.WriteString("GetArgs: ")
		ans.WriteString(args.toString())
		ans.WriteString(" ")
		ans.WriteString("GetReply: ")
		ans.WriteString(reply.toString())

	case PutArgs:
		reply := op.Reply.(PutReply)

		ans.WriteString("PutArgs: ")
		ans.WriteString(args.toString())
		ans.WriteString(" ")
		ans.WriteString("PutReply: ")
		ans.WriteString(reply.toString())

	}
	return ans.String()
}

func (args *GetArgs) toString() string {
	var ans bytes.Buffer
	ans.WriteString("ID: ")
	ans.WriteString(fmt.Sprint(args.ID))
	ans.WriteString(", ")
	ans.WriteString("Key: ")
	ans.WriteString(fmt.Sprint(args.Key))
	return ans.String()
}

func (reply *GetReply) toString() string {
	var ans bytes.Buffer
	ans.WriteString("Value: ")
	ans.WriteString(fmt.Sprint(reply.Value))
	ans.WriteString(", ")
	ans.WriteString("Err: ")
	ans.WriteString(fmt.Sprint(reply.Err))
	return ans.String()
}


func (args *PutArgs) toString() string {
	var ans bytes.Buffer
	ans.WriteString("ID: ")
	ans.WriteString(fmt.Sprint(args.ID))
	ans.WriteString(", ")
	ans.WriteString("Key: ")
	ans.WriteString(fmt.Sprint(args.Key))
	ans.WriteString(" ")
	ans.WriteString("Value: ")
	ans.WriteString(fmt.Sprint(args.Value))
	ans.WriteString(" ")
	ans.WriteString("DoHash: ")
	ans.WriteString(fmt.Sprint(args.DoHash))
	return ans.String()
}

func (reply *PutReply) toString() string {
	var ans bytes.Buffer
	ans.WriteString("PreviousValue: ")
	ans.WriteString(fmt.Sprint(reply.PreviousValue))
	ans.WriteString(", ")
	ans.WriteString("Err: ")
	ans.WriteString(fmt.Sprint(reply.Err))
	return ans.String()
}

func (kv *KVPaxos) printSeqToState() {
	// DB := makeDebugger("SeqToState", 0, kv.me)

	seqToState := kv.px.GetSeqToState()
	for key, _ := range seqToState {
		log.Printf("\tkey: %v", key)
	}
	log.Printf("\tsize of seqToState %v", len(seqToState))
}


func (kv *KVPaxos) printStateSize() {
	log.Printf("size at server %v", kv.me)
	log.Printf("\tkvStore: %v", unsafe.Sizeof(kv.kvstore))
	log.Printf("\tputCache: %v", unsafe.Sizeof(kv.putCache))
	log.Printf("\tgetCache: %v", unsafe.Sizeof(kv.getCache))
}

func (kv *KVPaxos) printState() {
	var size uintptr
	size += kv.printGetCache()
	size += kv.printPutCache()
	size += kv.printKVStore()
	log.Printf("[printState] totalSize: %v", size)
}

func (kv *KVPaxos) printGetCache() uintptr {
	log.Printf("[GetCache]")
	var size uintptr
	for id, reply := range kv.getCache {
		log.Printf("\t id: %v, reply %v", id, "")
		size += unsafe.Sizeof(reply)
	}
	return size
}

func (kv *KVPaxos) printPutCache() uintptr {
	log.Printf("[PutCache]")
	var size uintptr
	for id, reply := range kv.getCache {
		log.Printf("\t id: %v, reply %v", id, "")
		size += unsafe.Sizeof(reply)
	}
	return size
}

func (kv *KVPaxos) printKVStore() uintptr {
	log.Printf("[KVStore]")
	var size uintptr
	for key, reply := range kv.kvstore {
		log.Printf("\t key: %v, val %v", key, "")
		size += unsafe.Sizeof(reply)
	}
	return size
}