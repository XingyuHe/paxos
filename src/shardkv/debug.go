package shardkv

import (
	"bytes"
	"fmt"
	// "log"
	// "unsafe"
)


type Debugger struct {
	fn string
	ID int64
	server int
	gid int64
}

func (DB *Debugger) printf(step int, args ...interface{}) {
	// log.Printf("[%v.%v] ID: %v Server: %v gid: %v %v", DB.fn, step, DB.ID, DB.server, DB.gid, fmt.Sprint(args...))
	// log.Printf("[%v.%v] ID: %v Server: %v ", DB.fn, step, DB.ID, DB.server)
}


func makeDebugger(fn string, ID int64, server int, gid int64) Debugger {
	return Debugger{fn: fn, ID: ID, server: server, gid: gid}
}

func (op *Op) toString() string {
	var ans bytes.Buffer

	switch agree := op.Agree.(type) {
	case GetAgree:
		ans.WriteString("GetAgree: ")
		ans.WriteString(agree.toString())

	case PutAgree:
		ans.WriteString("PutAgree: ")
		ans.WriteString(agree.toString())

	case PrepareConfigAgree:
		ans.WriteString("PrepareConfigAgree: ")
		ans.WriteString(agree.toString())

	case CommitConfigAgree:
		ans.WriteString("CommitConfigAgree: ")
		ans.WriteString(agree.toString())

	case MoveShardAgree:
		ans.WriteString("MoveShardAgree: ")
		ans.WriteString(agree.toString())


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

func (agree *CommitConfigAgree) toString() string {
	var ans bytes.Buffer
	ans.WriteString("Num: ")
	ans.WriteString(fmt.Sprintf("%v", agree.Num))
	ans.WriteString(" NewKPVDict")
	for key, putIDToVal := range agree.NewKPV {
		ans.WriteString(fmt.Sprintf("\t key: %v, putIDToVal: %v\n", key, putIDToVal))
	}
	return ans.String()
}

func (agree *MoveShardAgree) toString() string {
	var ans bytes.Buffer
	ans.WriteString("Shards: ")
	ans.WriteString(fmt.Sprintf("%v", agree.Shards))
	ans.WriteString("ID: ")
	ans.WriteString(fmt.Sprintf("%v", agree.ID))
	ans.WriteString("Num: ")
	ans.WriteString(fmt.Sprintf("%v", agree.Num))
	return ans.String()
}

func (agree *PrepareConfigAgree) toString() string {
	var ans bytes.Buffer
	ans.WriteString("Num: ")
	ans.WriteString(fmt.Sprint(agree.Num))
	ans.WriteString("ID: ")
	ans.WriteString(fmt.Sprint(agree.ID))
	return ans.String()
}


func (agree *GetAgree) toString() string {
	var ans bytes.Buffer
	ans.WriteString("Key: ")
	ans.WriteString(fmt.Sprint(agree.Key))
	ans.WriteString(", ")
	ans.WriteString("GetID: ")
	ans.WriteString(fmt.Sprint(agree.GetID))
	ans.WriteString(", ")
	ans.WriteString("LastPutID: ")
	ans.WriteString(fmt.Sprint(agree.LastPutID))
	return ans.String()
}

func (agree *PutAgree) toString() string {
	var ans bytes.Buffer
	ans.WriteString("Key: ")
	ans.WriteString(fmt.Sprint(agree.Key))
	ans.WriteString(", ")
	ans.WriteString("PutID: ")
	ans.WriteString(fmt.Sprint(agree.PutID))
	ans.WriteString(", ")
	ans.WriteString("Val: ")
	ans.WriteString(fmt.Sprint(agree.Val))
	ans.WriteString(", ")
	ans.WriteString("DoHash: ")
	ans.WriteString(fmt.Sprint(agree.DoHash))
	return ans.String()
}

func (kpv *KeyToPastPutIDToValue) toString() string {
	var ans bytes.Buffer
	for key, od := range kpv.mapping {
		ans.WriteString(fmt.Sprintf("\tkey: %v, putIDtoVal: %v\n", key, *od))
	}
	return ans.String()
}

func (kpv *KeyToPastPutIDToValue) printSize() string {
	var ans bytes.Buffer
	total := 0
	for key, value := range kpv.mapping {
		ans.WriteString(fmt.Sprintf("\tkey: %v, size: %v", key, value.size()))
		ans.WriteString("\n")
		total += value.size()
	}
	ans.WriteString(fmt.Sprintf("total cnt: %v", total))
	return ans.String()
}
