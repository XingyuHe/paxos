package shardmaster
import (
	"fmt"
	"bytes"
	// "log"
)


type Debugger struct {
	fn string
	ID int64
	server int
}

func (DB *Debugger) printf(step int, args ...interface{}) {
	// log.Printf("[%v.%v] ID: %v Server: %v %v", DB.fn, step, DB.ID, DB.server, fmt.Sprint(args...))
	// log.Printf("[%v.%v] ID: %v Server: %v ", DB.fn, step, DB.ID, DB.server)
}


func makeDebugger(fn string, ID int64, server int) Debugger {
	return Debugger{fn: fn, ID: ID, server: server}
}

func (op *Op) toString() string {
	var ans bytes.Buffer

	switch args := op.Agree.(type) {
	case JoinArgs:
		ans.WriteString("JoinArgs: ")
		ans.WriteString(args.toString())
	case LeaveArgs:
		ans.WriteString("LeaveArgs: ")
		ans.WriteString(args.toString())
	case MoveArgs:
		ans.WriteString("MoveArgs: ")
		ans.WriteString(args.toString())
	case QueryArgs:
		ans.WriteString("QueryArgs: ")
		ans.WriteString(args.toString())

	}
	return ans.String()
}

func (args *JoinArgs) toString() string {
	var ans bytes.Buffer
	ans.WriteString("GID: ")
	ans.WriteString(fmt.Sprint(args.GID))
	ans.WriteString(", New Replica group:")

	for server := range args.Servers {
		ans.WriteString(fmt.Sprintf("\tserver %v\n", server))
	}

	return ans.String()
}

func (args *LeaveArgs) toString() string {
	var ans bytes.Buffer
	ans.WriteString("GID: ")
	ans.WriteString(fmt.Sprint(args.GID))

	return ans.String()
}

func (args *MoveArgs) toString() string {
	var ans bytes.Buffer
	ans.WriteString("Shard: ")
	ans.WriteString(fmt.Sprint(args.Shard))
	ans.WriteString(", GID: ")
	ans.WriteString(fmt.Sprint(args.GID))

	return ans.String()
}

func (args *QueryArgs) toString() string {
	var ans bytes.Buffer
	ans.WriteString("Num: ")
	ans.WriteString(fmt.Sprint(args.Num))
	return ans.String()
}

func (sm *ShardMaster) allConfigToString() string {
	var ans bytes.Buffer
	for _, config := range sm.configs {
		ans.WriteString(fmt.Sprintf("\t%v\n", config))
	}
	return ans.String()
}