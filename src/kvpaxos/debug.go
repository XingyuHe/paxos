package kvpaxos
import (
	"log"
	// "fmt"
)


type Debugger struct {
	fn string
	ID int64
	server int
}

func (DB *Debugger) printf(step int, args ...interface{}) {
	// log.Printf("[%v.%v] ID: %v Server: %v %v", DB.fn, step, DB.ID, DB.server, fmt.Sprint(args...))
	log.Printf("[%v.%v] ID: %v Server: %v ", DB.fn, step, DB.ID, DB.server)
}


func makeDebugger(fn string, ID int64, server int) Debugger {
	return Debugger{fn: fn, ID: ID, server: server}
}