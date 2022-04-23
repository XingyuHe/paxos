package base

// import "log"
// ort "fmt"


type Debugger struct {
	fn string
	peerCalled int
	seq int
}

func (DB *Debugger) Printf(step int, args ...interface{}) {
	// log.Printf("[%v.%v] Server: %v %v", DB.fn, step, DB.peerCalled, fmt.Sprint(args...))
	// log.Printf("[%v.%v] ID: %v Server: %v ", DB.fn, step, DB.ID, DB.server)
}

func MakeDebugger(fn string, peerCalled int) Debugger {
	return Debugger{fn: fn, peerCalled:  peerCalled}
}
