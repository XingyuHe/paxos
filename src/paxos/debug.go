package paxos
// import "log"


type Debugger struct {
	fn string
	peerCalled int
	seq int
}

func (DB *Debugger) printf(step int, args string) {
	// log.Printf("[%v. %v] peer: %v seq: %v args: %v", DB.fn, step, DB.peerCalled, DB.seq, args)
	// log.Printf("[%v. %v] peer: %v ", DB.fn, step, DB.peerCalled)
}

func makeDebugger(fn string, peerCalled int, seq int) Debugger {
	return Debugger{fn: fn, peerCalled:  peerCalled, seq: seq}
}