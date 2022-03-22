package paxos
// import "log"
import "bytes"
import "fmt"
import "sort"


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

func (px *Paxos) SeqToStateToString() string {
	px.mu.Lock(); defer px.mu.Unlock()

	var keys []int
	for seq, _ := range px.seqToState {
		keys = append(keys, seq)
	}

	sort.Ints(keys)

	var ans bytes.Buffer
	ans.WriteString("seqToState: ")
	for _, key := range keys {
		ans.WriteString(fmt.Sprintf("\t seq: %v, state: %v\n", key, px.seqToState[key].va))
	}
	return ans.String()
}