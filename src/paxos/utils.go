package paxos

import (
	"fmt"
	// "log"
	"math"
	// "math/rand"
	// "time"
)

// generic functions
func MinIntSlice(s []int) int {
	N := len(s)
	min := math.MaxInt

	for i := 0; i < N; i++ {
		if (s[i] < min) { min = s[i] }
	}

	return min
}

func MaxIntSlice(s []int) int {
	N := len(s)
	max := math.MinInt

	for i := 0; i < N; i++ {
		if (s[i] > max) { max = s[i] }
	}

	return max
}

func Max(a int, b int) int {
	if (a > b) {
		return a
	}
	return b
}

// Paxos build arguments
func (px *Paxos) buildPrepareArgsReply(seq int, n int) (PaxosArgs, PaxosReply) {
		var args PaxosArgs
		args.HandlerArgs = PrepareArgs{N: n, Seq: seq}
		args.Done = px.getDone()
		args.SrcPeerIdx = px.me
		args.Seq = seq
		var reply PaxosReply
		reply.HandlerReply = PrepareReply{}
		return args, reply
}


func (px *Paxos) buildAcceptArgsReply(seq int, n int, v interface {}) (PaxosArgs, PaxosReply) {
		var args PaxosArgs
		args.HandlerArgs = AcceptArgs{N: n, V: v, Seq: seq}
		args.Done = px.getDone()
		args.SrcPeerIdx = px.me
		args.Seq = seq
		var reply PaxosReply
		reply.HandlerReply = PrepareReply{}
		return args, reply
}

func (px *Paxos) buildDecideArgsReply(seq int, v interface {}) (PaxosArgs, PaxosReply) {
		var args PaxosArgs
		args.HandlerArgs = DecideArgs{V: v, Seq: seq}
		args.Done = px.getDone()
		args.SrcPeerIdx = px.me
		args.Seq = seq
		var reply PaxosReply
		reply.HandlerReply = DecideReply{}
		return args, reply
}

// Paxos is functions
func (px *Paxos) isMajority(n int) bool {
	return n > len(px.peers) / 2
}

func (px *Paxos) isDecided(seq int) bool {
	if px.isSeqInit(seq) {
		if px.seqToState[seq].va != nil {
			return px.seqToState[seq].decided
		} else {
			return false
		}
	}
	return false
}


func (px *Paxos) isForgotten(seq int) bool {
	return seq < px.Min()
}

func (px *Paxos) isForgottenSafe(seq int) bool {
	px.mu.RLock(); defer px.mu.RUnlock()
	return seq < px.Min()
}

func (px *Paxos) isDecidedSafe(seq int) bool {
	px.mu.RLock(); defer px.mu.RUnlock()
	if px.isSeqInit(seq) {
		if px.seqToState[seq].va != nil {
			return px.seqToState[seq].decided
		} else {
			return false
		}
	}
	return false
}

func (px *Paxos) isSeqInit(seq int) bool {
	 _, ok := px.seqToState[seq]
	 return ok
}

func (px *Paxos) isSelf(peerIdx int) bool {
	return peerIdx == px.me
}

// changing PAXOS state
func (px *Paxos) updateStateDecided(seq int, decided bool) {
	px.seqToState[seq].decided = decided
}

func (px *Paxos) updateStateVa(seq int, v interface {}) {
	px.seqToState[seq].va = v
}

func (px *Paxos) updateStateNa(seq int, na int) {
	px.seqToState[seq].na = na
}

func (px *Paxos) updateStateNp(seq int, np int) {
	px.seqToState[seq].np = np
}

func (px *Paxos) updatePaxosMinDone(newMinDone int) {
	px.minDone = newMinDone
}

func (px *Paxos) updatePaxosMaxDone(newMaxDone int) {
	if (newMaxDone > px.maxDone) {
		px.maxDone = newMaxDone
	}
}

func (px *Paxos) updatePaxosPeerDone(peerIdx int, done int) {
	DB := makeDebugger("updatePaxosPeerDone", px.me, -1)
	DB.printf(1, fmt.Sprintf("minDone %v from peer %v, done value %v", px.minDone, peerIdx, done))

	if (done != px.peerDone[peerIdx]) {
		DB.printf(2, fmt.Sprintf("from peer %v, done value %v", peerIdx, done))

		// decide whether do update Min()
		if (px.peerDone[peerIdx] == px.minDone) {
			px.peerDone[peerIdx] = done
			tempMinDone := MinIntSlice(px.peerDone)
			DB.printf(3, fmt.Sprintf("from peer %v, done value %v, tempMinDone %v", peerIdx, done, tempMinDone))

			// delete unnecessary instance from the changed Min()
			if (tempMinDone != px.minDone) {
				for seq := px.minDone; seq <= tempMinDone; seq++ {
					delete(px.seqToState, seq)
				}
				px.updatePaxosMinDone(tempMinDone)
				DB.printf(4, fmt.Sprintf("minDone %v from peer %v, done value %v", px.minDone, peerIdx, done))
			}
		} else {
			px.peerDone[peerIdx] = done
		}

	}
}

func (px *Paxos) initPaxosState(seq int) {
	DB := makeDebugger("initPaxosState", px.me, seq)
	if !px.isSeqInit(seq) && !px.isForgotten(seq) {
		DB.printf(1, fmt.Sprintf("new paxo instance seq %v", seq))
		px.seqToState[seq] = &PaxosState{np: -1, na: -1, va: nil, decided: false}
	}
}

func (px *Paxos) getDone() int {
	return px.peerDone[px.me]
}

func (px *Paxos) waitRand() {
	// log.Printf("[waitRand]: begin waiting")
	// if rand.Int() % 2 == 0 {
	// 	time.Sleep(time.Duration(1 * time.Second))
	// }
	// log.Printf("[waitRand]: end waiting")
}


func (px *Paxos) fillPaxosReply(args *PaxosArgs, reply *PaxosReply) {
	switch args.HandlerArgs.(type) {
	case PrepareArgs:
		reply.HandlerReply = PrepareReply{}
	case AcceptArgs:
		reply.HandlerReply = AcceptReply{}
	case DecideArgs:
		reply.HandlerReply = DecideReply{}
	}
}
