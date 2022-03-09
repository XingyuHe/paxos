package paxos
import (
	"fmt"
)

// issue outgoing rpc
func (px *Paxos) startPaxo(seq int, v interface{}) {
	DB := makeDebugger("startPaxo", px.me, seq)
	DB.printf(1, fmt.Sprintf("seq: %v, v: %v", seq, v))

	nextN := 0

	// TODO: server 0 grabs lock and proposes to server 1, waiting for server 1 lock
	// server 1 already holds lock and proposes to server 1, waiting for its lock
	DB.printf(2, fmt.Sprintf("seq: %v, v: %v", seq, v))

	for (!px.isDecidedSafe(seq) && !px.isForgottenSafe(seq) && !px.dead) {
		N := nextN
		nextN = N + 1

		// propose
		DB.printf(3, fmt.Sprintf("seq: %v N: %v nextN: %v", seq, N, nextN))
		ok, vPrime, highestNpFromPropose := px.propose(seq, N)
		nextN = Max(nextN, highestNpFromPropose + 1)
		if (!ok) {
			DB.printf(4, fmt.Sprintf("start fail seq: %v N: %v nextN: %v", seq, N, nextN))
			continue
		}

		// accept
		if (vPrime == nil) { // TODO: not sure if this works
			vPrime = v
		}
		ok, highestNpFromAccept := px.accept(seq, N, vPrime)
		nextN = Max(nextN, highestNpFromAccept + 1)

		if (!ok) {
			DB.printf(5, fmt.Sprintf("start fail seq: %v N: %v nextN: %v", seq, N, nextN))
			continue
		}
		// decide
		px.decide(seq, vPrime)

		// update N for the next round
		px.waitRand()
	}
}

func (px *Paxos) decide(seq int, v interface {}) {
	for i, peer := range px.peers {
		args, reply := px.buildDecideArgsReply(seq, v)
		if (px.isSelf(i)) {
			px.PaxosProtocol(&args, &reply)
		} else {
			call(peer, "Paxos.PaxosProtocol", &args, &reply)
		}
	}
}



func (px *Paxos) accept(seq int, N int, v interface {}) (bool, int) {
	agreeCnt := 0
	DB := makeDebugger("accept", px.me, seq)
	highestNp := -1

	for i, peer := range px.peers {

		DB.printf(1, fmt.Sprintf("seq: %v, Np: %v, v: %v, to peer %v", seq, N, v, i))
		args, reply := px.buildAcceptArgsReply(seq, N, v)
		if (px.isSelf(i)) {
			DB.printf(2, fmt.Sprintf("seq: %v, Np: %v, v: %v, to peer %v", seq, N, v, i))
			px.PaxosProtocol(&args, &reply)
		} else {
			DB.printf(3, fmt.Sprintf("seq: %v, Np: %v, v: %v, to peer %v", seq, N, v, i))
			ok := call(peer, "Paxos.PaxosProtocol", &args, &reply)
			DB.printf(4, fmt.Sprintf("seq: %v, Np: %v, v: %v, to peer %v", seq, N, v, i))
			if (!ok) {
				DB.printf(11, fmt.Sprintf("connection fail seq: %v, Np: %v, v: %v, to peer %v", seq, N, v, i))
				continue
			}
		}

		handlerReply := reply.HandlerReply.(AcceptReply)

		if (!handlerReply.Ok) {
			DB.printf(5, fmt.Sprintf("seq: %v, Np: %v, v: %v, to peer %v", seq, N, v, i))
		} else {
			DB.printf(6, fmt.Sprintf("seq: %v, Np: %v, v: %v, to peer %v", seq, N, v, i))
			agreeCnt++
		}
		highestNp = Max(highestNp, handlerReply.Np)
	}
	return px.isMajority(agreeCnt), highestNp
}

func (px *Paxos) propose(seq int, N int) (bool, interface {}, int) {
	agreeCnt := 0
	highestNa := -1
	highestNp := -1
	var highestVa interface {}
	DB := makeDebugger("propose", px.me, seq)

	for i, peer := range px.peers {

		DB.printf(1, fmt.Sprintf("to peer %v", i))
		args, reply := px.buildPrepareArgsReply(seq, N)
		DB.printf(2, fmt.Sprintf("to peer %v", i))
		if (px.isSelf(i)) {
			DB.printf(3, fmt.Sprintf("to peer %v", i))
			px.PaxosProtocol(&args, &reply)
		} else {
			DB.printf(4, fmt.Sprintf("to peer %v", i))
			ok := call(peer, "Paxos.PaxosProtocol", &args, &reply)
			if (!ok) {
				DB.printf(5, fmt.Sprintf("connection fail to peer %v", i))
				continue
				// TODO: createa new function that checks if PAXOS is over based on
				// 	PAXOS args and based on seqToState
			}
		}
		handlerReply := reply.HandlerReply.(PrepareReply)
		DB.printf(6, fmt.Sprintf("seq: %v, Np: %v, Na: %v, Va: %v", seq, handlerReply.Np, handlerReply.Na, handlerReply.Va))

		if (handlerReply.Ok) {
			agreeCnt++
			DB.printf(7, fmt.Sprintf("seq: %v 6 peer %v, handlerNa: %v, hgihestNa: %v", seq, i, handlerReply.Na, highestNa))
			if (handlerReply.Na > highestNa) {
			DB.printf(8, fmt.Sprintf("to peer %v", i))
				highestNa = handlerReply.Na
			DB.printf(9, fmt.Sprintf("to peer %v", i))
				highestVa = handlerReply.Va
			DB.printf(10, fmt.Sprintf("to peer %v", i))
			}
		}
		DB.printf(12, fmt.Sprintf("to peer %v, highestVa: %v, highestNa: %v", i, highestVa, highestNa))
		highestNp = Max(highestNp, handlerReply.Np)
	}
	DB.printf(13, "")
	return px.isMajority(agreeCnt), highestVa, highestNp
}


