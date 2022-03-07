package paxos
import (
	"fmt"
)

// issue outgoing rpc
func (px *Paxos) startPaxo(seq int, v interface{}) {
	DB := makeDebugger("startPaxo", px.me)
	DB.printf(1, fmt.Sprintf("seq: %v, v: %v", seq, v))

	nextN := 0

	// TODO: server 0 grabs lock and proposes to server 1, waiting for server 1 lock
	// server 1 already holds lock and proposes to server 1, waiting for its lock
	DB.printf(2, fmt.Sprintf("seq: %v, v: %v", seq, v))

	for (!px.isDecided(seq)) {
		N := nextN
		nextN = N + 1

		// propose
		DB.printf(2, "propose")
		DB.printf(3, fmt.Sprintf("N: %v nextN: %v", N, nextN))
		ok, vPrime, highestNpFromPropose := px.propose(seq, N)
		nextN = Max(nextN, highestNpFromPropose + 1)
		if (!ok) {
			continue
		}

		// accept
		if (vPrime == nil) { // TODO: not sure if this works
			vPrime = v
		}
		ok, highestNpFromAccept := px.accept(seq, N, vPrime)
		nextN = Max(nextN, highestNpFromAccept + 1)

		if (!ok) {
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
	DB := makeDebugger("accept", px.me)
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
	DB := makeDebugger("propose", px.me)

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
				DB.printf(5, fmt.Sprintf("to peer %v", i))
				continue
			}
		}
		handlerReply := reply.HandlerReply.(PrepareReply)
		DB.printf(6, fmt.Sprintf("Seq: %v, Np: %v, Na: %v, Va: %v", seq, handlerReply.Np, handlerReply.Na, handlerReply.Va))

		if (handlerReply.Ok) {
			agreeCnt++
			DB.printf(7, fmt.Sprintf("6 peer %v, handlerNa: %v, hgihestNa: %v", i, handlerReply.Na, highestNa))
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



// handle incoming rpc
func (px *Paxos) prepareHandler(args *PrepareArgs, reply *PrepareReply) {
	DB := makeDebugger("prepareHandler", px.me)
	DB.printf(1, fmt.Sprintf("Seq: %v, N: %v", args.Seq, args.N))
	state := px.seqToState[args.Seq]
	reply.Np = state.np
	if (args.N > state.np) {
		DB.printf(2, fmt.Sprintf("Seq: %v, N: %v", args.Seq, args.N))
		px.updateStateNp(args.Seq, args.N)
		reply.Va = state.va
		reply.Na = state.na
		reply.Ok = true
		DB.printf(3, fmt.Sprintf("Seq: %v, N: %v", args.Seq, args.N))
	} else {
		DB.printf(4, fmt.Sprintf("Seq: %v, N: %v", args.Seq, args.N))
		reply.Ok = false
	}
	DB.printf(5, fmt.Sprintf("Seq: %v, Na: %v, Np: %v, Va: %v", args.Seq, reply.Na, reply.Np, reply.Va))
}
