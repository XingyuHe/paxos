package paxos

import (
	"fmt"
)

// handle incoming rpc
func (px *Paxos) prepareHandler(args *PrepareArgs, reply *PrepareReply) {
	DB := makeDebugger("prepareHandler", px.me, args.Seq)
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

func (px *Paxos) acceptHandler(args *AcceptArgs, reply *AcceptReply) {
	DB := makeDebugger("acceptHandler", px.me, args.Seq)
	DB.printf(1, fmt.Sprintf("Seq: %v, V: %v, N: %v", args.Seq,args.V, args.N))

	state := px.seqToState[args.Seq]
	DB.printf(1, fmt.Sprintf("peer state: Np %v, Na %v, V %v", state.np, state.na, state.va))
	reply.Np = state.np
	if (args.N >= state.np) {
	DB.printf(2, fmt.Sprintf("Seq: %v, V: %v, N: %v", args.Seq,args.V, args.N))
		px.updateStateNa(args.Seq, args.N)
		px.updateStateNp(args.Seq, args.N)
		px.updateStateVa(args.Seq, args.V)
		reply.Ok = true

		// TODO: this can form a deadlock
		// px.mu.Lock()
		// px.updatePaxosMaxDone(args.Seq)
		// px.mu.Unlock()
		// where can i update maxDone

	DB.printf(3, fmt.Sprintf("Seq: %v, V: %v, N: %v", args.Seq,args.V, args.N))
	} else {
	DB.printf(4, fmt.Sprintf("Seq: %v, V: %v, N: %v", args.Seq,args.V, args.N))
		reply.Ok = false
	}
}

func (px *Paxos) decideHandler(args *DecideArgs, reply *DecideReply) {
	DB := makeDebugger("decideHandler", px.me, args.Seq)
	DB.printf(1, fmt.Sprintf("seq: %v val: %v ", args.Seq, args.V))
	px.updateStateVa(args.Seq, args.V)
	px.updateStateDecided(args.Seq, true)
}



func (px *Paxos) PaxosProtocol(args *PaxosArgs, reply *PaxosReply) error {

	DB := makeDebugger("PaxosProtocol", px.me, args.Seq)
	DB.printf(1, fmt.Sprintf("seq: %v, from peer: %v", args.Seq, args.SrcPeerIdx))

	px.mu.Lock()
	DB.printf(2, fmt.Sprintf("seq: %v, from peer: %v", args.Seq, args.SrcPeerIdx))
	px.updatePaxosPeerDone(args.SrcPeerIdx, args.Done)
	px.initPaxosState(args.Seq)
	px.mu.Unlock()

	// QUESTION: Done() could be called here and delete px.seqToState[arts.Seq]

	DB.printf(3, fmt.Sprintf("seq: %v, from peer: %v", args.Seq, args.SrcPeerIdx))
	px.mu.RLock(); defer px.mu.RUnlock()

	// TODO:
	// handle args.Seq <= px.Done
	// if it still exists, then it doesn't matter
	// if it doesn't exists, then Min() has been upgraded
	if px.isForgotten(args.Seq) {
		// TODO: should we check px.isDecided? For nodes behind to learn the value
		// directly rejecting it doesn't really allow that
		px.fillPaxosReply(args, reply)
		return nil
	}

	px.seqToState[args.Seq].mu.Lock(); defer px.seqToState[args.Seq].mu.Unlock();

	switch handlerArgs := args.HandlerArgs.(type) {
	case PrepareArgs:
		DB.printf(6, fmt.Sprintf("seq: %v, from peer: %v", args.Seq, args.SrcPeerIdx))
		var handlerReply PrepareReply
		px.prepareHandler(&handlerArgs, &handlerReply)
		reply.HandlerReply = handlerReply
	case AcceptArgs:
		DB.printf(7, fmt.Sprintf("seq: %v, from peer: %v", args.Seq, args.SrcPeerIdx))
		var handlerReply AcceptReply
		px.acceptHandler(&handlerArgs, &handlerReply)
		reply.HandlerReply = handlerReply
	case DecideArgs:
		DB.printf(8, fmt.Sprintf("seq: %v, from peer: %v", args.Seq, args.SrcPeerIdx))
		var handlerReply DecideReply
		px.decideHandler(&handlerArgs, &handlerReply)
		reply.HandlerReply = handlerReply
	default:
		return nil // TODO: better error message
	}
	DB.printf(9, fmt.Sprintf("seq: %v, from peer: %v", args.Seq, args.SrcPeerIdx))
	return nil
}