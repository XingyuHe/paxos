package paxos

import "sync"

type PaxosState struct {
	np int
	na int
	va interface {}
	mu sync.Mutex
}

type PaxosArgs struct {
	HandlerArgs interface {}
	SrcPeerIdx int
	Seq int
	Done int
}

type PaxosReply struct {
	HandlerReply interface {}
	Done int
}

type PrepareArgs struct {
	Seq int
	N int
}

type PrepareReply struct {
	Np int
	Na int
	Va interface {}
	Ok bool
}

type AcceptArgs struct {
	Seq int
	N int
	V interface {}
}

type AcceptReply struct {
	Np int
	Ok bool
}

type DecideArgs struct {
	Seq int
	V interface {}
}

type DecideReply struct {
}

type ProposeRes struct {
	ok bool
	highestVa interface {}
}

