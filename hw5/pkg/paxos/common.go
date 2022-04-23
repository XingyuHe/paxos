package paxos

import (
	"bytes"
	"coms4113/hw5/pkg/base"
	"fmt"
	"sync"
)

type PaxosState struct {
	np int
	na int
	va interface {}
	decided bool
	mu sync.Mutex
}

type PaxosArgs struct {
	HandlerArgs interface {}
	SrcPeerIdx int
	Done int
}

type PaxosReply struct {
	HandlerReply interface {}
	Done int
	Decided bool
	Forgotten bool
	Va interface {}
}

type PrepareArgs struct {
	N int
	SessionId int
}

type PrepareReply struct {
	Np int
	Na int
	Va interface {}
	Ok bool
}

type AcceptArgs struct {
	N int
	V interface {}
	SessionId int
}

type AcceptReply struct {
	Np int
	Ok bool
}

type DecideArgs struct {
	V interface {}
	SessionId int
}

type DecideReply struct {
}

type ProposeRes struct {
	ok bool
	highestVa interface {}
}


func MsgToStr(message base.Message) string {
	var ans bytes.Buffer

	switch msg := message.(type) {
	case *ProposeRequest:
		ans.WriteString("ProposeRequest: ")
		ans.WriteString(fmt.Sprintf("%v", msg))

	case *AcceptRequest:
		ans.WriteString("AcceptRequest: ")
		ans.WriteString(fmt.Sprintf("%v", msg))

	case *DecideRequest:
		ans.WriteString("DecideRequest: ")
		ans.WriteString(fmt.Sprintf("%v", msg))

	case *ProposeResponse:
		ans.WriteString("ProposeResponse: ")
		ans.WriteString(fmt.Sprintf("%v", msg))

	case *AcceptResponse:
		ans.WriteString("AcceptResponse: ")
		ans.WriteString(fmt.Sprintf("%v", msg))

	case *DecideResponse:
		ans.WriteString("DecideResponse: ")
		ans.WriteString(fmt.Sprintf("%v", msg))
	}

	ans.WriteString("\n")
	return ans.String()
}