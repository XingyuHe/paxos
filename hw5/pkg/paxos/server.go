package paxos

import (
	"coms4113/hw5/pkg/base"
	"fmt"
)

const (
	Propose = "propose"
	Accept  = "accept"
	Decide  = "decide"
)

type Proposer struct {
	N             int
	Phase         string
	N_a_max       int
	V             interface{}
	SuccessCount  int
	ResponseCount int
	// To indicate if response from peer is received, should be initialized as []bool of len(server.peers)
	Responses []bool
	// Use this field to check if a message is latest.
	SessionId int

	// in case node will propose again - restore initial value
	InitialValue interface{}
}

type ServerAttribute struct {
	peers []base.Address
	me    int

	// Paxos parameter
	n_p int
	n_a int
	v_a interface{}

	// final result
	agreedValue interface{}

	// Propose parameter
	proposer Proposer

	// retry
	timeout *TimeoutTimer
}

type Server struct {
	base.CoreNode
	ServerAttribute
}

func NewServer(peers []base.Address, me int, proposedValue interface{}) *Server {
	response := make([]bool, len(peers))
	return &Server{
		CoreNode: base.CoreNode{},
		ServerAttribute: ServerAttribute{
			peers: peers,
			me:    me,
			proposer: Proposer{
				InitialValue: proposedValue,
				Responses:    response,
			},
			timeout: &TimeoutTimer{},
		},
	}
}

func (server *Server) MessageHandler(message base.Message) []base.Node {
	//TODO: implement it
	nextServers := make([]base.Node, 0)
	switch msg := message.(type) {
	case *ProposeRequest:
		newServer := server.handlePrepareRequest(msg)
		nextServers = append(nextServers, newServer)

	case *AcceptRequest:
		newServer := server.handleAcceptRequest(msg)
		nextServers = append(nextServers, newServer)

	case *DecideRequest:
		newServer := server.handleDecideRequest(msg)
		nextServers = append(nextServers, newServer)

	case *ProposeResponse:
		newServers := server.handleProposerResponse(msg)
		for _, nServer := range newServers {
			nextServers = append(nextServers, nServer)
		}
	case *AcceptResponse:
		newServers := server.handleAcceptResponse(msg)
		for _, nServer := range newServers {
			nextServers = append(nextServers, nServer)
		}

	case *DecideResponse:
	}

	return nextServers
}

// To start a new round of Paxos.
func (server *Server) StartPropose() {
	//TODO: implement it
	DB := base.MakeDebugger("StartPropose", server.me)
	proposerResponse := make([]bool, len(server.peers))
	for i, _ := range proposerResponse {
		proposerResponse[i] = false
	}
	// proposer := buildProposer(1, Propose, server.getAddress(), 0, 0, proposerResponse, 1, server.getAddress())

	proposer := Proposer{
			N:             server.n_p + 1,
			N_a_max: 		0,
			Phase:         Propose,
			V:             server.proposer.InitialValue,
			SuccessCount:  0,
			ResponseCount: 0,
			Responses:     proposerResponse,
			SessionId:     server.proposer.SessionId + 1,
			InitialValue:  server.proposer.InitialValue,
	}
	server.proposer = proposer
	DB.Printf(1, "initial value ", server.proposer.InitialValue)
	server.setProposalResponse()
}


// Returns a deep copy of server node
func (server *Server) copy() *Server {
	response := make([]bool, len(server.peers))
	for i, flag := range server.proposer.Responses {
		response[i] = flag
	}

	var copyServer Server
	copyServer.me = server.me
	// shallow copy is enough, assuming it won't change
	copyServer.peers = server.peers
	copyServer.n_a = server.n_a
	copyServer.n_p = server.n_p
	copyServer.v_a = server.v_a
	copyServer.agreedValue = server.agreedValue
	copyServer.proposer = Proposer{
		N:             server.proposer.N,
		Phase:         server.proposer.Phase,
		N_a_max:       server.proposer.N_a_max,
		V:             server.proposer.V,
		SuccessCount:  server.proposer.SuccessCount,
		ResponseCount: server.proposer.ResponseCount,
		Responses:     response,
		InitialValue:  server.proposer.InitialValue,
		SessionId:     server.proposer.SessionId,
	}

	// doesn't matter, timeout timer is state-less
	copyServer.timeout = server.timeout

	return &copyServer
}

func (server *Server) NextTimer() base.Timer {
	return server.timeout
}

// A TimeoutTimer tick simulates the situation where a proposal procedure times out.
// It will close the current Paxos round and start a new one if no consensus reached so far,
// i.e. the server after timer tick will reset and restart from the first phase if Paxos not decided.
// The timer will not be activated if an agreed value is set.
func (server *Server) TriggerTimer() []base.Node {
	if server.timeout == nil {
		return nil
	}

	subNode := server.copy()
	subNode.StartPropose()

	return []base.Node{subNode}
}

func (server *Server) Attribute() interface{} {
	return server.ServerAttribute
}

func (server *Server) Copy() base.Node {
	return server.copy()
}

func (server *Server) Hash() uint64 {
	return base.Hash("paxos", server.ServerAttribute)
}

func (server *Server) Equals(other base.Node) bool {
	otherServer, ok := other.(*Server)

	if !ok || server.me != otherServer.me ||
		server.n_p != otherServer.n_p || server.n_a != otherServer.n_a || server.v_a != otherServer.v_a ||
		(server.timeout == nil) != (otherServer.timeout == nil) {
			fmt.Printf("[Equals] server struct do not match\n")
		return false
	}

	if server.proposer.N != otherServer.proposer.N || server.proposer.V != otherServer.proposer.V ||
		server.proposer.N_a_max != otherServer.proposer.N_a_max || server.proposer.Phase != otherServer.proposer.Phase ||
		server.proposer.InitialValue != otherServer.proposer.InitialValue ||
		server.proposer.SuccessCount != otherServer.proposer.SuccessCount ||
		server.proposer.ResponseCount != otherServer.proposer.ResponseCount {
			fmt.Printf("[Equals] proposer struct do not match\n")
			fmt.Printf("%v \n", server.proposer)
			fmt.Printf("%v \n", otherServer.proposer)
		return false
	}

	for i, response := range server.proposer.Responses {
		if response != otherServer.proposer.Responses[i] {
			fmt.Printf("[Equals] proposer response do not match, i: %v\n", i)
			return false
		}
	}

	return true
}

func (server *Server) Address() base.Address {
	return server.peers[server.me]
}
