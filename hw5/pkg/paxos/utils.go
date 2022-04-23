package paxos

import (
	"coms4113/hw5/pkg/base"
	"fmt"
)

// changing PAXOS state

func (server *Server) updateStateVa(v interface {}) *Server{
	newServer := server.copy()
	px := newServer.Attribute().(ServerAttribute)
	px.v_a = v
	newServer.ServerAttribute = px
	return newServer
}

func (server *Server) updateStateNa(na int) *Server{
	newServer := server.copy()
	px := newServer.Attribute().(ServerAttribute)
	px.n_a = na
	newServer.ServerAttribute = px
	return newServer
}

func (server *Server) updateStateNp(np int) {
	server.n_p = np
}

func (server *Server) updateProposer(proposer Proposer) {
	server.proposer = proposer
}

func (server *Server) updateProposerPhase(phase string) {
	server.proposer.Phase = phase
}

func (server *Server) updateProposerN(N int) {
	server.proposer.N = N
}

func (server *Server) resetProposer() {
	server.proposer.SuccessCount = 0
	server.proposer.ResponseCount = 0
	server.proposer.Responses = make([]bool, len(server.proposer.Responses))
}

func (server *Server) updateState(np int, na int, va interface {}) {
	server.n_p = np
	server.n_a = na
	server.v_a = va
}

func (server *Server) updateStateDecided(va interface {}) {
	server.v_a = va
}

// update proposer info
func (server *Server) updateProposerResponses(response *ProposeResponse) {

	responderAddr := response.From()
	responderIdx := server.findAddrIdx(responderAddr)

	if response.Ok {
		server.proposer.SuccessCount++
	}
	server.proposer.ResponseCount++
	server.proposer.Responses[responderIdx] = response.Ok
}

func (server *Server) updateAcceptorResponses(response *AcceptResponse) {

	responderAddr := response.From()
	responderIdx := server.findAddrIdx(responderAddr)

	if response.Ok {
		server.proposer.SuccessCount++
	}
	server.proposer.ResponseCount++
	server.proposer.Responses[responderIdx] = response.Ok
}

func (server *Server) updateProposerVaNa(response *ProposeResponse) {
	if !base.IsNil(response.V_a) {
		if server.proposer.N_a_max < response.N_a {
			server.proposer.N_a_max = response.N_a
			server.proposer.V = response.V_a
		}
	}
}



// build args
func buildPrepareArgsReply(n int, sessionId int) (PrepareArgs, PrepareReply) {
	return PrepareArgs{N: n, SessionId: sessionId}, PrepareReply{}
}

func buildAcceptArgsReply(n int, v interface {}, sessionId int) (AcceptArgs, AcceptReply) {
	return AcceptArgs{N: n, V: v, SessionId: sessionId}, AcceptReply{}
}

func buildDecideArgsReply(v interface {}, sessionId int) (DecideArgs, DecideReply) {
	return DecideArgs{V: v, SessionId: sessionId}, DecideReply{}
}

// build request
func buildProposeRequest(from, to base.Address, N int, sessionId int) *ProposeRequest{
	res := ProposeRequest{
		CoreMessage: base.MakeCoreMessage(from, to),
		N: N,
		SessionId: sessionId,
	}
	return &res
}

func buildAcceptRequest(from, to base.Address, N int, V interface {}, sessionId int) *AcceptRequest {
	res := AcceptRequest{
		CoreMessage: base.MakeCoreMessage(from, to),
		N: N,
		V: V,
		SessionId: sessionId,
	}
	return &res
}

func buildDecideRequest(from base.Address, to base.Address, va interface {}, sessionId int) *DecideRequest{
	res := DecideRequest{
		CoreMessage: base.MakeCoreMessage(from, to),
		V: va,
		SessionId: sessionId,
	}
	return &res
}

// build response
func buildProposeResponse(from, to base.Address, ok bool, np int, na int, va interface {}, sessionId int) *ProposeResponse{
	res := ProposeResponse{
		CoreMessage: base.MakeCoreMessage(from, to),
		Ok: ok,
		N_p: np,
		N_a: na,
		V_a: va,
		SessionId: sessionId,
	}
	return &res
}

// reply to response
func (reply *PrepareReply) toResponse(from, to base.Address, sessionId int) *ProposeResponse{
	return buildProposeResponse(from, to, reply.Ok, reply.Np, reply.Na, reply.Va, sessionId)
}

func buildProposer(N int, phase string, V interface {}, successCount int, responseCount int, responses []bool, sessionId int, initialValue interface {}) Proposer {
	return Proposer{
			N:             N,
			Phase:         phase,
			V:             V,
			SuccessCount:  successCount,
			ResponseCount: responseCount,
			Responses:     responses,
			SessionId:     sessionId,
			InitialValue:  initialValue,
	}
}

// others
func (server *Server) getAddress() base.Address {
	return server.peers[server.me]
}
func Max(a int, b int) int {
	if (a > b) {
		return a
	}
	return b
}
func (server *Server) findAddrIdx(addr base.Address) int {
	fmt.Printf("peers: %v\n", server.peers)
	fmt.Printf("target addr: %v\n", addr)
	for i, tempAddr := range server.peers {
		if tempAddr == addr {
			return i
		}
	}
	return -1
}

func (prp *Proposer) majoritySuccess() bool {
	successCnt := 0
	for _, resp := range prp.Responses {
		if resp {
			successCnt++
		}
	}
	return successCnt > len(prp.Responses) / 2
}

func (prp *Proposer) majorityFail() bool {
	failCnt := prp.ResponseCount - prp.SuccessCount
	return failCnt > len(prp.Responses) / 2
}