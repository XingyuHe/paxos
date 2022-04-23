package paxos

import (
	"coms4113/hw5/pkg/base"
	// "fmt"
)

// issue outgoing rpc

func (server *Server) handleAcceptResponse(response *AcceptResponse) []*Server {

	DB := base.MakeDebugger("handleAcceptResponse", server.me)
	DB.Printf(1, "proposer V ", server.proposer.V)

	nextServers := make([]*Server, 0)

	if response.SessionId != server.proposer.SessionId ||
	server.proposer.Phase != Accept {
		DB.Printf(-1, "wrong sessionId or wrong phase")
		nextServers = append(nextServers, server.copy())
		return nextServers
	}

	baseNewServer := server.copy()
	baseNewServer.updateAcceptorResponses(response)
	DB.Printf(2, "responder status: ", baseNewServer.proposer.Responses, " number of responses: ", len(baseNewServer.proposer.Responses), " success threshold: ", len(baseNewServer.proposer.Responses) / 2)


	if baseNewServer.proposer.majoritySuccess() {
		newDecideServer := baseNewServer.copy()
		newDecideServer.proposer.Phase = Decide
		newDecideServer.proposer.Responses = make([]bool, len(server.peers))
		newDecideServer.proposer.ResponseCount = 0
		newDecideServer.proposer.SuccessCount = 0


		DB.Printf(10, "decided, ", newDecideServer.proposer.Phase)

		requestMsg := make([]base.Message, 0)

		for _, peer := range newDecideServer.peers {
			var msg base.Message
			DB.Printf(1, "calling peer ", peer, " with V ", newDecideServer.proposer.V)
			msg = &DecideRequest{
				base.MakeCoreMessage(server.getAddress(), peer),
				newDecideServer.proposer.V,
				newDecideServer.proposer.SessionId,
			}
			requestMsg = append(requestMsg, msg)
		}

		newDecideServer.SetResponse(requestMsg)

		nextServers = append(nextServers, baseNewServer)
		nextServers = append(nextServers, newDecideServer)
	} else {
		nextServers = append(nextServers, baseNewServer)
	}

	return nextServers

}

func (server *Server) handleProposerResponse(response *ProposeResponse) []*Server {

	DB := base.MakeDebugger("handleProposerResponse", server.me)
	DB.Printf(1, "response ", response.V_a)

	nextServers := make([]*Server, 0)

	if response.SessionId != server.proposer.SessionId ||
	server.proposer.Phase != Propose {
		DB.Printf(-1, "server sessionId wrong or phase is wrong")
		nextServers = append(nextServers, server.copy())
		return nextServers
	}

	baseNewServer := server.copy()
	baseNewServer.updateProposerResponses(response)
	baseNewServer.updateProposerVaNa(response)

	DB.Printf(2, "responder status: ", baseNewServer.proposer.Responses, " number of responses: ", len(baseNewServer.proposer.Responses), " success threshold: ", len(baseNewServer.proposer.Responses) / 2, " current proposer V ", baseNewServer.proposer.V)

	if baseNewServer.proposer.majoritySuccess() && !base.IsNil(baseNewServer.proposer.V) {
		DB.Printf(10, "original, ", server.proposer.V)
		DB.Printf(10, "baseNewServer, ", baseNewServer.proposer.V)
		newAcceptServer := baseNewServer.copy()
		newAcceptServer.updateProposerPhase(Accept)
		newAcceptServer.resetProposer()

		DB.Printf(10, "majority success, ", newAcceptServer.proposer.V)

		responseMsg := make([]base.Message, 0)

		for _, peer := range newAcceptServer.peers {
			var msg base.Message
			DB.Printf(1, "calling peer ", peer, " with V ", newAcceptServer.proposer.V)
			msg = &AcceptRequest{
				CoreMessage: base.MakeCoreMessage(server.getAddress(), peer),
				N: newAcceptServer.proposer.N,
				V: newAcceptServer.proposer.V,
				SessionId: newAcceptServer.proposer.SessionId,
			}
			responseMsg = append(responseMsg, msg)
		}

		newAcceptServer.SetResponse(responseMsg)

		nextServers = append(nextServers, baseNewServer)
		nextServers = append(nextServers, newAcceptServer)
		DB.Printf(1, "sucess ", newAcceptServer.proposer.V)
	} else {
		nextServers = append(nextServers, baseNewServer)
	}

	return nextServers
}




func (server *Server) setProposalResponse() {
	px := server.Attribute().(ServerAttribute)
	response := make([]base.Message, 0)

	for _, peer := range px.peers {
		var msg base.Message
		msg = buildProposeRequest(server.getAddress(), peer, server.proposer.N, server.proposer.SessionId)
		response = append(response, msg)
	}

	server.SetResponse(response)
}


