package paxos

import (
	"coms4113/hw5/pkg/base"
	"fmt"
)

func (server *Server) handlePrepareRequest(args *ProposeRequest) *Server {
	DB := base.MakeDebugger("handlePrepareRequest", server.me)
	DB.Printf(1, args)


	newServer := server.copy()

	var response ProposeResponse
	response.N_p = server.n_p
	response.CoreMessage = base.MakeCoreMessage(args.To(), args.From())
	response.SessionId = args.SessionId

	if (args.N > server.n_p) {
		DB.Printf(2, fmt.Sprintf("N: %v", args.N))

		newServer.n_p = args.N

		response.V_a = server.v_a
		response.N_a = server.n_a
		response.Ok = true

		DB.Printf(3, fmt.Sprintf("N: %v", args.N))
	} else {
		DB.Printf(4, fmt.Sprintf("N: %v", args.N))
		response.Ok = false

	}
	DB.Printf(5, fmt.Sprintf("Na: %v, Np: %v, Va: %v", response.N_a, response.N_p, response.V_a))

	newServer.SetSingleResponse(&response)
	return newServer
}

func (server *Server) handleAcceptRequest(args *AcceptRequest) *Server {

	newServer := server.copy()
	DB := base.MakeDebugger("handleAcceptRequest", server.me)
	DB.Printf(1, args)

	DB.Printf(1, fmt.Sprintf("peer state: Np %v, Na %v, V %v", server.n_p, server.n_a, server.v_a))

	var response AcceptResponse
	response.CoreMessage = base.MakeCoreMessage(args.To(), args.From())
	response.SessionId = args.SessionId

	if (args.N >= server.n_p) {
		DB.Printf(2, fmt.Sprintf("V: %v, N: %v", args.V, args.N))

		newServer.n_p = args.N
		newServer.n_a = args.N
		newServer.v_a = args.V

		response.Ok = true
		response.V = args.V

		DB.Printf(3, fmt.Sprintf("V: %v, N: %v", args.V, args.N))

	} else {
		DB.Printf(4, fmt.Sprintf("V: %v, N: %v", args.V, args.N))
		response.Ok = false
	}

	response.N_p = server.n_p
	newServer.SetSingleResponse(&response)
	return newServer
}

func (server *Server) handleDecideRequest(args *DecideRequest) *Server {
	px := server.Attribute().(ServerAttribute)
	DB := base.MakeDebugger("handleDecideRequest", px.me)
	DB.Printf(1, fmt.Sprintf("v val: %v ", args.V))
	newServer := server.copy()
	newServer.agreedValue = args.V
	newServer.v_a = args.V

	// response := DecideResponse{
	// 	base.MakeCoreMessage()
	// }

	// newServer.SetSingleResponse(&response)
	return newServer
}
