package paxos

import (
	"coms4113/hw5/pkg/base"
	"fmt"
)

// Fill in the function to lead the program to a state where A2 rejects the Accept Request of P1
func ToA2RejectP1() []func(s *base.State) bool {
	// S1 phase needs to be Accept
	// there is a msg from S2 to S1 of the type AcceptResponse
	// s.GetNode("s1").prop


	p1PreparePhase := func(s *base.State) bool {
		fmt.Println("check for p1 prepartion")
		s1 := s.GetNode("s1").(*Server)
		valid := s1.proposer.Phase == Propose
		if valid {
			fmt.Println("... p1 entered Propose phase")
		}
		return valid
	}

	p1ProposalAccepted := func(s *base.State) bool {
		s1 := s.GetNode("s1").(*Server)
		otherok := 0
		valid := false
		for _, node := range s.Nodes() {
			s := node.(*Server)
			if s1.proposer.N > s.n_p {
				otherok++
			}
		}
		if otherok > 0 {
			valid = true
		}

		if valid {
			fmt.Println("... p1 proposal accepted")
		}
		return valid
	}

	p1AcceptPhase := func(s *base.State) bool {
		s1 := s.GetNode("s1").(*Server)
		valid := false
		if s1.proposer.Phase == Accept {
			valid = true
		}
		if valid {
			fmt.Println("... p1 entered Accept phase")
		}
		return valid
	}


	a2RejectP1 := func(s *base.State) bool {
		s1 := s.GetNode("s1").(*Server)
		valid := false

		if s1.proposer.Phase == Accept && s1.proposer.V == "v1" && s1.proposer.Responses[1] == false {
			valid = true
		}
		if valid {
			fmt.Println("... after a2 rejects p1 in accept phase")
		}
		return valid
	}



	return []func(s *base.State) bool {
		p1PreparePhase,
		p1ProposalAccepted,
		p1AcceptPhase,
		a2RejectP1,
	}
}



// Fill in the function to lead the program to a state where a consensus is reached in Server 3.
func ToConsensusCase5() []func(s *base.State) bool {

	p3PreparePhase := func(s *base.State) bool {
		s3 := s.GetNode("s3").(*Server)
		valid := false
		if (s3.proposer.Phase == Propose) {
			for _, m := range s.Network {
				resp, ok := m.(*ProposeResponse)
				if ok && resp.Ok && m.From() == "s2" && m.To() == "s3" {
					valid = true
				}
			}
		}
		if valid {
			fmt.Println("... p2 entered Propose phase accepted by p1")
		}
		return valid
	}

	p3AcceptPhase := func(s *base.State) bool {
		s3 := s.GetNode("s3").(*Server)
		valid := s3.proposer.Phase == Accept && s3.proposer.V == "v3"
		if valid {
			fmt.Println("... p3 entered Accept phase")
		}
		return valid
	}

	s3DecidePhase := func(s *base.State) bool {
		s3 := s.GetNode("s3").(*Server)
		valid := false
		if s3.proposer.Phase == Decide && s3.proposer.V == "v3" {
			valid = true
		}
		if valid {
			fmt.Println("... s2 entered decide phase")
		}
		return valid
	}
	s3KnowConsensus := func(s *base.State) bool {
		valid := true
		for addr, node := range s.Nodes() {
			server := node.(*Server)
			if addr == "s3" && server.agreedValue != "v3" {
				valid = false
			}

			if addr != "s3" && server.agreedValue != nil {
				valid = false
			}
		}
		if valid {
			fmt.Printf("... s3 know the consensus first")
		}
		return valid
	}

	return []func(s *base.State) bool {
		p3PreparePhase,
		p3AcceptPhase,
		s3DecidePhase,
		s3KnowConsensus,
	}
}

// Fill in the function to lead the program to a state where all the Accept Requests of P1 are rejected
func NotTerminate1() []func(s *base.State) bool {

	p1PreparePhase := func(s *base.State) bool {
		s1 := s.GetNode("s1").(*Server)
		valid := false
		if (s1.proposer.Phase == Propose) {
			valid = true
		}
		if valid {
			fmt.Println("... p1 entered Propose phase")
		}
		return valid
	}

	p1PreparePhaseOk := func(s *base.State) bool {
		s1 := s.GetNode("s1").(*Server)
		valid := false
		validByServer := []bool {false, false, false}
		if (s1.proposer.Phase == Propose) {
			valid = true
			for _, m := range s.Network {
				resp, ok := m.(*ProposeResponse)
				if ok && resp.Ok && m.To() == "s1" {
					validByServer[0] = true
				}
				if ok && resp.Ok && m.To() == "s2" {
					validByServer[1] = true
				}
				if ok && resp.Ok && m.To() == "s3" {
					validByServer[2] = true
				}
			}
		}
		valid = valid || validByServer[0] || validByServer[1] || validByServer[2]
		if valid {
			fmt.Println("... p1 Propose phase ok")
		}
		return valid
	}

	p1AcceptPhase := func(s *base.State) bool {
		s1 := s.GetNode("s1").(*Server)
		valid := s1.proposer.Phase == Accept
		if valid {
			fmt.Println("... p3 entered Accept phase")
		}
		return valid
	}

	p3PreparePhase := func(s *base.State) bool {
		s3 := s.GetNode("s3").(*Server)
		s1 := s.GetNode("s1").(*Server)
		valid := false

		if (s3.proposer.Phase == Propose && s3.proposer.N > s1.proposer.N && s1.proposer.Phase == Accept && s1.proposer.ResponseCount == 0) {
			for _, resp := range s1.proposer.Responses {
				valid = valid || resp == false
			}
		}
		if valid {
			fmt.Println("... p3 entered Propose phase")
		}
		return valid
	}

	p1AcceptP3 := func(s *base.State) bool {
		s3 := s.GetNode("s3").(*Server)
		s1 := s.GetNode("s1").(*Server)
		valid := false

		if (s3.proposer.Phase == Propose && s3.proposer.N > s1.proposer.N && s1.proposer.Phase == Accept && s1.proposer.ResponseCount == 0 && s3.proposer.Responses[0] == true) {
			for _, resp := range s1.proposer.Responses {
				valid = valid || resp == false
			}
		}
		if valid {
			fmt.Println("... s1 accept p3")
		}
		return valid
	}

	p2AcceptP3 := func(s *base.State) bool {
		s3 := s.GetNode("s3").(*Server)
		s1 := s.GetNode("s1").(*Server)
		valid := false

		if (s3.proposer.Phase == Propose && s3.proposer.N > s1.proposer.N && s1.proposer.Phase == Accept && s1.proposer.ResponseCount == 0 && s3.proposer.Responses[1] == true) {
			for _, resp := range s1.proposer.Responses {
				valid = valid || resp == false
			}
		}
		if valid {
			fmt.Println("... s2 accept p3")
		}
		return valid
	}

	p3AcceptP3 := func(s *base.State) bool {
		s3 := s.GetNode("s3").(*Server)
		s1 := s.GetNode("s1").(*Server)
		valid := false

		if (s3.proposer.Phase == Propose && s3.proposer.N > s1.proposer.N && s1.proposer.Phase == Accept && s1.proposer.ResponseCount == 0 && s3.proposer.Responses[2] == true) {
			for _, resp := range s1.proposer.Responses {
				valid = valid || resp == false
			}
		}
		if valid {
			fmt.Println("... s3 accept p3")
		}
		return valid
	}

	p3AcceptPhase := func(s *base.State) bool {
		s3 := s.GetNode("s3").(*Server)
		s1 := s.GetNode("s1").(*Server)
		valid := false
		if (s3.proposer.Phase == Accept && s1.proposer.Phase == Accept) {
			valid = true
			for _, resp := range s3.proposer.Responses {
				valid = valid || resp == false
			}
		}
		if valid {
			fmt.Println("... p3 entered Accept phase")
		}
		return valid
	}

	p3RejectP1 := func(s *base.State) bool {
		s1 := s.GetNode("s1").(*Server)

		valid := false
		if (s1.proposer.Phase == Accept) {
			for _, m := range s.Network {
				resp, ok := m.(*AcceptResponse)
				if ok && !resp.Ok && resp.From() == "s3" && resp.To() == "s1" {
					valid = true
				}
			}
		}
		if valid {
			fmt.Println("... p3 rejects p1")
		}
		return valid
	}

	p2RejectP1 := func(s *base.State) bool {
		s1 := s.GetNode("s1").(*Server)

		valid := false
		if (s1.proposer.Phase == Accept) {
			for _, m := range s.Network {
				resp, ok := m.(*AcceptResponse)
				if ok && !resp.Ok && resp.From() == "s2" && resp.To() == "s1" {
					valid = true
				}
			}
		}
		if valid {
			fmt.Println("... p2 rejects p1")
		}
		return valid
	}

	p1RejectP1 := func(s *base.State) bool {
		s1 := s.GetNode("s1").(*Server)

		valid := false
		if (s1.proposer.Phase == Accept) {
			for _, m := range s.Network {
				resp, ok := m.(*AcceptResponse)
				if ok && !resp.Ok && resp.From() == "s1" && resp.To() == "s1" {
					valid = true
				}
			}
		}
		if valid {
			fmt.Println("... p1 rejects p1")
		}
		return valid
	}

	return []func(s *base.State) bool {
		p1PreparePhase,
		p1PreparePhaseOk,
		p1AcceptPhase,
		p3PreparePhase,
		p1AcceptP3,
		p2AcceptP3,
		p3AcceptP3,
		p3AcceptPhase,
		p3RejectP1,
		p2RejectP1,
		p1RejectP1,
	}
}

// Fill in the function to lead the program to a state where all the Accept Requests of P3 are rejected
func NotTerminate2() []func(s *base.State) bool {

	p1PreparePhase := func(s *base.State) bool {
		s1 := s.GetNode("s1").(*Server)
		s3 := s.GetNode("s3").(*Server)
		valid := false

		if (s1.proposer.Phase == Propose && s1.proposer.N > s3.proposer.N && s3.proposer.Phase == Accept && s3.proposer.ResponseCount == 0) {
			for _, resp := range s3.proposer.Responses {
				valid = valid || resp == false
			}
		}
		if valid {
			fmt.Println("... p1 entered Propose phase")
		}
		return valid
	}

	p1AcceptPhase := func(s *base.State) bool {
		s1 := s.GetNode("s1").(*Server)
		s3 := s.GetNode("s3").(*Server)
		valid := s1.proposer.Phase == Accept && s3.proposer.ResponseCount == 0
		if valid {
			fmt.Println("... p1 entered accept phase ")
		}
		return valid
	}

	p1RejectP3 := func(s *base.State) bool {
		s3 := s.GetNode("s3").(*Server)

		valid := false
		if (s3.proposer.Phase == Accept) {
			for _, m := range s.Network {
				resp, ok := m.(*AcceptResponse)
				if ok && !resp.Ok && resp.From() == "s1" && resp.To() == "s3" {
					valid = true
				}
			}
		}
		if valid {
			fmt.Println("... p1 rejects p3")
		}
		return valid
	}

	p2RejectP3 := func(s *base.State) bool {
		s3 := s.GetNode("s3").(*Server)

		valid := false
		if (s3.proposer.Phase == Accept) {
			for _, m := range s.Network {
				resp, ok := m.(*AcceptResponse)
				if ok && !resp.Ok && resp.From() == "s2" && resp.To() == "s3" {
					valid = true
				}
			}
		}
		if valid {
			fmt.Println("... p2 rejects p3")
		}
		return valid
	}

	p3RejectP3 := func(s *base.State) bool {
		s3 := s.GetNode("s3").(*Server)

		valid := false
		if (s3.proposer.Phase == Accept) {
			for _, m := range s.Network {
				resp, ok := m.(*AcceptResponse)
				if ok && !resp.Ok && resp.From() == "s3" && resp.To() == "s3" {
					valid = true
				}
			}
		}
		if valid {
			fmt.Println("... p3 rejects p3")
		}
		return valid
	}

	return []func(s *base.State) bool {
		p1PreparePhase,
		p1AcceptPhase,
		p1RejectP3,
		p2RejectP3,
		p3RejectP3,
	}
	// panic("fill me in")
}

// Fill in the function to lead the program to a state where all the Accept Requests of P1 are rejected again.
func NotTerminate3() []func(s *base.State) bool {
	p3PreparePhase := func(s *base.State) bool {
		s3 := s.GetNode("s3").(*Server)
		s1 := s.GetNode("s1").(*Server)
		valid := false

		if (s3.proposer.Phase == Propose && s3.proposer.N > s1.proposer.N && s1.proposer.Phase == Accept && s1.proposer.ResponseCount == 0) {
			for _, resp := range s1.proposer.Responses {
				valid = valid || resp == false
			}
		}
		if valid {
			fmt.Println("... p3 entered Propose phase")
		}
		return valid
	}

	p1AcceptP3 := func(s *base.State) bool {
		s3 := s.GetNode("s3").(*Server)
		s1 := s.GetNode("s1").(*Server)
		valid := false

		if (s3.proposer.Phase == Propose && s3.proposer.N > s1.proposer.N && s1.proposer.Phase == Accept && s1.proposer.ResponseCount == 0 && s3.proposer.Responses[0] == true) {
			for _, resp := range s1.proposer.Responses {
				valid = valid || resp == false
			}
		}
		if valid {
			fmt.Println("... s1 accept p3")
		}
		return valid
	}

	p2AcceptP3 := func(s *base.State) bool {
		s3 := s.GetNode("s3").(*Server)
		s1 := s.GetNode("s1").(*Server)
		valid := false

		if (s3.proposer.Phase == Propose && s3.proposer.N > s1.proposer.N && s1.proposer.Phase == Accept && s1.proposer.ResponseCount == 0 && s3.proposer.Responses[1] == true) {
			for _, resp := range s1.proposer.Responses {
				valid = valid || resp == false
			}
		}
		if valid {
			fmt.Println("... s2 accept p3")
		}
		return valid
	}

	p3AcceptP3 := func(s *base.State) bool {
		s3 := s.GetNode("s3").(*Server)
		s1 := s.GetNode("s1").(*Server)
		valid := false

		if (s3.proposer.Phase == Propose && s3.proposer.N > s1.proposer.N && s1.proposer.Phase == Accept && s1.proposer.ResponseCount == 0 && s3.proposer.Responses[2] == true) {
			for _, resp := range s1.proposer.Responses {
				valid = valid || resp == false
			}
		}
		if valid {
			fmt.Println("... s3 accept p3")
		}
		return valid
	}

	p3AcceptPhase := func(s *base.State) bool {
		s3 := s.GetNode("s3").(*Server)
		s1 := s.GetNode("s1").(*Server)
		valid := false
		if (s3.proposer.Phase == Accept && s1.proposer.Phase == Accept) {
			valid = true
			for _, resp := range s3.proposer.Responses {
				valid = valid || resp == false
			}
		}
		if valid {
			fmt.Println("... p3 entered Accept phase")
		}
		return valid
	}

	p3RejectP1 := func(s *base.State) bool {
		s1 := s.GetNode("s1").(*Server)

		valid := false
		if (s1.proposer.Phase == Accept) {
			for _, m := range s.Network {
				resp, ok := m.(*AcceptResponse)
				if ok && !resp.Ok && resp.From() == "s3" && resp.To() == "s1" {
					valid = true
				}
			}
		}
		if valid {
			fmt.Println("... p3 rejects p1")
		}
		return valid
	}

	p2RejectP1 := func(s *base.State) bool {
		s1 := s.GetNode("s1").(*Server)

		valid := false
		if (s1.proposer.Phase == Accept) {
			for _, m := range s.Network {
				resp, ok := m.(*AcceptResponse)
				if ok && !resp.Ok && resp.From() == "s2" && resp.To() == "s1" {
					valid = true
				}
			}
		}
		if valid {
			fmt.Println("... p2 rejects p1")
		}
		return valid
	}

	p1RejectP1 := func(s *base.State) bool {
		s1 := s.GetNode("s1").(*Server)

		valid := false
		if (s1.proposer.Phase == Accept) {
			for _, m := range s.Network {
				resp, ok := m.(*AcceptResponse)
				if ok && !resp.Ok && resp.From() == "s1" && resp.To() == "s1" {
					valid = true
				}
			}
		}
		if valid {
			fmt.Println("... p1 rejects p1")
		}
		return valid
	}

	return []func(s *base.State) bool {
		p3PreparePhase,
		p1AcceptP3,
		p2AcceptP3,
		p3AcceptP3,
		p3AcceptPhase,
		p3RejectP1,
		p2RejectP1,
		p1RejectP1,
	}
}

// Fill in the function to lead the program to make P1 propose first, then P3 proposes, but P1 get rejects in
// Accept phase
func concurrentProposer1() []func(s *base.State) bool {
	return NotTerminate1()
}

// Fill in the function to lead the program continue  P3's proposal  and reaches consensus at the value of "v3".
func concurrentProposer2() []func(s *base.State) bool {

	p1AcceptP3 := func(s *base.State) bool {
		s3 := s.GetNode("s3").(*Server)
		valid := false

		if (s3.proposer.Phase == Accept && s3.proposer.Responses[0] == true) {
			valid = true
		}
		if valid {
			fmt.Println("... s1 accept p3")
		}
		return valid
	}

	p2AcceptP3 := func(s *base.State) bool {
		s3 := s.GetNode("s3").(*Server)
		valid := false

		if (s3.proposer.Phase == Accept && s3.proposer.Responses[1] == true) {
			valid = true
		}
		if valid {
			fmt.Println("... s2 accept p3")
		}
		return valid
	}

	p3AcceptP3 := func(s *base.State) bool {
		s3 := s.GetNode("s3").(*Server)
		valid := false

		if (s3.proposer.Phase == Decide && s3.proposer.Responses[2] == true) {
			valid = true
		}
		if valid {
			fmt.Println("... s3 accept p3")
		}
		return valid
	}


	return []func(s *base.State) bool {
		p1AcceptP3,
		p2AcceptP3,
		p3AcceptP3,
		// p3DecidePhase,
	}
}
