package main

import "log"

type acceptorInstance struct {
	instanceID     int
	promisedBallot int
	acceptValue    string
	acceptBallot   int
}

type acceptor struct {
	instances     map[int]*acceptorInstance
	instanceGroup *InstanceGroup
}

func newAcceptor(instanceGroup *InstanceGroup) *acceptor {
	a := &acceptor{instanceGroup: instanceGroup}
	a.instances = make(map[int]*acceptorInstance)

	return a
}

func (a *acceptor) onPrepare(msg message) {
	inst := a.instances[msg.instanceID]
	if inst == nil {
		inst = &acceptorInstance{}
		inst.instanceID = msg.instanceID
		a.instances[inst.instanceID] = inst
	}

	var m message
	m.typ = Promised
	m.instanceID = msg.instanceID
	m.from = a.instanceGroup.getNodeID()
	m.proposalBallot = msg.proposalBallot
	m.acceptBallot = inst.acceptBallot
	m.acceptValue = inst.acceptValue
	if msg.proposalBallot > inst.promisedBallot {
		log.Printf("acceptor: %d pass prepare from(%d) instanceID(%d) proposalID(%d) promisedBallot(%d) acceptBallot(%d)", a.instanceGroup.getNodeID(), msg.from, msg.instanceID, msg.proposalBallot, inst.promisedBallot, inst.acceptBallot)
		inst.promisedBallot = msg.proposalBallot
	} else {
		log.Printf("acceptor: %d reject prepare from(%d) instanceID(%d) proposalID(%d) promisedBallot(%d) acceptBallot(%d)", a.instanceGroup.getNodeID(), msg.from, msg.instanceID, msg.proposalBallot, inst.promisedBallot, inst.acceptBallot)
		m.rejectBallot = inst.promisedBallot
	}

	a.instanceGroup.response(msg.from, m)
}

func (a *acceptor) onAccept(msg message) {
	inst := a.instances[msg.instanceID]
	if inst == nil {
		return
	}

	var m message
	m.typ = Accepted
	m.instanceID = msg.instanceID
	m.from = a.instanceGroup.getNodeID()
	m.proposalBallot = msg.proposalBallot

	if msg.proposalBallot >= inst.promisedBallot {
		log.Printf("acceptor: %d pass accept from(%d) instanceID(%d) proposalID(%d) promisedBallot(%d) acceptBallot(%d) oldValue(%s) newValue(%s)", a.instanceGroup.getNodeID(), msg.from, msg.instanceID, msg.proposalBallot, inst.promisedBallot, inst.acceptBallot, inst.acceptValue, msg.acceptValue)
		inst.acceptValue = msg.acceptValue
		inst.acceptBallot = msg.proposalBallot
		inst.promisedBallot = msg.proposalBallot

		m.acceptBallot = inst.acceptBallot
		m.acceptValue = inst.acceptValue

		if a.instances[inst.instanceID+1] == nil {
			// multi-paxos 中的优化，省去了连续成功后的prepare阶段
			newInst := &acceptorInstance{}
			newInst.instanceID = msg.instanceID + 1
			newInst.promisedBallot = msg.proposalBallot
			a.instances[newInst.instanceID] = newInst
		}

	} else {
		log.Printf("acceptor: %d reject accept from(%d) instanceID(%d) proposalID(%d) promisedBallot(%d) acceptBallot(%d) oldValue(%s) newValue(%s)", a.instanceGroup.getNodeID(), msg.from, msg.instanceID, msg.proposalBallot, inst.promisedBallot, inst.acceptBallot, inst.acceptValue, msg.acceptValue)
		m.rejectBallot = inst.promisedBallot
	}

	a.instanceGroup.response(msg.from, m)
}
