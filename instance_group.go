package main

import (
	"log"
	"time"
)

type InstanceGroup struct {
	instanceGroupID int
	node            *Node
	tm              *timerMgr
	nextInstanceID  int
	learner         *learner
	acceptor        *acceptor
	proposer        *proposer
}

func newInstanceGroup(node *Node, instanceGroupID int, sm statemachine) *InstanceGroup {
	instanceGroup := &InstanceGroup{node: node, instanceGroupID: instanceGroupID, nextInstanceID: 1}
	instanceGroup.tm = newTimerMgr()
	instanceGroup.acceptor = newAcceptor(instanceGroup)
	instanceGroup.proposer = newProposer(instanceGroup)
	instanceGroup.learner = newLearner(instanceGroup, sm)

	go instanceGroup.run()

	return instanceGroup
}

func (instanceGroup *InstanceGroup) commit(val string) (string, error) {
	return instanceGroup.proposer.commit(val)
}

func (instanceGroup *InstanceGroup) getNodeID() int {
	return instanceGroup.node.getNodeID()
}

func (instanceGroup *InstanceGroup) getNodeCount() int {
	return instanceGroup.node.getNodeCount()
}

func (instanceGroup *InstanceGroup) getNextInstanceID() int {
	return instanceGroup.nextInstanceID
}

func (instanceGroup *InstanceGroup) updateNextInstanceID() {
	instanceGroup.nextInstanceID++
}

func (instanceGroup *InstanceGroup) send(id int, m message) {
	instanceGroup.node.network.send(id, m)
}

func (instanceGroup *InstanceGroup) response(id int, m message) {
	instanceGroup.node.network.response(id, m)
}

func (instanceGroup *InstanceGroup) broadcast(m message, self bool) {
	for k := range instanceGroup.node.network.nodeConns1 {
		if !self && k == instanceGroup.node.getNodeID() {
			continue
		}

		instanceGroup.node.network.send(k, m)
	}
}

func (instanceGroup *InstanceGroup) run() {
	for {
		m, ok := instanceGroup.node.network.recv(time.Millisecond * 10)

		if ok {
			switch m.typ {
			case Prepare:
				instanceGroup.acceptor.onPrepare(m)
			case Propose:
				instanceGroup.acceptor.onAccept(m)
			case Promised:
				instanceGroup.proposer.onPromised(m)
			case Accepted:
				instanceGroup.proposer.onAccepted(m)
			case PushLearn:
				instanceGroup.learner.leanValue(m)
			case PullLearnRequest:
				instanceGroup.learner.onPullLearnRequest(m)
			case PullLearnResponse:
				instanceGroup.learner.onPullLearnResponse(m)

			default:
				log.Printf("node: %d unexpected message type: %d\n", instanceGroup.node.getNodeID(), m.typ)
			}
		}

		instanceGroup.tm.update()
		instanceGroup.proposer.update(true)

		select {
		case <-time.After(time.Microsecond):
		}
	}
}
