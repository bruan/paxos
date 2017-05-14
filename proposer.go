package main

import (
	"errors"
	"log"
	"sync"
	"time"
)

const (
	proposerNone int = iota
	proposerPrepareing
	proposerAccepting
	proposerClosen
)

type proposerInstance struct {
	instanceID     int
	state          int
	proposalBallot int
	acceptValue    string
	acceptBallot   int
	counter        counter
}

type proposer struct {
	sequence            int
	multiProposalBallot int
	commitValue         string
	hasNewCommitValue   bool
	commitValueLock     sync.Mutex // commit协程跟instance协程保护锁
	waitCommitLock      sync.Mutex // 几个submit协程保护锁
	resultChan          chan string
	instanceGroup       *InstanceGroup
	workInstanceID      int
	instances           map[int]*proposerInstance
}

func newProposer(instanceGroup *InstanceGroup) *proposer {
	p := &proposer{sequence: 0, instanceGroup: instanceGroup}
	p.resultChan = make(chan string)
	p.instances = make(map[int]*proposerInstance)

	return p
}

func (p *proposer) commit(val string) (string, error) {
	p.waitCommitLock.Lock()
	defer p.waitCommitLock.Unlock()

	p.commitValueLock.Lock()
	p.commitValue = val
	p.hasNewCommitValue = true
	p.commitValueLock.Unlock()

	var result string
	select {
	case result = <-p.resultChan:
	case <-time.After(time.Second * 500000):
		return "", errors.New("result chan timeout")
	}

	return result, nil
}

func (p *proposer) update(init bool) {
	p.commitValueLock.Lock()
	if init && !p.hasNewCommitValue {
		p.commitValueLock.Unlock()
		return
	}
	p.hasNewCommitValue = false
	p.commitValueLock.Unlock()

	instanceID := p.instanceGroup.getNextInstanceID()
	inst := &proposerInstance{instanceID: instanceID, state: proposerNone, proposalBallot: 1, acceptBallot: 0, acceptValue: ""}
	inst.counter.nodeCount = p.instanceGroup.getNodeCount()
	p.instances[instanceID] = inst
	p.workInstanceID = instanceID

	if p.multiProposalBallot != 0 {
		inst.proposalBallot = p.multiProposalBallot
		p.commitValueLock.Lock()
		inst.acceptValue = p.commitValue
		p.commitValueLock.Unlock()
		p.accept(inst)
	} else {
		p.prepare(inst)
	}
}

func (p *proposer) prepare(inst *proposerInstance) {
	maxRejectN := inst.counter.getMaxRejectN()
	inst.counter.startNewRound()
	inst.acceptBallot = 0

	inst.proposalBallot = p.genProposalID(maxRejectN)
	inst.state = proposerPrepareing

	m := message{typ: Prepare, from: p.instanceGroup.getNodeID(), instanceID: inst.instanceID, proposalBallot: inst.proposalBallot}
	p.instanceGroup.broadcast(m, true)

	p.instanceGroup.tm.delTimer(AcceptedTimeout)
	p.instanceGroup.tm.addTimer(PromisedTimeout, time.Millisecond*20000000, func(int) {
		log.Printf("proposer: %d promise timeout instanceID(%d)", p.instanceGroup.getNodeID(), inst.instanceID)
		p.prepare(inst)
	})

	log.Printf("proposer: %d start prepare instanceID(%d) proposalID(%d)", p.instanceGroup.getNodeID(), inst.instanceID, inst.proposalBallot)
}

func (p *proposer) onPromised(m message) {
	inst := p.instances[m.instanceID]
	if inst == nil {
		return
	}

	if inst.state != proposerPrepareing {
		return
	}

	if inst.proposalBallot != m.proposalBallot {
		log.Printf("proposer: %d received a error promise instanceID(%d) proposalID(%d) igoned it", p.instanceGroup.getNodeID(), inst.instanceID, inst.proposalBallot)
		return
	}

	if m.rejectBallot == 0 {
		inst.counter.addPass(m.from)

		log.Printf("proposer: %d received a new promise from(%d) instanceID(%d) proposalID(%d) acceptBallot(%d) acceptValue(%s)", p.instanceGroup.getNodeID(), m.from, m.instanceID, m.proposalBallot, m.acceptBallot, m.acceptValue)

		// 找到最大acceptBallot的值
		if m.acceptBallot > inst.acceptBallot {
			inst.acceptBallot = m.acceptBallot
			inst.acceptValue = m.acceptValue
		}
	} else {
		log.Printf("proposer: %d received a reject promise from(%d) instanceID(%d) proposalID(%d) rejectBallot(%d) acceptBallot(%d) acceptV(%s)", p.instanceGroup.getNodeID(), m.from, m.instanceID, m.proposalBallot, m.rejectBallot, m.acceptBallot, m.acceptValue)
		inst.counter.addReject(m.from, m.rejectBallot)
	}

	if inst.counter.isPassedOnThisRound() {
		// 如果prepare阶段对应的instanceID没有冲突，就试着提交自己的value
		if inst.acceptBallot == 0 {
			p.commitValueLock.Lock()
			inst.acceptValue = p.commitValue
			p.commitValueLock.Unlock()
		}
		p.accept(inst)
	} else if inst.counter.isRejectedOnThisRound() || inst.counter.isAllReceiveOnThisRound() {
		p.prepare(inst)
	}
}

func (p *proposer) accept(inst *proposerInstance) {
	inst.counter.startNewRound()

	m := message{typ: Propose, from: p.instanceGroup.getNodeID(), instanceID: inst.instanceID, proposalBallot: inst.proposalBallot, acceptValue: inst.acceptValue}
	p.instanceGroup.broadcast(m, true)

	inst.state = proposerAccepting

	p.instanceGroup.tm.delTimer(PromisedTimeout)
	p.instanceGroup.tm.addTimer(AcceptedTimeout, time.Millisecond*20000000, func(int) {
		log.Printf("proposer: %d accept timeout instanceID(%d)", p.instanceGroup.getNodeID(), inst.instanceID)
		p.prepare(inst)
	})

	log.Printf("proposer: %d start accept instanceID(%d) proposalID(%d) acceptBallot(%d) acceptValue(%s)", p.instanceGroup.getNodeID(), inst.instanceID, inst.proposalBallot, inst.acceptBallot, inst.acceptValue)
}

func (p *proposer) onAccepted(msg message) {
	inst := p.instances[msg.instanceID]
	if inst == nil {
		return
	}

	if inst.state != proposerAccepting {
		return
	}

	if inst.proposalBallot != msg.proposalBallot {
		log.Printf("proposer: %d received a error instanceID(%d) proposalID(%d) igoned it", p.instanceGroup.getNodeID(), msg.instanceID, msg.proposalBallot)
		return
	}

	if msg.rejectBallot == 0 {
		inst.counter.addPass(msg.from)
		log.Printf("proposer: %d received a new accept from(%d) instanceID(%d) proposalID(%d) acceptBallot(%d) acceptValue(%s)", p.instanceGroup.getNodeID(), msg.from, msg.instanceID, msg.proposalBallot, msg.acceptBallot, msg.acceptValue)
	} else {
		inst.counter.addReject(msg.from, msg.rejectBallot)
		log.Printf("proposer: %d received a reject accept from(%d) instanceID(%d) proposalID(%d) rejectBallot(%d) acceptBallot(%d) acceptValue(%s)", p.instanceGroup.getNodeID(), msg.from, msg.instanceID, msg.proposalBallot, msg.rejectBallot, msg.acceptBallot, msg.acceptValue)
	}

	if inst.counter.isPassedOnThisRound() {
		inst.state = proposerClosen
		p.instanceGroup.tm.delTimer(AcceptedTimeout)
		ret := p.instanceGroup.learner.onValueClosed(inst.instanceID, inst.acceptValue)

		log.Printf("proposer: %d closen value instanceID(%d) acceptBallot(%d) acceptValue(%s)\n", p.instanceGroup.getNodeID(), inst.instanceID, inst.acceptBallot, inst.acceptValue)
		if inst.acceptBallot == 0 {
			p.commitValueLock.Lock()
			p.resultChan <- ret
			p.commitValueLock.Unlock()
			p.multiProposalBallot = inst.proposalBallot
		} else {
			p.multiProposalBallot = 0
			p.update(false)
		}

	} else if inst.counter.isRejectedOnThisRound() || inst.counter.isAllReceiveOnThisRound() {
		p.prepare(inst)
	}
}

func (p *proposer) genProposalID(maxRejectN int) int {
	sequence := maxRejectN >> 16
	if sequence < p.sequence {
		sequence = p.sequence
	}
	sequence++
	p.sequence = sequence
	return p.sequence<<16 | p.instanceGroup.getNodeID()
}
