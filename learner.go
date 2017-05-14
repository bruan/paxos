package main

import (
	"log"
	"time"
)

type learnerInstance struct {
	instanceID  int
	acceptValue string
}

type learner struct {
	sm            statemachine
	instances     map[int]*learnerInstance
	instanceGroup *InstanceGroup
}

func newLearner(instanceGroup *InstanceGroup, sm statemachine) *learner {
	l := learner{instanceGroup: instanceGroup, sm: sm}
	l.instances = make(map[int]*learnerInstance)
	l.instanceGroup.tm.addTimer(PullLearnTimeout, time.Millisecond*200, l.checkLearn)

	return &l
}

func (l *learner) checkLearn(int) {
	m := message{typ: PullLearnRequest, from: l.instanceGroup.getNodeID(), instanceID: l.instanceGroup.getNextInstanceID()}
	l.instanceGroup.broadcast(m, false)
	l.instanceGroup.tm.addTimer(PullLearnTimeout, time.Millisecond*200, l.checkLearn)
}

func (l *learner) onValueClosed(instanceID int, value string) string {
	// 如果这个时候该节点崩溃了，此时集群中中的值是不被确定的（closed），等到下一次发起commit时，那一轮会最终确定这个值。
	m := message{typ: PushLearn, from: l.instanceGroup.getNodeID(), instanceID: instanceID, acceptValue: value}
	l.instanceGroup.broadcast(m, false)

	return l.leanValue(m)
}

func (l *learner) leanValue(m message) string {
	if l.instanceGroup.getNextInstanceID() != m.instanceID {
		return ""
	}

	l.instanceGroup.updateNextInstanceID()
	l.instances[m.instanceID] = &learnerInstance{instanceID: m.instanceID, acceptValue: m.acceptValue}

	ret := l.sm.exec(m.acceptValue)
	log.Printf("leaner: %d learn instanceID(%d) lean value(%s)", l.instanceGroup.getNodeID(), m.instanceID, m.acceptValue)

	return ret
}

func (l *learner) onPullLearnRequest(msg message) {
	if l.instanceGroup.getNextInstanceID() <= msg.instanceID {
		return
	}

	inst := l.instances[msg.instanceID]
	if inst == nil {
		return
	}

	m := message{typ: PullLearnResponse, from: l.instanceGroup.getNodeID(), instanceID: inst.instanceID, acceptValue: inst.acceptValue}
	l.instanceGroup.send(msg.from, m)
}

func (l *learner) onPullLearnResponse(m message) {
	l.leanValue(m)
}
