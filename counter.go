package main

type counter struct {
	nodeCount int
	passes    map[int]int
	rejects   map[int]int
}

func (c *counter) addReject(id int, rejectN int) {
	c.rejects[id] = rejectN
}

func (c *counter) addPass(id int) {
	c.passes[id] = 1
}

func (c *counter) getMaxRejectN() int {
	var rejectN int
	for _, v := range c.rejects {
		if rejectN < v {
			rejectN = v
		}
	}

	return rejectN
}

func (c *counter) isPassedOnThisRound() bool {
	return len(c.passes) >= c.nodeCount/2+1
}

func (c *counter) isRejectedOnThisRound() bool {
	return len(c.rejects) >= c.nodeCount/2+1
}

func (c *counter) isAllReceiveOnThisRound() bool {
	return len(c.passes)+len(c.rejects) == c.nodeCount
}

func (c *counter) startNewRound() {
	c.passes = make(map[int]int)
	c.rejects = make(map[int]int)
}
