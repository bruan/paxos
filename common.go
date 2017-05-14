package main

const (
	Connect int = iota + 1
	Prepare
	Propose
	Promised
	Accepted
	InstanceID
	PushLearn
	PullLearnRequest
	PullLearnResponse
	Closed
)

const (
	PromisedTimeout int = iota + 1
	AcceptedTimeout
	PullLearnTimeout
)

type message struct {
	typ            int
	from           int
	instanceID     int
	proposalBallot int
	rejectBallot   int
	acceptBallot   int
	acceptValue    string
}
