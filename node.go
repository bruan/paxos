package main

// Node 节点
type Node struct {
	nodeID         int
	network        *NodeNetwork
	instanceGroups map[int]*InstanceGroup
}

func newNode(nodeID int, listenAddr string, nodeAddrs map[int]string) *Node {
	network := NewNodeNetwork(nodeID, listenAddr, nodeAddrs)
	if network == nil {
		return nil
	}

	node := &Node{nodeID: nodeID, network: network}
	node.instanceGroups = make(map[int]*InstanceGroup)

	return node
}

func (node *Node) getNodeID() int {
	return node.nodeID
}

func (node *Node) getNodeCount() int {
	return len(node.network.nodeAddrs)
}

func (node *Node) getInstanceGroup(instanceGroupID int) *InstanceGroup {
	return node.instanceGroups[instanceGroupID]
}

func (node *Node) newInstanceGroup(instanceGroupID int, sm statemachine) *InstanceGroup {
	instanceGroup := newInstanceGroup(node, instanceGroupID, sm)
	node.instanceGroups[instanceGroupID] = instanceGroup

	return instanceGroup
}
