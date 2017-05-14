package main

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"net"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"
)

type NodeNetwork struct {
	nodeID     int
	listenAddr string
	nodeAddrs  map[int]string
	recvQueue  chan message
	nodeConns1 map[int]*NodeConn // 主动发起的连接
	nodeConns2 map[int]*NodeConn // 被动接受的连接
}

func NewNodeNetwork(nodeID int, listenAddr string, nodeAddrs map[int]string) *NodeNetwork {
	network := NodeNetwork{nodeID: nodeID, listenAddr: listenAddr, nodeAddrs: nodeAddrs}

	listen, err := net.Listen("tcp", listenAddr)
	if err != nil {
		log.Printf("error listening: %s", err)
		return nil
	}

	network.recvQueue = make(chan message)
	network.nodeConns1 = make(map[int]*NodeConn)
	network.nodeConns2 = make(map[int]*NodeConn)

	go network.accept(listen)

	for k, v := range nodeAddrs {
		nodeConn1 := newNodeConn(k, v, true, &network)
		network.nodeConns1[k] = nodeConn1

		nodeConn2 := newNodeConn(k, v, false, &network)
		network.nodeConns2[k] = nodeConn2
	}

	return &network
}

func (network *NodeNetwork) accept(listen net.Listener) {
	defer listen.Close()

	for {
		conn, err := listen.Accept()
		if err != nil {
			log.Printf("error accepting: %s", err)
			continue
		}

		var buf [4]byte
		_, err = io.ReadFull(conn, buf[:])
		if err != nil {
			log.Printf("io.ReadFull failed: %s", err)
			conn.Close()
			continue
		}

		id := int(binary.LittleEndian.Uint32(buf[:]))
		log.Printf("accept id %d", id)

		nodeConn := network.nodeConns2[id]
		if nodeConn == nil {
			log.Printf("nw.nodeConns2[%d] == nil", id)
			conn.Close()
			continue
		}

		if !atomic.CompareAndSwapUint32(&nodeConn.connFlag, 0, 1) {
			log.Printf("nodeConn.connFlag [%d]", id)
			conn.Close()
			continue
		}

		nodeConn.conn = conn
		nodeConn.readReader = bufio.NewReader(nodeConn.conn)

		log.Printf("accept connect from %d", id)

		go nodeConn.acceptProcess()
	}
}

func (network *NodeNetwork) send(id int, m message) {
	conn := network.nodeConns1[id]
	if conn == nil {
		return
	}

	if atomic.LoadUint32(&conn.connFlag) == 0 {
		return
	}

	select {
	case conn.sendBuf <- m:
	case <-time.After(time.Second):
		log.Printf("send timeout to(%d)", id)
	}
}

func (network *NodeNetwork) response(id int, m message) {
	conn := network.nodeConns2[id]
	if conn == nil {
		return
	}

	if atomic.LoadUint32(&conn.connFlag) == 0 {
		return
	}

	select {
	case conn.sendBuf <- m:
	case <-time.After(time.Second):
		log.Printf("response timeout to(%d)", id)
	}
}

func (network *NodeNetwork) recv(timeout time.Duration) (message, bool) {
	select {
	case m := <-network.recvQueue:
		return m, true
	case <-time.After(timeout):
		return message{}, false
	}
}

type NodeConn struct {
	id         int
	addr       string
	active     bool
	conn       net.Conn
	readReader *bufio.Reader
	readBuf    []byte
	sendBuf    chan message
	connFlag   uint32
	waitExit   sync.WaitGroup
	network    *NodeNetwork
}

func newNodeConn(id int, addr string, active bool, network *NodeNetwork) *NodeConn {
	c := NodeConn{id: id, addr: addr, active: active, network: network}

	c.readBuf = make([]byte, 1024)
	c.sendBuf = make(chan message, 1)

	if active {
		go c.process()
	}

	return &c
}

func (c *NodeConn) process() {
	for {
		if !c.connect() {
			time.Sleep(time.Second)
			continue
		}
		atomic.StoreUint32(&c.connFlag, 1)
		c.waitExit.Add(2)
		go c.send()
		go c.recv()

		c.waitExit.Wait()
		atomic.StoreUint32(&c.connFlag, 0)
		c.conn = nil
	}
}

func (c *NodeConn) acceptProcess() {
	c.waitExit.Add(2)
	go c.send()
	go c.recv()

	c.waitExit.Wait()

	c.conn = nil
	atomic.StoreUint32(&c.connFlag, 0)
}

func (c *NodeConn) send() {
	defer func() {
		c.conn.Close()
		c.waitExit.Done()

		if err := recover(); err != nil {
			fmt.Printf("panic: %v\n\n%s", err, debug.Stack())
		}
	}()

	for {
		m := <-c.sendBuf

		if m.typ == Closed {
			break
		}

		var buf [1024]byte
		var size uint32
		binary.LittleEndian.PutUint32(buf[:], size)
		size += 4
		binary.LittleEndian.PutUint32(buf[size:], uint32(m.typ))
		size += 4
		binary.LittleEndian.PutUint32(buf[size:], uint32(m.from))
		size += 4
		binary.LittleEndian.PutUint32(buf[size:], uint32(m.instanceID))
		size += 4
		binary.LittleEndian.PutUint32(buf[size:], uint32(m.proposalBallot))
		size += 4
		binary.LittleEndian.PutUint32(buf[size:], uint32(m.rejectBallot))
		size += 4
		binary.LittleEndian.PutUint32(buf[size:], uint32(m.acceptBallot))
		size += 4
		val := []byte(m.acceptValue)
		for i := range val {
			buf[size] = val[i]
			size++
		}
		binary.LittleEndian.PutUint32(buf[:], size)

		_, err := c.conn.Write(buf[:size])
		if err != nil {
			fmt.Println("Conn.Write failed:", err)
			break
		}
	}
}

func (c *NodeConn) recv() {
	defer func() {
		m := message{typ: Closed}
		c.sendBuf <- m

		c.conn.Close()
		c.waitExit.Done()

		if err := recover(); err != nil {
			fmt.Printf("panic: %v\n\n%s", err, debug.Stack())
		}
	}()

	for {
		var head [4]byte
		_, err := io.ReadFull(c.readReader, head[:])
		if err != nil {
			log.Printf("io.ReadFull1 failed: %s id: %d active: %v", err, c.id, c.active)
			break
		}

		size := binary.LittleEndian.Uint32(head[:]) - 4
		if size > uint32(len(c.readBuf)) {
			panic("size > uint32(len(c.readBuf))")
		}

		_, err = io.ReadFull(c.readReader, c.readBuf[:size])
		if err != nil {
			log.Printf("io.ReadFull2 failed: %s id: %d", err, c.id)
			break
		}

		var n int
		var m message
		m.typ = int(binary.LittleEndian.Uint32(c.readBuf[n:]))
		n += 4
		m.from = int(binary.LittleEndian.Uint32(c.readBuf[n:]))
		n += 4
		m.instanceID = int(binary.LittleEndian.Uint32(c.readBuf[n:]))
		n += 4
		m.proposalBallot = int(binary.LittleEndian.Uint32(c.readBuf[n:]))
		n += 4
		m.rejectBallot = int(binary.LittleEndian.Uint32(c.readBuf[n:]))
		n += 4
		m.acceptBallot = int(binary.LittleEndian.Uint32(c.readBuf[n:]))
		n += 4
		m.acceptValue = string(c.readBuf[n:size])

		c.network.recvQueue <- m
	}
}

func (c *NodeConn) connect() bool {
	conn, err := net.Dial("tcp", c.addr)
	if err != nil {
		log.Println("re connect error ", err)
		return false
	}

	c.conn = conn
	c.readReader = bufio.NewReader(c.conn)

	var buf [4]byte
	binary.LittleEndian.PutUint32(buf[:], uint32(c.network.nodeID))
	_, err = c.conn.Write(buf[:])
	if err != nil {
		fmt.Println("Conn.Write failed:", err)
		conn.Close()
		return false
	}

	log.Printf("connect to %d", c.id)

	return true
}
