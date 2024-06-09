// Contains the implementation of a LSP client.

package lsp

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/cmu440/lspnet"
)

type client struct {
	conn                       *lspnet.UDPConn
	connectionId               int
	sequenceNumber             int
	params                     *Params
	allRoutinesTerminatedChan  chan bool
	closeClientChan            chan bool
	closeReadRoutineChan       chan bool
	connEstablishedChan        chan bool
	inboundMessageChan         chan *Message
	requestBeenClosedChan      chan bool
	responseBeenClosedChan     chan bool
	requestConnLostChan        chan bool
	responseConnLostChan       chan bool
	requestMessageExistsChan   chan bool
	responseMessageExistsChan  chan bool
	requestNewSequenceNumChan  chan bool
	responseNewSequenceNumChan chan int
	requestOrderedMessageChan  chan bool
	responseOrderedMessageChan chan *Message
	writeChan                  chan *Message
}

// NewClient creates, initiates, and returns a new client. This function
// should return after a connection with the server has been established
// (i.e., the client has received an Ack message from the server in response
// to its connection request), and should return a non-nil error if a
// connection could not be made (i.e., if after K epochs, the client still
// hasn't received an Ack message from the server in response to its K
// connection requests).
//
// initialSeqNum is an int representing the Initial Sequence Number (ISN) this
// client must use. You may assume that sequence numbers do not wrap around.
//
// hostport is a colon-separated string identifying the server's host address
// and port number (i.e., "localhost:9999").
func NewClient(hostport string, initialSeqNum int, params *Params) (Client, error) {
	// Open up UDP connection
	addr, err := lspnet.ResolveUDPAddr("udp", hostport)
	if err != nil {
		return nil, err
	}
	//bind local socket to remote address
	conn, err := lspnet.DialUDP("udp", nil, addr)
	if err != nil {
		return nil, err
	}
	// Instantiate client
	client := &client{
		conn:                 conn,
		closeReadRoutineChan: make(chan bool),
		connEstablishedChan:  make(chan bool),
		inboundMessageChan:   make(chan *Message),
		sequenceNumber:       initialSeqNum,
		params:               params,
	}
	// Launch read, and worker routines
	go client.clientWorkerRoutine()
	go client.readRoutine()
	// block on signal that indicates whether LSP connection was established or not
	madeGood := <-client.connEstablishedChan
	if madeGood {
		return client, nil
	}
	client.Close()
	return nil, errors.New("unable to establish connection")
}

// ConnID returns the connection ID of the client. It is immutable after being set initially.
func (c *client) ConnID() int {
	return c.connectionId
}

// Read will hang if server has already been explicitly closed
func (c *client) Read() ([]byte, error) {
	// If Close has been called on the client, subsequent calls to Read must either return a non-nil error, or never return anything.
	c.requestBeenClosedChan <- true
	beenClosed := <-c.responseBeenClosedChan
	if beenClosed {
		return nil, errors.New("client has been closed")
	}
	// Return a non-nil error if the connection with the server has been lost and no other messages are waiting to be returned
	c.requestConnLostChan <- true
	connLost := <-c.responseConnLostChan
	if connLost {
		c.requestMessageExistsChan <- true
		exists := <-c.responseMessageExistsChan
		if !exists {
			return nil, errors.New("connection has been lost")
		}
	}
	c.requestOrderedMessageChan <- true
	msg := <-c.responseOrderedMessageChan
	return msg.Payload, nil
	// (3) the server is closed ~ this is ambiguous af, so will ignore for now. Only thing I can think of right now that would check for this is a ICMP message
}

// Write will hang if server has already been explicitly closed
func (c *client) Write(payload []byte) error {
	// return a non-nil error if the connection with the server has been lost
	c.requestConnLostChan <- true
	connLost := <-c.responseConnLostChan
	if connLost {
		return errors.New("connection has been lost")
	}
	// If Close has been called on the client, subsequent calls to Write must either return a non-nil error, or never return anything.
	c.requestBeenClosedChan <- true
	beenClosed := <-c.responseBeenClosedChan
	if beenClosed {
		return errors.New("client has been closed")
	}
	// construct data message
	c.requestNewSequenceNumChan <- true
	seqNum := <-c.responseNewSequenceNumChan
	checkSum := CalculateChecksum(c.ConnID(), seqNum, len(payload), payload)
	dataMsg := NewData(c.ConnID(), seqNum, len(payload), payload, checkSum)
	// offload to worker routine
	c.writeChan <- dataMsg
	return nil
}

// Close will hang if server has already been explicitly closed
func (c *client) Close() error {
	c.closeClientChan <- true
	<-c.allRoutinesTerminatedChan
	return nil
}

func (c *client) clientWorkerRoutine() {
	// This should only return when all spawned routines are terminated
	defer func() { c.allRoutinesTerminatedChan <- true }()
	for {
		select {
		case <-c.closeClientChan:
			// insert closeProcedure
			return
		case <-c.writeChan:
			// Put into queue to be admitted into session sliding window
		}
	}
}

// readRoutine continuously reads LSP packets from the UDP connection, and passes them to the worker routine for handling
func (c *client) readRoutine() {
	for {
		select {
		case <-c.closeReadRoutineChan:
			return
		default:
			c.readAndProcessPacket()
		}
	}
}

// readAndProcessPacket reads a packet from the UDP connection and processes it
func (c *client) readAndProcessPacket() {
	buffer := make([]byte, 2000)
	bytesRead, err := c.conn.Read(buffer)
	if err != nil {
		fmt.Println("Error reading from connection:", err)
		return
	}

	validBuffer := buffer[:bytesRead]
	msg, err := unmarshalAndVerifyMessage(validBuffer)
	if err != nil {
		fmt.Println("Error processing message:", err)
		return
	}

	c.inboundMessageChan <- msg
}

// unmarshalAndVerifyMessage unmarshals the buffer into a Message and verifies its integrity
func unmarshalAndVerifyMessage(buffer []byte) (*Message, error) {
	msg := &Message{}
	if err := json.Unmarshal(buffer, msg); err != nil {
		return nil, fmt.Errorf("unmarshal error: %w", err)
	}

	// Verify payload size
	if msg.Size > len(msg.Payload) {
		return nil, fmt.Errorf("payload size mismatch")
	}

	msg.Payload = msg.Payload[:msg.Size]

	// Verify checksum
	if msg.Checksum != CalculateChecksum(msg.ConnID, msg.SeqNum, msg.Size, msg.Payload) {
		return nil, fmt.Errorf("checksum mismatch")
	}

	return msg, nil
}
