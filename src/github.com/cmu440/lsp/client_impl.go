// Contains the implementation of a LSP client.

package lsp

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/cmu440/lspnet"
	"time"
)

type client struct {
	beenClosed                 bool
	conn                       *lspnet.UDPConn
	connAckReceived            bool
	connectionId               int
	currentEpochsElapsed       int
	isConnLost                 bool
	sequenceNumber             int
	params                     *Params
	allRoutinesTerminatedChan  chan bool
	closeClientChan            chan bool
	closeReadRoutineChan       chan bool
	connEstablishedChan        chan bool // connEstablishedChan signals to startup routine via NewClient method, whether a connection with the server was established or not. A false signal would trigger the Close client method.
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
	addr, err := lspnet.ResolveUDPAddr("udp", hostport)
	if err != nil {
		return nil, err
	}
	// bind local socket to remote address
	conn, err := lspnet.DialUDP("udp", nil, addr)
	if err != nil {
		return nil, err
	}
	// Instantiate client
	client := &client{
		beenClosed:                 false,
		conn:                       conn,
		connAckReceived:            false,
		connectionId:               0,
		currentEpochsElapsed:       0,
		isConnLost:                 false,
		sequenceNumber:             initialSeqNum,
		params:                     params,
		allRoutinesTerminatedChan:  make(chan bool),
		closeClientChan:            make(chan bool),
		closeReadRoutineChan:       make(chan bool),
		connEstablishedChan:        make(chan bool),
		inboundMessageChan:         make(chan *Message),
		requestBeenClosedChan:      make(chan bool),
		responseBeenClosedChan:     make(chan bool),
		requestConnLostChan:        make(chan bool),
		responseConnLostChan:       make(chan bool),
		requestMessageExistsChan:   make(chan bool),
		responseMessageExistsChan:  make(chan bool),
		requestNewSequenceNumChan:  make(chan bool),
		responseNewSequenceNumChan: make(chan int),
		requestOrderedMessageChan:  make(chan bool),
		responseOrderedMessageChan: make(chan *Message),
		writeChan:                  make(chan *Message),
	}
	// Launch read, and worker routines
	go client.clientWorkerRoutine()
	go client.readRoutine()

	// Block on signal that indicates whether LSP connection was established or not
	madeGood := <-client.connEstablishedChan
	if madeGood {
		return client, nil
	}

	// Close the client if the connection was not established
	_ = client.Close()
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

	// Attempt to send the connection message to the server with retry
	err := c.retrySendConnect(3) // 3 attempts
	if err != nil {
		fmt.Println("Failed to establish connection:", err)
		c.connEstablishedChan <- false // this will go on to close the server ~ look up NewClient method
	}

	epochTimer := time.NewTimer(time.Duration(c.params.EpochMillis) * time.Millisecond)
	for {
		select {
		case <-c.closeClientChan:
			// insert close Procedure
			return
		case <-c.closeReadRoutineChan:
			// insert close readRoutine procedure
		case msg := <-c.inboundMessageChan:
			// check message type
			switch msg.Type {
			case MsgAck:
				// { if conn ack, signal a connection has been established}
				// { if ack doesn't fall under sliding window, discard} else { acknowledge corresponding message and make adjustWindow call and sendWindow call}
			case MsgCAck:
				// { if ack doesn't fall under sliding window, discard} else { acknowledge corresponding message and make adjustWindow call and sendWindow call}
			case MsgData:
				// if data message, send ack and add to readBuffer (buffer is ordered by serverSeqNum) { if serverSeqNum  isn't >= client chosen ISN +1, ignore msg} {if duplicate seqNum, ack and don't add to buffer}
			default:
				// Do nothing. client should ignore connect messages
			}
		case <-c.requestBeenClosedChan:
			c.responseBeenClosedChan <- c.beenClosed
		case <-c.requestConnLostChan:
			c.responseConnLostChan <- c.isConnLost
		case <-c.requestMessageExistsChan:
			// insert message exists query procedure
		case <-c.requestNewSequenceNumChan:
			c.sequenceNumber++
			c.responseNewSequenceNumChan <- c.sequenceNumber
		case <-c.requestOrderedMessageChan:
			// insert ordered message read
		case <-c.writeChan:
			// If the seqNum received is contiguous with the tail of writeBuff, push it to tail(and push others in intermediate buffer that are contiguous with that too)
			// make adjustWindow call
			// make sendWindow call
			// Else, keep in intermediate buffer ~ this can be PQ?
		case <-epochTimer.C:
			c.handleEpochEvent()
		}
	}
}

// handleEpochEvent check pdf documentation
func (c *client) handleEpochEvent() {
	// Increment the epochs elapsed counter
	c.currentEpochsElapsed++

	// check we are not at epochLimit
	if c.currentEpochsElapsed < c.params.EpochLimit {
		// If we haven't received a connection acknowledgment, attempt to resend
		if !c.connAckReceived {
			// Retry sending the connection request up to 3 times
			err := c.retrySendConnect(3)
			if err != nil {
				fmt.Println("Send failed with error:", err)
			}
		} else {
			if len(slidingWindow) == 0 {
				// If there are no messages to send, send a heartbeat instead
			} else {
				// Else resend packets in sliding window (with maxUnacked constraint) that have not yet been acknowledged, according to exponential back off rules. (make sendWindow call)
			}
		}
	} else {
		// If the epoch limit is reached, signal that the connection is lost
		fmt.Println("Epoch limit reached.")
		c.isConnLost = true
		// close connection
		err := c.conn.Close()
		if err != nil {
			fmt.Println("Failed to close LSP connection:", err)
			return
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
	msg, err := c.unmarshalAndVerifyMessage(validBuffer)
	if err != nil {
		fmt.Println("Error processing message:", err)
		return
	}

	c.inboundMessageChan <- msg
}

// unmarshalAndVerifyMessage unmarshals the buffer into a Message and verifies its integrity using checksum calculation and payload size
func (c *client) unmarshalAndVerifyMessage(buffer []byte) (*Message, error) {
	msg := &Message{}
	if err := json.Unmarshal(buffer, msg); err != nil {
		return nil, fmt.Errorf("unmarshal error: %w", err)
	}

	// Verify connection ID of message
	if c.connAckReceived && msg.ConnID != c.connectionId {
		return nil, fmt.Errorf("connection ID mismatch")
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

// sendMessage attempts to send a message over the network up to maxRetries times.
func (c *client) sendMessage(msg *Message, maxRetries int) error {
	if maxRetries < 0 {
		maxRetries = 0
	}

	marshalledMsg, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("error marshalling message: %v", err)
	}

	for attempt := 1; attempt <= maxRetries; attempt++ {
		_, err = c.conn.Write(marshalledMsg)
		if err == nil {
			// If write is successful, exit the loop
			return nil
		}
		fmt.Printf("Attempt %d: Error sending message: %v\n", attempt, err)
	}
	// Return the error after maxRetries attempts
	return fmt.Errorf("after %d attempts, failed to send message: %v", maxRetries, err)
}

// retrySendConnect constructs the connection message and uses sendMessage to send it.
func (c *client) retrySendConnect(maxRetries int) error {
	connMsg := NewConnect(c.sequenceNumber)
	return c.sendMessage(connMsg, maxRetries)
}
