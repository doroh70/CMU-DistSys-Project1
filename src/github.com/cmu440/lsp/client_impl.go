// Contains the implementation of a LSP client.

package lsp

import (
	"container/heap"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/cmu440/lspnet"
	"time"
)

type client struct {
	addr                             *lspnet.UDPAddr
	beenClosed                       bool
	conn                             *lspnet.UDPConn
	connAckReceived                  bool
	connectionId                     int
	currentEpochsElapsed             int
	dataSentLastEpoch                bool
	epochTicker                      *time.Ticker
	isConnLost                       bool
	initialSequenceNumber            int // This value should only be instantiated in NewClient method
	numMessagesToReturn              int
	params                           *Params
	sequenceNumber                   int
	readQueue                        *MessageQueue
	readSet                          HashSetInt
	lastReadSeqNum                   int
	slidingWindow                    *SlidingWindow
	allSpawnedRoutinesTerminatedChan chan bool
	closeClientChan                  chan bool
	closeReadRoutineChan             chan bool
	readRoutineClosedChan            chan bool
	connEstablishedChan              chan bool // connEstablishedChan signals to startup routine via NewClient method, whether a connection with the server was established or not. A false signal would trigger the Close client method.
	inboundMessageChan               chan *Message
	requestBeenClosedChan            chan bool
	responseBeenClosedChan           chan bool
	requestConnLostChan              chan bool
	responseConnLostChan             chan bool
	requestMessageExistsChan         chan bool
	responseMessageExistsChan        chan bool
	requestNewSequenceNumChan        chan bool
	responseNewSequenceNumChan       chan int
	requestOrderedMessageChan        chan bool
	responseOrderedMessageChan       chan *Message
	writeChan                        chan *Message
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

	readQueue := &MessageQueue{}
	heap.Init(readQueue)

	// Instantiate client
	client := &client{
		addr:                             addr,
		beenClosed:                       false,
		conn:                             conn,
		connAckReceived:                  false,
		connectionId:                     0,
		currentEpochsElapsed:             0,
		dataSentLastEpoch:                false,
		isConnLost:                       false,
		initialSequenceNumber:            initialSeqNum,
		params:                           params,
		sequenceNumber:                   initialSeqNum,
		readQueue:                        readQueue,
		readSet:                          NewHashSetInt(),
		lastReadSeqNum:                   initialSeqNum,
		slidingWindow:                    NewSlidingWindow(params, initialSeqNum),
		allSpawnedRoutinesTerminatedChan: make(chan bool, 1),
		closeClientChan:                  make(chan bool),
		closeReadRoutineChan:             make(chan bool, 1),
		readRoutineClosedChan:            make(chan bool, 1),
		connEstablishedChan:              make(chan bool),
		inboundMessageChan:               make(chan *Message),
		requestBeenClosedChan:            make(chan bool),
		responseBeenClosedChan:           make(chan bool),
		requestConnLostChan:              make(chan bool),
		responseConnLostChan:             make(chan bool),
		requestMessageExistsChan:         make(chan bool),
		responseMessageExistsChan:        make(chan bool),
		requestNewSequenceNumChan:        make(chan bool),
		responseNewSequenceNumChan:       make(chan int),
		requestOrderedMessageChan:        make(chan bool),
		responseOrderedMessageChan:       make(chan *Message),
		writeChan:                        make(chan *Message),
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
	<-client.allSpawnedRoutinesTerminatedChan
	return nil, errors.New("unable to establish connection")
}

// ConnID returns the connection ID of the client. It is immutable after being set initially.
func (c *client) ConnID() int {
	return c.connectionId
}

// Read will hang if client has already been explicitly closed
func (c *client) Read() ([]byte, error) {
	// If Close has been called on the client, subsequent calls to Read must either return a non-nil error, or never return anything.
	c.requestBeenClosedChan <- true
	beenClosed := <-c.responseBeenClosedChan
	if beenClosed {
		return nil, errors.New("client has been closed")
	}
	var msg *Message

	c.requestOrderedMessageChan <- true
	msg = <-c.responseOrderedMessageChan

	// Return a non-nil error if the connection with the server has been lost and no other messages are waiting to be returned
	c.requestConnLostChan <- true
	connecLost := <-c.responseConnLostChan
	if connecLost {
		c.requestMessageExistsChan <- true
		exists := <-c.responseMessageExistsChan
		if !exists {
			return nil, errors.New("connection has been lost")
		}
	}
	return msg.Payload, nil
	// (3) the server is closed ~ this is ambiguous af, so will ignore for now. Only thing I can think of right now that would check for this is a ICMP message. can do this
}

// Write will hang if client has already been explicitly closed
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

// Close will hang if client has already been explicitly closed
func (c *client) Close() error {
	c.closeClientChan <- true
	<-c.allSpawnedRoutinesTerminatedChan
	return nil
}

func (c *client) clientWorkerRoutine() {
	// This should only return when all spawned routines are terminated
	defer func() { c.allSpawnedRoutinesTerminatedChan <- true }()

	// Attempt to send the connection message to the server with retry
	connMsg := NewConnect(c.initialSequenceNumber)
	err := SendMessage(c.conn, connMsg, nil, 3)
	if err != nil {
		// fmt.Println("Failed to establish connection:", err)
	} else {
		c.dataSentLastEpoch = true
	}

	c.epochTicker = time.NewTicker(time.Duration(c.params.EpochMillis) * time.Millisecond)
	defer c.epochTicker.Stop()

	firstShutdown := true // ensures we perform shut down procedure once
	for {
		select {
		case <-c.closeClientChan:
			c.beenClosed = true
		case msg := <-c.inboundMessageChan:
			c.currentEpochsElapsed = 0 // reset epochLimit counter
			connEstablishedFunc := func() {
				if msg.SeqNum == c.initialSequenceNumber && !c.connAckReceived {
					c.connAckReceived = true
					c.connectionId = msg.ConnID
					c.connEstablishedChan <- true
				}
			}
			acknowledgeFunc := func(cumulative bool) {
				if c.slidingWindow.ValidAck(msg.SeqNum) {
					c.slidingWindow.AcknowledgeMessage(msg.SeqNum, cumulative)
					c.slidingWindow.AdjustWindow()
					c.dataSentLastEpoch = c.slidingWindow.SendNewlyAdmittedToWindow(c.conn, nil)
				}
			}
			thisMightPerformShutdownProcedure := func() {
				if c.beenClosed && c.slidingWindow.Size+c.slidingWindow.writeQueue.Len() == 0 && firstShutdown {
					_ = c.conn.Close()
					c.closeReadRoutineChan <- true
					firstShutdown = false
				}
			}

			// check message type
			switch msg.Type {
			case MsgAck:
				// if conn ack, signal a connection has been established
				connEstablishedFunc()
				// { if ack doesn't fall under sliding window, discard} else { acknowledge corresponding message and make AdjustWindow call and SendNewlyAdmittedToWindow call}
				acknowledgeFunc(false)

				thisMightPerformShutdownProcedure()
			case MsgCAck:
				// if conn ack, signal a connection has been established
				connEstablishedFunc()
				// { if ack doesn't fall under sliding window, discard} else { acknowledge corresponding message and make AdjustWindow call and SendNewlyAdmittedToWindow call}
				acknowledgeFunc(true)

				thisMightPerformShutdownProcedure()
			case MsgData:
				// if data message, send ack and add to readBuffer (buffer is ordered by serverSeqNum)  { if serverSeqNum  isn't > client chosen ISN, ignore msg} {if duplicate seqNum, ack and don't add to buffer}
				if msg.SeqNum > c.initialSequenceNumber {
					if !c.readSet.Contains(msg.SeqNum) {
						c.readSet.Add(msg.SeqNum)
						heap.Push(c.readQueue, msg)
						if msg.SeqNum == c.lastReadSeqNum+1 && c.numMessagesToReturn > 0 {
							_ = heap.Pop(c.readQueue)
							c.responseOrderedMessageChan <- msg
							c.lastReadSeqNum += 1
							c.numMessagesToReturn--
						}
					}
					// send ack
					ack := NewAck(msg.ConnID, msg.SeqNum)
					err := SendMessage(c.conn, ack, nil, 3)
					if err != nil {
						// fmt.Println("Ack failed with error:", err)
					}
				} else {
					// fmt.Printf("message with unexpexted seq num received: %s\n", msg)
				}
			default:
				// Do nothing. client should ignore connect messages
			}
		case <-c.readRoutineClosedChan:
			if c.beenClosed {
				return
			}
			c.allSpawnedRoutinesTerminatedChan <- true
		case <-c.requestBeenClosedChan:
			c.responseBeenClosedChan <- c.beenClosed
		case <-c.requestConnLostChan:
			c.responseConnLostChan <- c.isConnLost
		case <-c.requestMessageExistsChan:
			if c.readQueue.Len() > 0 {
				c.responseMessageExistsChan <- true
			} else {
				c.responseMessageExistsChan <- false
			}
		case <-c.requestNewSequenceNumChan:
			c.sequenceNumber += 1
			c.responseNewSequenceNumChan <- c.sequenceNumber
		case <-c.requestOrderedMessageChan:
			msg := c.readQueue.Peek()
			if (msg != nil) && (msg.(*Message).SeqNum == c.lastReadSeqNum+1) {
				_ = heap.Pop(c.readQueue)
				c.responseOrderedMessageChan <- msg.(*Message)
				c.lastReadSeqNum += 1
			} else {
				c.numMessagesToReturn++
			}
		case msg := <-c.writeChan:
			if msg != nil {
				// push in intermediate buffer ~ this will be PQ
				c.slidingWindow.Push(msg)
				// make AdjustWindow call
				c.slidingWindow.AdjustWindow()
				// make SendNewlyAdmittedToWindow call
				c.dataSentLastEpoch = c.slidingWindow.SendNewlyAdmittedToWindow(c.conn, nil)
			}
		case <-c.epochTicker.C:
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
			connMsg := NewConnect(c.initialSequenceNumber)
			err := SendMessage(c.conn, connMsg, nil, 3)
			if err != nil {
				// fmt.Println("Connect failed with error:", err)
			}
		} else {
			// resend packets in sliding window (with maxUnacked constraint) that have not yet been acknowledged, according to exponential back off rules. (make SendWindowWithBackOff call)
			c.dataSentLastEpoch = c.slidingWindow.SendWindowWithBackOff(c.conn, nil)
			// If we did not send data message in last epoch, send heartbeat
			if !c.dataSentLastEpoch {
				heartBeat := NewAck(c.connectionId, 0)
				err := SendMessage(c.conn, heartBeat, nil, 3)
				if err != nil {
					// fmt.Println("Heartbeat failed with error:", err)
				} else {
					// fmt.Println("Client sent HeartBeat")
				}
			}
		}
	} else {
		// If the epoch limit is reached, signal that the connection is lost and stop epoch ticker
		// fmt.Println("Epoch limit reached.")
		c.epochTicker.Stop()
		c.isConnLost = true
		if !c.connAckReceived {
			c.connEstablishedChan <- false
		}
		//signal readRoutine to close
		c.closeReadRoutineChan <- true
		// close connection
		err := c.conn.Close()
		for ; c.numMessagesToReturn > 0; c.numMessagesToReturn-- {
			c.responseOrderedMessageChan <- nil
		}
		if err != nil {
			// fmt.Println("Failed to close LSP connection:", err)
			return
		}
	}
	c.dataSentLastEpoch = false
}

// readRoutine continuously reads LSP packets from the UDP connection, and passes them to the worker routine for handling
func (c *client) readRoutine() {
	for {
		select {
		case <-c.closeReadRoutineChan:
			c.readRoutineClosedChan <- true
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
		// fmt.Println("Error reading from connection:", err)
		return
	}

	validBuffer := buffer[:bytesRead]
	msg, err := c.unmarshalAndVerifyMessage(validBuffer)
	if err != nil {
		// fmt.Println("Error processing message:", err)
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

	msg.Payload = msg.Payload[:msg.Size] // "If the size of the data is longer than the given size, there is no “correct” behavior, but one such solution, which LSP should employ, is to simply truncate the data to the correct length."

	// Verify checksum
	if msg.Type == MsgData && msg.Checksum != CalculateChecksum(msg.ConnID, msg.SeqNum, msg.Size, msg.Payload) {
		return nil, fmt.Errorf("checksum mismatch")
	}

	return msg, nil
}
