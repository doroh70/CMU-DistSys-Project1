// Contains the implementation of a LSP server.

package lsp

import (
	"container/heap"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/cmu440/lspnet"
	"time"
)

type server struct {
	numActiveConns                        int
	activeConnAddresses                   HashSetStr
	beenClosed                            bool
	params                                *Params
	clientConnections                     map[int]*clientStub
	lostConnections                       map[int]*clientStub
	clientLostDuringClose                 bool
	sequentialConnId                      int
	socket                                *lspnet.UDPConn
	allSpawnedRoutinesTerminatedChan      chan error
	closeClientSessionChan                chan int
	closeReadRoutineChan                  chan bool //yet to use
	closeServerChan                       chan bool
	inboundMessageChan                    chan *messageAndAddress
	readRoutineClosedChan                 chan bool
	requestActiveConnChan                 chan int
	responseConnActiveChan                chan bool
	requestConnClosedChan                 chan int
	responseConnClosedChan                chan bool
	requestConnLostChan                   chan int
	responseConnLostChan                  chan bool
	requestGenericConnLostNoMessagesChan  chan bool
	responseGenericConnLostNoMessagesChan chan int
	requestMessageExistsChan              chan int
	responseMessageExistsChan             chan bool
	requestNewSequenceNumChan             chan int
	responseNewSequenceNumChan            chan int
	requestOrderedMessageChan             chan bool
	responseOrderedMessageChan            chan *Message
	writeChan                             chan *Message
}

type messageAndAddress struct {
	msg  *Message
	addr *lspnet.UDPAddr
}

// Status ~ Connection status
type Status int

// constants for the Status type
const (
	active Status = iota
	closed
	lost
)

func (s Status) String() string {
	switch s {
	case active:
		return "active"
	case closed:
		return "closed"
	case lost:
		return "lost"
	default:
		return "unknown"
	}
}

type clientStub struct {
	addr                  *lspnet.UDPAddr
	closeOpFinished       bool
	currentEpochsElapsed  int
	dataSentLastEpoch     bool
	initialSequenceNumber int
	lastReadSeqNum        int
	sequenceNumber        int
	readSet               HashSetInt
	readQueue             *MessageQueue
	slidingWindow         *SlidingWindow
	status                Status
}

// NewServer creates, initiates, and returns a new server. This function should
// NOT block. Instead, it should spawn one or more goroutines (to handle things
// like accepting incoming client connections, triggering epoch events at
// fixed intervals, synchronizing events using a for-select loop like you saw in
// project 0, etc.) and immediately return. It should return a non-nil error if
// there was an error resolving or listening on the specified port number.
func NewServer(port int, params *Params) (Server, error) {
	// Resolve the UDP address to bind to
	addr, err := lspnet.ResolveUDPAddr("udp", fmt.Sprintf("localhost:%d", port))
	if err != nil {
		return nil, fmt.Errorf("failed to resolve UDP address: %v", err)
	}

	// Create and bind the UDP connection
	conn, err := lspnet.ListenUDP("udp", addr)
	if err != nil {
		return nil, fmt.Errorf("failed to listen on UDP address: %v", err)
	}

	// Create the server instance
	srv := &server{
		numActiveConns:                        0,
		activeConnAddresses:                   NewHashSetStr(),
		beenClosed:                            false,
		params:                                params,
		clientConnections:                     make(map[int]*clientStub),
		lostConnections:                       make(map[int]*clientStub),
		clientLostDuringClose:                 false,
		sequentialConnId:                      1,
		socket:                                conn,
		allSpawnedRoutinesTerminatedChan:      make(chan error),
		closeClientSessionChan:                make(chan int),
		closeReadRoutineChan:                  make(chan bool, 1),
		closeServerChan:                       make(chan bool),
		inboundMessageChan:                    make(chan *messageAndAddress),
		readRoutineClosedChan:                 make(chan bool, 1),
		requestActiveConnChan:                 make(chan int),
		responseConnActiveChan:                make(chan bool),
		requestConnClosedChan:                 make(chan int),
		responseConnClosedChan:                make(chan bool),
		requestConnLostChan:                   make(chan int),
		responseConnLostChan:                  make(chan bool),
		requestGenericConnLostNoMessagesChan:  make(chan bool),
		responseGenericConnLostNoMessagesChan: make(chan int),
		requestMessageExistsChan:              make(chan int),
		responseMessageExistsChan:             make(chan bool),
		requestNewSequenceNumChan:             make(chan int),
		responseNewSequenceNumChan:            make(chan int),
		requestOrderedMessageChan:             make(chan bool),
		responseOrderedMessageChan:            make(chan *Message),
		writeChan:                             make(chan *Message),
	}

	// Launch read and server worker routines
	go srv.readRoutine()
	go srv.serverWorkerRoutine()

	return srv, nil
}

func (s *server) readRoutine() {
	for {
		select {
		case <-s.closeReadRoutineChan:
			s.readRoutineClosedChan <- true
			return
		default:
			s.readAndProcessPacket()
		}
	}
}

func (s *server) readAndProcessPacket() {
	buffer := make([]byte, 2000)
	bytesRead, addr, err := s.socket.ReadFromUDP(buffer)
	if err != nil {
		// fmt.Println("Error reading from connection:", err)
		return
	}

	validBuffer := buffer[:bytesRead]
	msg, err := s.unmarshalAndVerifyMessage(validBuffer)
	if err != nil {
		// fmt.Println("Error processing message:", err)
		return
	}

	s.inboundMessageChan <- &messageAndAddress{msg: msg, addr: addr}
}

// unmarshalAndVerifyMessage unmarshals the buffer into a Message and verifies its integrity using checksum calculation and payload size
func (s *server) unmarshalAndVerifyMessage(buffer []byte) (*Message, error) {
	msg := &Message{}
	if err := json.Unmarshal(buffer, msg); err != nil {
		return nil, fmt.Errorf("unmarshal error: %w", err)
	}

	if err := s.verifyMessage(msg); err != nil {
		return nil, err
	}

	return msg, nil
}

// verifyMessage verifies the integrity of a Message using checksum calculation and payload size
func (s *server) verifyMessage(msg *Message) error {
	switch msg.Type {
	case MsgData:
		// Verify payload size
		if msg.Size > len(msg.Payload) {
			return fmt.Errorf("payload size mismatch")
		}

		msg.Payload = msg.Payload[:msg.Size] // "If the size of the data is longer than the given size, there is no “correct” behavior, but one such solution, which LSP should employ, is to simply truncate the data to the correct length."

		// Verify checksum
		if msg.Checksum != CalculateChecksum(msg.ConnID, msg.SeqNum, msg.Size, msg.Payload) {
			return fmt.Errorf("checksum mismatch")
		}
	default:
	}
	return nil
}

func (s *server) serverWorkerRoutine() {
	ticker := time.NewTicker(time.Duration(s.params.EpochMillis) * time.Millisecond)

	checkConnState := func(id int, status Status) {
		var chanInsert bool
		if s.clientConnections[id] != nil {
			chanInsert = s.clientConnections[id].status == status
		} else {
			chanInsert = false
		}
		switch status {
		case active:
			s.responseConnActiveChan <- chanInsert
		case closed:
			s.responseConnClosedChan <- chanInsert
		case lost:
			s.responseConnLostChan <- chanInsert
		}
	}

	for {
		select {
		case id := <-s.closeClientSessionChan:
			if s.clientConnections[id] != nil {
				s.clientConnections[id].status = closed
				s.mightTerminateClientConnection(id)
			}
		case <-s.closeServerChan:
			s.beenClosed = true
		case msgAndAddy := <-s.inboundMessageChan:
			var stub *clientStub
			acknowledgeFunc := func(cumulative bool) {
				if stub.slidingWindow.ValidAck(msgAndAddy.msg.SeqNum) {
					stub.currentEpochsElapsed = 0 // reset epochLimit counter
					stub.slidingWindow.AcknowledgeMessage(msgAndAddy.msg.SeqNum, cumulative)
					stub.slidingWindow.AdjustWindow()
					stub.dataSentLastEpoch = stub.slidingWindow.SendNewlyAdmittedToWindow(s.socket, stub.addr)
				} else if stub.slidingWindow.Size+stub.slidingWindow.writeQueue.Len() == 0 && msgAndAddy.msg.SeqNum == 0 {
					stub.currentEpochsElapsed = 0 // reset epochLimit counter if no messages to send and ack is seq # 0
				}
			}

			// check message type
			switch msgAndAddy.msg.Type {
			case MsgConnect:
				if !s.activeConnAddresses.Contains(msgAndAddy.addr.String()) && s.clientConnections[msgAndAddy.msg.ConnID] == nil {
					// send conn ack
					ack := NewAck(s.sequentialConnId, msgAndAddy.msg.SeqNum)
					s.sequentialConnId++
					err := SendMessage(s.socket, ack, msgAndAddy.addr, 3)
					if err != nil {
						// fmt.Println("Ack failed with error:", err)
					} else {
						readQueue := &MessageQueue{}
						heap.Init(readQueue)
						//instantiate client stub
						stub = &clientStub{
							addr:                  msgAndAddy.addr,
							closeOpFinished:       false,
							currentEpochsElapsed:  0,
							dataSentLastEpoch:     false,
							initialSequenceNumber: msgAndAddy.msg.SeqNum,
							lastReadSeqNum:        msgAndAddy.msg.SeqNum,
							sequenceNumber:        msgAndAddy.msg.SeqNum,
							readSet:               NewHashSetInt(),
							readQueue:             readQueue,
							slidingWindow:         NewSlidingWindow(s.params, msgAndAddy.msg.SeqNum),
							status:                active,
						}
						// add client stub to server clientConnections
						s.clientConnections[ack.ConnID] = stub
						// add address to active addresses set
						s.activeConnAddresses.Add(msgAndAddy.addr.String())
						s.numActiveConns++
					}
				}
			case MsgAck:
				if s.activeConnAddresses.Contains(msgAndAddy.addr.String()) && s.clientConnections[msgAndAddy.msg.ConnID] != nil {
					stub = s.clientConnections[msgAndAddy.msg.ConnID]
					//stub.currentEpochsElapsed = 0 // reset epochLimit counter if no messages to send and ack is seq # 0
					acknowledgeFunc(false)
					s.mightTerminateClientConnection(msgAndAddy.msg.ConnID)
				}
			case MsgCAck:
				if s.activeConnAddresses.Contains(msgAndAddy.addr.String()) && s.clientConnections[msgAndAddy.msg.ConnID] != nil {
					stub = s.clientConnections[msgAndAddy.msg.ConnID]
					//stub.currentEpochsElapsed = 0 // reset epochLimit counter if no messages to send and ack is seq # 0
					acknowledgeFunc(true)
					s.mightTerminateClientConnection(msgAndAddy.msg.ConnID)
				}
			case MsgData:
				if s.activeConnAddresses.Contains(msgAndAddy.addr.String()) && s.clientConnections[msgAndAddy.msg.ConnID] != nil {
					stub = s.clientConnections[msgAndAddy.msg.ConnID]
					stub.currentEpochsElapsed = 0 // reset epochLimit counter
					// if data message, send ack and add to readBuffer (buffer is ordered by serverSeqNum)  { if serverSeqNum  isn't > client chosen ISN, ignore msg} {if duplicate seqNum, ack and don't add to buffer}
					if msgAndAddy.msg.SeqNum > stub.initialSequenceNumber {
						if !stub.readSet.Contains(msgAndAddy.msg.SeqNum) {
							stub.readSet.Add(msgAndAddy.msg.SeqNum)
							heap.Push(stub.readQueue, msgAndAddy.msg)
						}
						// send ack
						ack := NewAck(msgAndAddy.msg.ConnID, msgAndAddy.msg.SeqNum)
						err := SendMessage(s.socket, ack, msgAndAddy.addr, 3)
						if err != nil {
							// fmt.Println("Ack failed with error:", err)
						}
					} else {
						// fmt.Printf("message with unexpexted seq num received: %s\n", msgAndAddy.msg)
					}
				}
			default:
				// Undefined behaviour
			}
		case <-s.readRoutineClosedChan:
			if s.clientLostDuringClose {
				s.allSpawnedRoutinesTerminatedChan <- errors.New("one or more clients lost during server close operation")
			} else {
				s.allSpawnedRoutinesTerminatedChan <- nil
			}
			return
		case id := <-s.requestActiveConnChan:
			checkConnState(id, active)
		case id := <-s.requestConnClosedChan:
			checkConnState(id, closed)
		case id := <-s.requestConnLostChan:
			checkConnState(id, lost)
		case <-s.requestGenericConnLostNoMessagesChan:
			var found = false
			// loop through lost connections, if one of them doesn't have any messages, remove from list and pass its id to responseChan
			for id, stub := range s.lostConnections {
				if stub.readQueue.Len() == 0 {
					delete(s.lostConnections, id)
					found = true
					s.responseGenericConnLostNoMessagesChan <- id
					break
				}
			}
			if !found {
				s.responseGenericConnLostNoMessagesChan <- 0
			}
		case id := <-s.requestMessageExistsChan:
			if s.clientConnections[id] == nil || s.clientConnections[id].readQueue.Peek() == nil {
				s.responseMessageExistsChan <- false
			} else {
				s.responseMessageExistsChan <- true
			}
		case id := <-s.requestNewSequenceNumChan:
			if s.clientConnections[id] == nil {
				s.responseNewSequenceNumChan <- -1 // pass flag to show an error
			} else {
				s.clientConnections[id].sequenceNumber++
				s.responseNewSequenceNumChan <- s.clientConnections[id].sequenceNumber
			}
		case <-s.requestOrderedMessageChan:
			var found bool
			// loop through each cliStub and check if message atop readQueue has seqNum == lastReadSeqNum + 1
			for _, stub := range s.clientConnections {
				msg := stub.readQueue.Peek()
				if (msg != nil) && (msg.(*Message).SeqNum == stub.lastReadSeqNum+1) {
					_ = heap.Pop(stub.readQueue)
					s.responseOrderedMessageChan <- msg.(*Message)
					stub.lastReadSeqNum++
					found = true
					break
				}
			}
			// if not found send a nil message
			if !found {
				s.responseOrderedMessageChan <- nil
			}
		case msg := <-s.writeChan:
			if msg != nil {
				if err := s.verifyMessage(msg); err == nil {
					//add message to slidingWindow of corresponding clientStub
					id := msg.ConnID
					if stub := s.clientConnections[id]; stub != nil {
						// push in intermediate buffer ~ this will be PQ
						stub.slidingWindow.Push(msg)
						// make AdjustWindow call
						stub.slidingWindow.AdjustWindow()
						// make SendNewlyAdmittedToWindow call
						stub.dataSentLastEpoch = stub.slidingWindow.SendNewlyAdmittedToWindow(s.socket, stub.addr)
					}
				}
			}
		case <-ticker.C:
			s.handleEpochEvent()
		}
	}
}

func (s *server) handleEpochEvent() {
	for id, stub := range s.clientConnections {
		if stub.status != lost && stub.closeOpFinished == false {
			// increment the epochs elapsed counter
			stub.currentEpochsElapsed++

			// check we are not at epochLimit
			if stub.currentEpochsElapsed < s.params.EpochLimit {
				// resend packets in sliding window (with maxUnacked constraint) that have not yet been acknowledged, according to exponential back off rules. (make SendWindowWithBackOff call)
				stub.dataSentLastEpoch = stub.slidingWindow.SendWindowWithBackOff(s.socket, stub.addr)
				// If we did not send data message in last epoch, send heartbeat
				if !stub.dataSentLastEpoch {
					heartBeat := NewAck(id, 0)
					err := SendMessage(s.socket, heartBeat, stub.addr, 3)
					if err != nil {
						// fmt.Println("Heartbeat failed with error:", err)
					} else {
						// fmt.Println("Server sent HeartBeat")
					}
				}
			} else {
				// If the epoch limit is reached, signal that the connection is lost
				stub.status = lost
				s.lostConnections[id] = stub
				// fmt.Printf("Epoch limit reached for client connection with address : %s.\n", stub.addr)
				if s.beenClosed {
					s.clientLostDuringClose = true
				}
				s.numActiveConns--
			}
			stub.dataSentLastEpoch = false

		}
		// if active conns  = 0, do below
		if s.numActiveConns == 0 && s.beenClosed {
			// close connection
			err := s.socket.Close()
			if err != nil {
				// fmt.Println("Failed to close LSP server socket:", err)
				return
			}
			s.closeReadRoutineChan <- true
		}
	}
}

func (s *server) mightTerminateClientConnection(id int) {
	// if status is closed or server closed and there are no more messages to send/ be acknowledged -> stub.CloseFin = true
	if stub := s.clientConnections[id]; stub != nil {
		if (stub.status == closed || s.beenClosed == true) && !stub.closeOpFinished {
			if stub.slidingWindow.Size+stub.slidingWindow.writeQueue.Len() == 0 {
				stub.closeOpFinished = true
				s.numActiveConns--
			}
		}
	}
}

// Read will hang if server has already been explicitly closed
func (s *server) Read() (int, []byte, error) {
	var msg *Message
	for {
		s.requestOrderedMessageChan <- true
		msg = <-s.responseOrderedMessageChan
		if msg != nil {
			break
		}

		// Return a non-nil error if a connection with any client has been lost and no other messages are waiting to be returned from said client
		s.requestGenericConnLostNoMessagesChan <- true
		id := <-s.responseGenericConnLostNoMessagesChan
		if id != 0 {
			return id, nil, errors.New("connection has been lost")
		}

		time.Sleep(1 * time.Millisecond)
	}
	// Return a non-nil error if the connection with the client has been explicitly closed
	s.requestConnClosedChan <- msg.ConnID
	beenClosed := <-s.responseConnClosedChan
	if beenClosed {
		return msg.ConnID, nil, errors.New("connection has been beenClosed")
	}

	return msg.ConnID, msg.Payload, nil
}

// Write will hang if server has already been explicitly closed
func (s *server) Write(connId int, payload []byte) error {
	s.requestActiveConnChan <- connId
	connActive := <-s.responseConnActiveChan
	if connActive {
		// construct data message
		s.requestNewSequenceNumChan <- connId
		seqNum := <-s.responseNewSequenceNumChan
		checkSum := CalculateChecksum(connId, seqNum, len(payload), payload)
		dataMsg := NewData(connId, seqNum, len(payload), payload, checkSum)
		// offload to worker routine
		s.writeChan <- dataMsg
		return nil
	}
	return fmt.Errorf("connection with connId %d not active", connId)
}

// CloseConn will hang if server has already been explicitly closed
// CloseConn terminates the client with the specified connection ID, returning
// a non-nil error if the specified connection ID does not exist. All pending
// messages to the client should be sent and acknowledged. However, unlike Close,
// this method should NOT block.
// You may assume that after CloseConn has been called, neither Write nor CloseConn will be called on that same connection ID again
func (s *server) CloseConn(connId int) error {
	s.requestActiveConnChan <- connId
	connActive := <-s.responseConnActiveChan
	if connActive {
		s.closeClientSessionChan <- connId
		return nil
	}
	return fmt.Errorf("connection with connId %d not active", connId)
}

// Close will hang if server has already been explicitly closed
// Close terminates all currently connected clients and shuts down the LSP server.
// This method should block until all pending messages for each client are sent
// and acknowledged. If one or more clients are lost during this time, a non-nil
// error should be returned. Once it returns, all goroutines running in the
// background should exit.
// You may also assume that no other Server methods calls will be made after Close has been called.
func (s *server) Close() error {
	s.closeServerChan <- true
	err := <-s.allSpawnedRoutinesTerminatedChan
	return err
}
