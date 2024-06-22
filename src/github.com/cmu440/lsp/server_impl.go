// Contains the implementation of a LSP server.

package lsp

import (
	"errors"
	"fmt"
	"github.com/cmu440/lspnet"
	"time"
)

type server struct {
	// TODO: Implement this!
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
	srv := &server{}

	// Launch read and server worker routines
	go srv.readRoutine()
	go srv.serverWorkerRoutine()

	return srv, nil
}

func (s *server) readRoutine() {

}

func (s *server) serverWorkerRoutine() {

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

		time.Sleep(10 * time.Millisecond)
	}
	// Return a non-nil error if the connection with the client has been explicitly closed
	s.requestConnClosed <- msg.ConnID
	closed := <-s.responseConnClosed
	if closed {
		return msg.ConnID, nil, errors.New("connection has been closed")
	}

	// Return a non-nil error if the connection with the client has been lost and no other messages are waiting to be returned
	s.requestConnLostChan <- msg.ConnID
	connLost := <-s.responseConnLostChan
	if connLost {
		s.requestMessageExistsChan <- msg.ConnID
		exists := <-s.responseMessageExistsChan
		if !exists {
			return msg.ConnID, nil, errors.New("connection has been lost")
		}
	}

	return msg.ConnID, msg.Payload, nil
}

func (s *server) Write(connId int, payload []byte) error {
	s.requestActiveConnChan <- connId
	active := <-s.responseActiveConnChan
	if active {
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

// CloseConn terminates the client with the specified connection ID, returning
// a non-nil error if the specified connection ID does not exist. All pending
// messages to the client should be sent and acknowledged. However, unlike Close,
// this method should NOT block.
func (s *server) CloseConn(connId int) error {
	s.requestActiveConnChan <- connId
	active := <-s.responseActiveConnChan
	if active {
		s.closeClientSession <- connId
	}
	return fmt.Errorf("connection with connId %d not active", connId)
}

// Close terminates all currently connected clients and shuts down the LSP server.
// This method should block until all pending messages for each client are sent
// and acknowledged. If one or more clients are lost during this time, a non-nil
// error should be returned. Once it returns, all goroutines running in the
// background should exit.
func (s *server) Close() error {
	s.closeServerChan <- true
	err := <-s.allSpawnedRoutinesTerminatedChan
	return err
}
