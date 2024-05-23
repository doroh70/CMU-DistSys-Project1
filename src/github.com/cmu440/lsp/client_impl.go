// Contains the implementation of a LSP client.

package lsp

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/cmu440/lspnet"
)

type client struct {
	activeRoutines                     int
	beenClosed                         bool
	clientSeqNumber                    int
	serverSeqNumber                    int
	conn                               *lspnet.UDPConn
	connID                             int
	serverMessagesMap                  map[int]*Message
	unAckedClientMessagesMap           map[int]*Message
	incrementNumActiveRoutinesChan     chan bool
	decrementNumActiveRoutinesChan     chan bool
	requestBeenClosedChan              chan bool
	responseBeenClosedChan             chan bool
	canSafelyCloseChan                 chan bool
	closeServerChan                    chan bool
	requestConnIDChan                  chan bool
	responseConnIDChan                 chan int
	requestSequencedServerMessageChan  chan bool
	responseSequencedServerMessageChan chan *Message
	requestNewSequenceNumChan          chan bool
	responseNewSequenceNumChan         chan int
	stopReadingChan                    chan bool
	stopWritingChan                    chan bool
	serverMessageChan                  chan *Message
	writeChan                          chan *Message
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
	const INITSEQUENCENUMBER = 1
	const NUMRETRIES = 5
	addr, err := lspnet.ResolveUDPAddr("udp", hostport)
	if err != nil {
		return nil, err
	}
	//bind local socket to remote address
	conn, err := lspnet.DialUDP("udp", nil, addr)
	if err != nil {
		return nil, err
	}
	//send conn message
	msg := NewConnect(INITSEQUENCENUMBER)
	marshalledConn, err := json.Marshal(msg)
	if err != nil {
		return nil, err
	}

	//Try sending multiple times
	for i := 0; i < NUMRETRIES; i++ {
		if _, err1 := conn.Write(marshalledConn); err1 == nil {
			break
		} else if i == NUMRETRIES-1 {
			return nil, err1
		}
	}
	//wait for conn ack
	var ack *Message
	buffer := make([]byte, 1024)
	bytesRead, err := conn.Read(buffer)
	if err != nil {
		return nil, err
	}
	//Trim the buffer to the number of valid bytes read
	validBuffer := buffer[:bytesRead]
	ack = &Message{}
	err = json.Unmarshal(validBuffer, ack)
	if err != nil {
		return nil, err
	}
	if ack.Type != MsgAck || ack.SeqNum != INITSEQUENCENUMBER {
		return nil, fmt.Errorf("invalid acknowledgment received")
	}
	//instantiate client
	client := &client{
		activeRoutines:                     0,
		beenClosed:                         false,
		clientSeqNumber:                    INITSEQUENCENUMBER,
		serverSeqNumber:                    INITSEQUENCENUMBER + 1, //per doc
		conn:                               conn,
		connID:                             ack.ConnID,
		serverMessagesMap:                  make(map[int]*Message),
		unAckedClientMessagesMap:           make(map[int]*Message),
		incrementNumActiveRoutinesChan:     make(chan bool),
		decrementNumActiveRoutinesChan:     make(chan bool),
		requestConnIDChan:                  make(chan bool),
		responseConnIDChan:                 make(chan int),
		closeServerChan:                    make(chan bool),
		requestBeenClosedChan:              make(chan bool),
		responseBeenClosedChan:             make(chan bool),
		canSafelyCloseChan:                 make(chan bool, 1),
		requestSequencedServerMessageChan:  make(chan bool),
		responseSequencedServerMessageChan: make(chan *Message),
		requestNewSequenceNumChan:          make(chan bool),
		responseNewSequenceNumChan:         make(chan int),
		stopReadingChan:                    make(chan bool, 1),
		stopWritingChan:                    make(chan bool, 1),
		serverMessageChan:                  make(chan *Message),
		writeChan:                          make(chan *Message),
	}

	//start client worker routine
	go client.clientWorkerRoutine()
	//start reader routine
	go client.readRoutine()
	//start writer routine
	go client.writeRoutine()
	return client, nil
}

func (c *client) runWithActiveRoutine(fn func()) {
	c.incrementNumActiveRoutinesChan <- true
	fn()
	c.decrementNumActiveRoutinesChan <- true
}

func (c *client) checkClosed() bool {
	//if client has been closed return error
	c.requestBeenClosedChan <- true
	closedb4 := <-c.responseBeenClosedChan
	if closedb4 {
		return true
	}
	return false
}

func (c *client) clientWorkerRoutine() {
	c.activeRoutines++
	defer func() { c.activeRoutines-- }()
	for {
		select {
		case <-c.incrementNumActiveRoutinesChan:
			c.activeRoutines++
		case <-c.decrementNumActiveRoutinesChan:
			c.activeRoutines--
		case <-c.requestBeenClosedChan:
			c.responseBeenClosedChan <- c.beenClosed
		case <-c.closeServerChan:
			//Mark the client as having been closed to prevent new operations
			c.beenClosed = true

			//Implement check to ensure unAckedClientMessagesMap is empty then do below..

			//Stop reading and writing routines by signaling stop channels
			c.stopReadingChan <- true
			c.stopWritingChan <- true

			c.canSafelyCloseChan <- true
			return
		case <-c.requestConnIDChan:
			c.responseConnIDChan <- c.connID
		case <-c.requestSequencedServerMessageChan:
			//Search serverMessagesMap for the message with the current serverSeqNumber
			msg, found := c.serverMessagesMap[c.serverSeqNumber]
			if found {
				//If the message is present, send the message through the channel
				c.responseSequencedServerMessageChan <- msg
				//Increment the server sequence number
				c.serverSeqNumber++
			} else {
				//If no message is found, send a nil pointer
				c.responseSequencedServerMessageChan <- nil
			}
		case <-c.requestNewSequenceNumChan:
			c.clientSeqNumber++
			c.responseNewSequenceNumChan <- c.clientSeqNumber
		case msg := <-c.serverMessageChan:
			//if we already have that message in map, send ack and continue
			if _, exists := c.serverMessagesMap[msg.SeqNum]; exists {
				ack := NewAck(c.connID, msg.SeqNum)
				c.writeChan <- ack
				continue
			} else {
				switch msg.Type {
				case MsgAck:
					//Delete singular message from unAckedClientMessagesMap
					if _, exists := c.unAckedClientMessagesMap[msg.SeqNum]; exists {
						delete(c.unAckedClientMessagesMap, msg.SeqNum)
						fmt.Println("Ack received, message removed:", msg.SeqNum)
					} else {
						fmt.Println("Ack received for non-existent message:", msg.SeqNum)
					}
				case MsgCAck:
					//Delete multiple messages from unAckedClientMessagesMap up to the given sequence number
					for seqNum := range c.unAckedClientMessagesMap {
						if seqNum <= msg.SeqNum {
							delete(c.unAckedClientMessagesMap, seqNum)
						}
					}
					fmt.Printf("CAck received, messages up to %d removed\n", msg.SeqNum)
				case MsgData:
					//verify checksum, if valid add to serverMessagesMap
					checkSum := CalculateChecksum(msg.ConnID, msg.SeqNum, msg.Size, msg.Payload)
					if checkSum == msg.Checksum {
						//If checksum is valid, add message to serverMessagesMap
						c.serverMessagesMap[msg.SeqNum] = msg
						//Then send acknowledgment back to sender
						ack := NewAck(msg.ConnID, msg.SeqNum)
						c.writeChan <- ack
					} else {
						//Handle the case where the checksum does not match
						fmt.Printf("Checksum verification failed for server message : %s\n", msg)
						continue
					}
				default:
					//do nothing, shouldn't get here
					fmt.Printf("Unexpected message type received from server: %s\n", msg)
				}
			}
		}
	}
}

func (c *client) writeRoutine() {
	c.runWithActiveRoutine(func() {
		for {
			select {
			case <-c.stopWritingChan:
				return
			case msg := <-c.writeChan:
				// Convert the message into JSON format
				byteMsg, err := json.Marshal(msg)
				if err != nil {
					fmt.Println("Error marshalling JSON:", err)
					continue // Skip this message if marshalling fails
				}

				maxRetries := 3
				for attempt := 0; attempt < maxRetries; attempt++ {
					nbytes, err := c.conn.Write(byteMsg)
					if err != nil {
						fmt.Printf("Error writing to socket: %v, retrying %d of %d\n", err, attempt+1, maxRetries)
						continue // Retry sending the message
					}
					if nbytes != len(byteMsg) {
						fmt.Printf("Warning: Only %d out of %d bytes sent, retrying %d of %d\n", nbytes, len(byteMsg), attempt+1, maxRetries)
						continue // Retry sending the message
					}
					// Exit the loop if the message is sent successfully
					break
				}

				//Check if message type is MsgData and if the message doesn't already exist in the map
				if msg.Type == MsgData {
					if _, exists := c.unAckedClientMessagesMap[msg.SeqNum]; !exists {
						//Add to unAckedMap
						c.unAckedClientMessagesMap[msg.SeqNum] = msg
					}
				}
			}
		}
	})
}

func (c *client) readRoutine() {
	c.runWithActiveRoutine(func() {
		for {
			select {
			case <-c.stopReadingChan:
				return
			default:
				var msg *Message
				buffer := make([]byte, 1024)
				for {
					bytesRead, err := c.conn.Read(buffer)
					if err == nil {
						validBuffer := buffer[:bytesRead]
						msg = &Message{}
						if err := json.Unmarshal(validBuffer, msg); err == nil {
							c.serverMessageChan <- msg
						} else {
							fmt.Println("Error unmarshalling JSON:", err)
						}
						break
					}
					fmt.Println("Error reading from connection:", err)
					break
				}
			}
		}
	})
}

func (c *client) ConnID() int {
	c.requestConnIDChan <- true
	id := <-c.responseConnIDChan
	return id
}

func (c *client) Read() ([]byte, error) {
	//if conn has been closed return error
	if closedb4 := c.checkClosed(); closedb4 {
		return nil, errors.New("server has already been closed")
	}
	//request for message in order of seq #
	var seqServerDataMsg *Message
	for {
		c.requestSequencedServerMessageChan <- true
		seqServerDataMsg = <-c.responseSequencedServerMessageChan
		if seqServerDataMsg != nil {
			break
		}
	}
	//return message
	return seqServerDataMsg.Payload, nil
}

func (c *client) Write(payload []byte) error {
	//if conn has been closed return error
	if closedb4 := c.checkClosed(); closedb4 {
		return errors.New("server has already been closed")
	}
	//construct data message
	c.requestNewSequenceNumChan <- true
	seqNum := <-c.responseNewSequenceNumChan
	checkSum := CalculateChecksum(c.ConnID(), seqNum, len(payload), payload)
	dataMsg := NewData(c.ConnID(), seqNum, len(payload), payload, checkSum)
	//send to writer routine
	c.writeChan <- dataMsg
	return nil
}

func (c *client) Close() error {
	//if conn has been closed return error
	if closedb4 := c.checkClosed(); closedb4 {
		return errors.New("server has already been closed")
	}
	//signal server has been closed
	c.closeServerChan <- true
	//all pending outgoing messages should be sent and acked ~ wait until unAcked buffer is empty and writes are sent
	<-c.canSafelyCloseChan
	//sanity checking all spawned routines are closed
	for {
		if c.activeRoutines == 0 {
			break
		}
	}
	//close LSP socket
	err := c.conn.Close()
	if err != nil {
		return err
	}
	return nil
}
