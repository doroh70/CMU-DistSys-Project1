package lsp

import (
	"container/heap"
	"encoding/json"
	"fmt"
	"github.com/cmu440/lspnet"
)

// MessageQueue implements heap.Interface and holds Messages. Messages are ordered by sequence number, lowest appearing first on the queue.
type MessageQueue []*Message

func (pq *MessageQueue) Len() int { return len(*pq) }

func (pq *MessageQueue) Less(i, j int) bool {
	// Less method determines the priority (sorted by SeqNum here)
	return (*pq)[i].SeqNum < (*pq)[j].SeqNum
}

func (pq *MessageQueue) Swap(i, j int) {
	(*pq)[i], (*pq)[j] = (*pq)[j], (*pq)[i]
}

func (pq *MessageQueue) Push(x interface{}) {
	// Push a new element into the heap
	*pq = append(*pq, x.(*Message))
}

func (pq *MessageQueue) Pop() interface{} {
	if pq.Len() == 0 {
		return nil
	}
	n := len(*pq)
	item := (*pq)[n-1]
	*pq = (*pq)[:n-1]
	return item
}

// Peek returns the highest priority element without removing it from the heap.
func (pq *MessageQueue) Peek() interface{} {
	if pq.Len() == 0 {
		return nil
	}
	// The highest priority element is at the root of the heap.
	return (*pq)[0]
}

type WindowedMessage struct {
	message          *Message
	acked            bool
	currentBackOff   int
	currBackOffTimer int
	sent             bool
}

type SlidingWindow struct {
	writeQueue         *MessageQueue
	fIndex             int
	lIndex             int
	window             []*WindowedMessage
	Size               int
	windowSize         int
	maxBackOffInterval int
	currUnacked        int
	maxUnacked         int
	lastSeqNum         int // last sequence num to pass through window
}

func NewSlidingWindow(params *Params, initialSeqNum int) *SlidingWindow {
	// Implementation here
	writeQueue := &MessageQueue{}
	heap.Init(writeQueue)
	sw := &SlidingWindow{
		writeQueue:         writeQueue,
		fIndex:             0,
		lIndex:             0,
		window:             make([]*WindowedMessage, params.WindowSize),
		Size:               0,
		windowSize:         params.WindowSize,
		maxBackOffInterval: params.MaxBackOffInterval,
		currUnacked:        0,
		maxUnacked:         params.MaxUnackedMessages,
		lastSeqNum:         initialSeqNum,
	}
	return sw
}

func (s *SlidingWindow) ValidAck(seqNumber int) bool {
	// Ensure the message at fIndex is initialized
	if s.window[s.fIndex] == nil {
		return false
	}

	// Define the start sequence number at fIndex
	startSeq := s.window[s.fIndex].message.SeqNum

	// Compute the end sequence number at the position just before lIndex
	endIndex := (s.lIndex - 1 + s.windowSize) % s.windowSize // wrap around safe
	endSeq := s.window[endIndex].message.SeqNum

	// Check if seqNumber falls within the logical range of [startSeq, endSeq]
	return seqNumber >= startSeq && seqNumber <= endSeq
}

// AcknowledgeMessage assumes that the message with seqNumber exists in the SlidingWindow. It will cause a runtime panic if seqNumber passed is not valid.
func (s *SlidingWindow) AcknowledgeMessage(seqNumber int, cumulative bool) {
	// sets s.window[s.fIndex].message.SeqNum - seqNumber position to acked
	singleAckFunc := func(seqNumber int) {
		updateIndexFromFirst := (s.fIndex + (seqNumber - s.window[s.fIndex].message.SeqNum)) % s.windowSize
		s.window[updateIndexFromFirst].acked = true
		s.currUnacked--
	}

	seq := seqNumber
	singleAckFunc(seq)
	seq--

	for ; cumulative && s.ValidAck(seq); seq-- {
		singleAckFunc(seq)
	}
}

func (s *SlidingWindow) AdjustWindow() {
	// Remove acknowledged messages from the front of the window
	for s.Size > 0 && s.window[s.fIndex] != nil && s.window[s.fIndex].acked {
		s.fIndex = (s.fIndex + 1) % s.windowSize
		s.Size--
	}

	// Handle new messages from the writeQueue
	for {
		// Check if we have space in the window
		if s.Size == s.windowSize {
			break // Window is full, cannot add more messages
		}

		nextMessage := s.writeQueue.Peek()
		if nextMessage == nil {
			break
		}

		// Calculate the sequence number of the last message in the window or lastSeqNum if window is empty
		lastIndex := (s.lIndex - 1 + s.windowSize) % s.windowSize
		var currentEndSeq int // latest message seqNum in window
		if s.window[lastIndex] != nil {
			currentEndSeq = s.window[lastIndex].message.SeqNum
		} else {
			currentEndSeq = s.lastSeqNum // This ensures the new message is treated as the first message
		}

		// Check if the next message is contiguous
		if nextMessage.(*Message).SeqNum == currentEndSeq+1 {
			// Add the next message to the window
			s.window[s.lIndex] = &WindowedMessage{
				message:          nextMessage.(*Message),
				acked:            false,
				currentBackOff:   -1,
				currBackOffTimer: 0,
				sent:             false,
			}
			// Move lIndex to the next available slot
			s.lIndex = (s.lIndex + 1) % s.windowSize
			// Remove the message from the writeQueue
			s.writeQueue.Pop()
			// Increment the size
			s.Size++
			// Update lastSeqNum
			s.lastSeqNum++
		} else {
			// If the next message is not contiguous, break the loop
			break
		}
	}
}

// SendNewlyAdmittedToWindow sends messages in the window with currentBackOff = -1 and updates their currentBackOff to 0 after sending them.
// If the destination address is nil, it is assumed that the connection was created with DialUDP.
func (s *SlidingWindow) SendNewlyAdmittedToWindow(conn *lspnet.UDPConn, addr *lspnet.UDPAddr) bool {
	// Flag to check if any data was sent successfully
	dataSent := false

	// Iterate through the window
	for i := 0; i < s.Size; i++ {
		// Check if we can send more unacknowledged messages
		if s.currUnacked < s.maxUnacked {
			// Calculate the current index in the circular buffer
			index := (s.fIndex + i) % s.windowSize

			// Check if the message at the current index has currentBackOff = -1 and has not been sent before
			if s.window[index] != nil && s.window[index].currentBackOff == -1 && !s.window[index].sent {
				// Send the message using SendMessage
				err := SendMessage(conn, s.window[index].message, addr, 3)
				if err != nil {
					fmt.Printf("Error sending message in window: %s, error: %v\n", s.window[index].message.String(), err)
					continue
				}

				// Mark that we have successfully sent at least one message
				dataSent = true

				// Update currentBackOff to 0 and mark message as sent
				s.window[index].currentBackOff = 0
				s.window[index].sent = true

				// Increment the count of unacknowledged messages
				s.currUnacked++
			}
		} else {
			break
		}
	}
	return dataSent
}

// SendWindowWithBackOff sends messages in the window that are due for transmission
// based on their back-off timer, and adjusts their back-off intervals accordingly.
// If the destination address is nil, it is assumed that the connection was created with DialUDP.
func (s *SlidingWindow) SendWindowWithBackOff(conn *lspnet.UDPConn, addr *lspnet.UDPAddr) bool {
	// Flag to check if any data was sent successfully
	dataSent := false

	// Iterate through the window
	for i := 0; i < s.Size; i++ {
		// Calculate the current index in the circular buffer
		index := (s.fIndex + i) % s.windowSize

		// Access the current message
		msg := s.window[index]

		// If currBackOffTimer reaches 0, send the message
		if msg.currBackOffTimer == 0 {
			// Check if we can send more unacknowledged messages or if this is a resend
			if s.currUnacked < s.maxUnacked || msg.sent {
				// Send the message
				err := SendMessage(conn, msg.message, addr, 3)
				if err != nil {
					fmt.Printf("Error sending message in window: %s, error: %v\n", s.window[index].message.String(), err)
					continue
				}

				// Mark that we have successfully sent at least one message
				dataSent = true

				// Update currentBackOff exponentially
				if msg.currentBackOff < s.maxBackOffInterval {
					msg.currentBackOff *= 2
					if msg.currentBackOff > s.maxBackOffInterval {
						msg.currentBackOff = s.maxBackOffInterval
					}
				}

				// After sending, reset currBackOffTimer to new currentBackOff
				msg.currBackOffTimer = msg.currentBackOff

				// Increment the count of unacknowledged messages if this is the first time the message is sent
				if !msg.sent {
					s.currUnacked++
					msg.sent = true
				}
			}
		} else {
			// Decrement the currBackOffTimer
			msg.currBackOffTimer--
		}
	}

	return dataSent
}

func (s *SlidingWindow) Push(msg *Message) {
	heap.Push(s.writeQueue, msg)
}

// HashSetInt type using a map for boolean values to track unique integers
type HashSetInt map[int]bool

// NewHashSetInt creates a new instance of HashSetInt
func NewHashSetInt() HashSetInt {
	return make(HashSetInt)
}

// Add adds a new number to the set
func (set HashSetInt) Add(num int) {
	set[num] = true
}

// Remove removes a number from the set
func (set HashSetInt) Remove(num int) {
	delete(set, num)
}

// Contains checks if a number is in the set
func (set HashSetInt) Contains(num int) bool {
	return set[num]
}

// HashSetStr type using a map for boolean values to track unique strings
type HashSetStr map[string]bool

// NewHashSetStr creates a new instance of HashSetStr
func NewHashSetStr() HashSetStr {
	return make(HashSetStr)
}

// Add adds a new string to the set
func (set HashSetStr) Add(str string) {
	set[str] = true
}

// Remove removes a string from the set
func (set HashSetStr) Remove(str string) {
	delete(set, str)
}

// Contains checks if a string is in the set
func (set HashSetStr) Contains(str string) bool {
	return set[str]
}

// SendMessage sends a message over a UDP connection with retry logic.
// If the destination address is nil, it is assumed that the connection was created with DialUDP.
func SendMessage(conn *lspnet.UDPConn, msg *Message, addr *lspnet.UDPAddr, maxRetries int) error {
	if maxRetries < 0 {
		maxRetries = 0
	}

	// Marshal the message to JSON format
	marshalledMsg, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("error marshalling message: %s, error: %v", msg.String(), err)
	}

	for attempt := 1; attempt <= maxRetries; attempt++ {
		// Write the marshalled message to the UDP connection
		if addr != nil {
			_, err = conn.WriteToUDP(marshalledMsg, addr)
		} else {
			_, err = conn.Write(marshalledMsg)
		}

		if err == nil {
			// If write is successful, exit the loop
			return nil
		}
		fmt.Printf("Attempt %d: Error sending message: %s, error: %v\n", attempt, msg.String(), err)
	}
	// Return the error after maxRetries attempts
	return fmt.Errorf("after %d attempts, failed to send message: %s, error: %v", maxRetries, msg.String(), err)
}
