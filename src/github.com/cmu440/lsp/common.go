package lsp

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
		return nil // or panic, or return an error
	}
	n := len(*pq)
	item := (*pq)[n-1]
	*pq = (*pq)[:n-1]
	return item
}

// Peek returns the highest priority element without removing it from the heap.
func (pq *MessageQueue) Peek() interface{} {
	if pq.Len() == 0 {
		return nil // or handle error as needed
	}
	// The highest priority element is at the root of the heap.
	return (*pq)[0]
}

type SlidingWindow struct {
	writeQueue *MessageQueue
}

func (s *SlidingWindow) ValidAck(seqNumber int) bool {
	// Implementation here
	return false
}

func (s *SlidingWindow) AcknowledgeMessage(seqNumber int, cumulative bool) {
	// Implementation here
}

func NewSlidingWindow(params *Params) *SlidingWindow {
	// Implementation here
	return nil
}

func (s *SlidingWindow) AdjustWindow() {
}

func (s *SlidingWindow) SendWindow() {
}

func (s *SlidingWindow) Size() int {
	return 0
}

// HashSet type using a map for boolean values to track unique sequence numbers
type HashSet map[int]bool

// NewHashSet creates a new instance of HashSet
func NewHashSet() HashSet {
	return make(HashSet)
}

// Add adds a new sequence number to the set
func (set HashSet) Add(seqNum int) {
	set[seqNum] = true
}

// Remove removes a sequence number from the set
func (set HashSet) Remove(seqNum int) {
	delete(set, seqNum)
}

// Contains checks if a sequence number is in the set
func (set HashSet) Contains(seqNum int) bool {
	return set[seqNum]
}
