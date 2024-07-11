package bitcoin

import (
	"log"
	"os"
)

var (
	INFO  *log.Logger
	WARN  *log.Logger
	ERROR *log.Logger
)

const (
	perm = os.FileMode(0666)
)

func InitLoggers(name string) (*os.File, error) {
	fileName := name + "_log.txt"
	// Open the file with truncate mode to replace the file if it already exists
	file, err := os.OpenFile(fileName, os.O_RDWR|os.O_CREATE|os.O_TRUNC, perm)
	if err != nil {
		return nil, err
	}
	file.Close()

	// Reopen the file in append mode for subsequent writes
	file, err = os.OpenFile(fileName, os.O_RDWR|os.O_CREATE|os.O_APPEND, perm)
	if err != nil {
		return nil, err
	}

	INFO = log.New(file, "INFO: ", log.Ldate|log.Ltime|log.Lshortfile)
	WARN = log.New(file, "WARN: ", log.Ldate|log.Ltime|log.Lshortfile)
	ERROR = log.New(file, "ERROR: ", log.Ldate|log.Ltime|log.Lshortfile)

	return file, nil
}

type Job struct {
	ClientID      int
	Message       *string
	Upper         uint64
	Lower         uint64
	ComputeTime   uint64
	WaitTime      uint64
	ResponseRatio float64
}

// UpdateWaitTime increments job wait time
func (job *Job) UpdateWaitTime() {
	job.WaitTime = job.WaitTime + 10
}

// UpdateResponseRatio updates job response ratio
func (job *Job) UpdateResponseRatio() {
	job.ResponseRatio = float64(job.WaitTime) / float64(job.ComputeTime)
}

// JobQueue implements heap.Interface and holds Jobs. Jobs are ordered by ResponseRatio, highest appearing first on the queue.
type JobQueue []*Job

func (jq *JobQueue) Len() int { return len(*jq) }

func (jq *JobQueue) Less(i, j int) bool {
	// Less method determines the priority (sorted by ResponseRatio here)
	return (*jq)[i].ResponseRatio > (*jq)[j].ResponseRatio
}

func (jq *JobQueue) Swap(i, j int) {
	(*jq)[i], (*jq)[j] = (*jq)[j], (*jq)[i]
}

func (jq *JobQueue) Push(x interface{}) {
	// Push a new element into the heap
	*jq = append(*jq, x.(*Job))
}

func (jq *JobQueue) Pop() interface{} {
	if jq.Len() == 0 {
		return nil
	}
	n := len(*jq)
	item := (*jq)[n-1]
	*jq = (*jq)[:n-1]
	return item
}

// Peek returns the highest priority element without removing it from the heap.
func (jq *JobQueue) Peek() interface{} {
	if jq.Len() == 0 {
		return nil
	}
	// The highest priority element is at the root of the heap.
	return (*jq)[0]
}
