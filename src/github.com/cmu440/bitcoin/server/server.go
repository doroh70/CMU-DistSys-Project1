package main

import (
	"container/heap"
	"encoding/json"
	"fmt"
	"github.com/cmu440/bitcoin"
	"math"
	"os"
	"strconv"
	"time"

	"github.com/cmu440/lsp"
)

type readResult struct {
	connId       int
	byteResponse []byte
	err          error
}

type request struct {
	jobs         []*bitcoin.Job // sorted slice according to nonce range
	bestHash     uint64
	bestNonce    uint64
	numJobs      int // Tells you how many pieces request was broken down to
	finishedJobs int // If finishedJobs = numJobs, send result with bestHash and bestNonce to client
}

type server struct {
	lspServer        lsp.Server
	jobQueue         *bitcoin.JobQueue      // max heap priority queue sorted by responseRatio
	minerJobs        map[int][]*bitcoin.Job // Identifies which miner has which jobs. int identifier is connID of miner ~ look up part A lsp impl for more details
	activeMiners     []int
	clientRequestMap map[int]*request // Identifies which client made which request. int identifier is connID of client
}

func startServer(port int) (*server, error) {
	lspServer, err := lsp.NewServer(port, lsp.NewParams())
	if err != nil {
		return nil, err
	}
	jobQueue := &bitcoin.JobQueue{}
	heap.Init(jobQueue)
	btcServer := &server{
		lspServer:        lspServer,
		jobQueue:         jobQueue,
		minerJobs:        make(map[int][]*bitcoin.Job),
		clientRequestMap: make(map[int]*request),
	}

	return btcServer, nil
}

func main() {

	const numArgs = 2
	if len(os.Args) != numArgs {
		fmt.Printf("Usage: ./%s <port>", os.Args[0])
		return
	}

	port, err := strconv.Atoi(os.Args[1])
	if err != nil {
		fmt.Println("Port must be a number:", err)
		return
	}

	srv, err := startServer(port)
	if err != nil {
		fmt.Println(err.Error())
		return
	}

	fmt.Println("Server listening on port", port)

	file, err := bitcoin.InitLoggers(fmt.Sprintf("server_%d", port))
	if err != nil {
		fmt.Printf("Failed to initialize loggers %s.\n", err)
		return
	}
	defer file.Close()

	defer srv.lspServer.Close()

	srv.serverLoop()
}

func (s *server) serverLoop() {
	ticker := time.NewTicker(5 * time.Second)

	readResultChan := make(chan *readResult)

	// Read routine
	go func() {
		for {
			// Read message and send to worker routine
			connId, byteResponse, err := s.lspServer.Read()
			readResultChan <- &readResult{connId, byteResponse, err}
		}
	}()

	// Worker Routine
	for {
		select {
		case <-ticker.C:
			// Update time waited/ responseRatio of every entry in our priority queue
			for _, job := range *s.jobQueue {
				job.UpdateWaitTime()
				job.UpdateResponseRatio()
			}
			// Re-heapify the entire queue after all updates. Costly gotta find a better sln...
			heap.Init(s.jobQueue)
		case result := <-readResultChan:
			// Process Message
			s.processMessage(result)
			// Send new jobs to miner if we can
			s.querySend()
		}
	}

}

func (s *server) processMessage(dataRead *readResult) {
	// handle err
	if dataRead.err != nil {
		if s.isMinerActive(dataRead.connId) {
			bitcoin.INFO.Printf("Miner with id %d has been lost, any remaining work will be reassigned.\n", dataRead.connId)
			//remove miner from activeMiners slice
			s.removeActiveMiner(dataRead.connId)
			if s.minerJobs[dataRead.connId] != nil {
				jobs := s.minerJobs[dataRead.connId]
				for _, job := range jobs {
					s.jobQueue.Push(job)
					// heap.Push(s.jobQueue, job) This is too expensive for large requests. Let's only maintain heap property at ticker event and right before we send job to miner
				}
				delete(s.minerJobs, dataRead.connId)
			}
		} else if s.clientRequestMap[dataRead.connId] != nil {
			bitcoin.INFO.Printf("Client with id %d has been lost, any remaining work done on behalf of the client will be dropped.\n", dataRead.connId)

			// Two-pointer technique to remove jobs in-place
			i := 0
			for _, job := range *s.jobQueue {
				if job.ClientID != dataRead.connId {
					(*s.jobQueue)[i] = job
					i++
				}
			}
			// Truncate the slice to remove the unwanted elements
			*s.jobQueue = (*s.jobQueue)[:i]

			delete(s.clientRequestMap, dataRead.connId)
		}
		return
	}
	var unmarshalledResponse bitcoin.Message
	err := json.Unmarshal(dataRead.byteResponse, &unmarshalledResponse)
	if err != nil {
		bitcoin.ERROR.Printf("Unable to unmarshall message for conn id %d: %s.\n", dataRead.connId, err)
		return
	}
	switch unmarshalledResponse.Type {
	// handle join from miner
	case bitcoin.Join:
		if !s.isMinerActive(dataRead.connId) {
			s.activeMiners = append(s.activeMiners, dataRead.connId)
			bitcoin.INFO.Printf("Miner with id %d has joined.\n", dataRead.connId)
		}
	// handle request
	case bitcoin.Request:
		// split request into jobs and add to jobQueue
		jobs := splitRequestIntoJobs(dataRead.connId, &unmarshalledResponse)
		for _, job := range jobs {
			heap.Push(s.jobQueue, job)
		}
		s.clientRequestMap[dataRead.connId] = &request{
			jobs:         jobs,
			bestHash:     math.MaxUint64,
			bestNonce:    math.MaxUint64,
			numJobs:      len(jobs),
			finishedJobs: 0,
		}
		bitcoin.INFO.Printf("Client request from id %d has been split and added to the job queue.\n", dataRead.connId)
	// handle dataRead
	case bitcoin.Result:
		// Obtain miner id
		minerId := dataRead.connId

		// look for job said miner was working on
		if s.minerJobs[minerId] != nil {
			jobs := s.minerJobs[minerId]
			job := jobs[0]

			// get client id from job
			cliId := job.ClientID

			// update appropriate clientRequestMap and see if we can send the final result to client
			if s.clientRequestMap[cliId] != nil {
				bitcoin.INFO.Printf("Obtained job result from miner with id : %d.\n", dataRead.connId)
				s.clientRequestMap[cliId].finishedJobs++
				if unmarshalledResponse.Hash < s.clientRequestMap[cliId].bestHash {
					s.clientRequestMap[cliId].bestHash = unmarshalledResponse.Hash
					s.clientRequestMap[cliId].bestNonce = unmarshalledResponse.Nonce
				}

				if s.clientRequestMap[cliId].finishedJobs == s.clientRequestMap[cliId].numJobs {
					result := bitcoin.NewResult(s.clientRequestMap[cliId].bestHash, s.clientRequestMap[cliId].bestNonce)
					marshalledResult, _ := json.Marshal(result)
					errLost := s.lspServer.Write(cliId, marshalledResult)
					if errLost != nil {
						bitcoin.INFO.Printf("Client with id %d has been lost, any remaining work done on behalf of the client will be dropped.\n", dataRead.connId)
					} else {
						bitcoin.INFO.Printf("Sent final result to client with id : %d.\n", cliId)
					}
					delete(s.clientRequestMap, cliId) // delete regardless
				}

				// Miner no longer has active jobs, since miner can only have 1 at a time
				delete(s.minerJobs, minerId)
			}
		}
	}
}

func splitRequestIntoJobs(clientId int, cliRequest *bitcoin.Message) []*bitcoin.Job {
	jobSize := uint64(10000)
	var jobs []*bitcoin.Job
	for start := cliRequest.Lower; start <= cliRequest.Upper; start += jobSize {
		end := start + jobSize - 1
		if end > cliRequest.Upper {
			end = cliRequest.Upper
		}
		job := &bitcoin.Job{
			ClientID:      clientId,
			Message:       &cliRequest.Data,
			Lower:         start,
			Upper:         end,
			ComputeTime:   calculateComputeTime(&cliRequest.Data, start, end),
			WaitTime:      0,
			ResponseRatio: 0,
		}
		job.UpdateWaitTime()
		job.UpdateResponseRatio()
		jobs = append(jobs, job)
	}
	return jobs
}

// Function to calculate compute time
func calculateComputeTime(message *string, lower, upper uint64) uint64 {
	msgLength := uint64(len(*message))
	numBlocks := (msgLength + 63) / 64 // SHA-256 processes data in 64-byte blocks
	return numBlocks * (upper - lower + 1)
}

// Function to check if a miner is active
func (s *server) isMinerActive(connId int) bool {
	for _, id := range s.activeMiners {
		if id == connId {
			return true
		}
	}
	return false
}

// Function to remove an active miner
func (s *server) removeActiveMiner(connId int) {
	for i, id := range s.activeMiners {
		if id == connId {
			s.activeMiners = append(s.activeMiners[:i], s.activeMiners[i+1:]...)
			break
		}
	}
}

func (s *server) querySend() {
	// Iterate through active miners to find an available one
	for _, minerId := range s.activeMiners {
		if s.minerJobs[minerId] == nil {
			// Check if there's a job available in the job queue
			if s.jobQueue.Len() > 0 {
				heap.Init(s.jobQueue)
				job := heap.Pop(s.jobQueue).(*bitcoin.Job)
				s.minerJobs[minerId] = []*bitcoin.Job{job}

				// Marshal the job to send to the miner
				jobMessage := bitcoin.NewRequest(*job.Message, job.Lower, job.Upper)
				marshalledJob, err := json.Marshal(jobMessage)
				if err != nil {
					bitcoin.ERROR.Printf("Error marshalling job for miner %d: %s\n", minerId, err)
					// Put the job back into the queue
					heap.Push(s.jobQueue, job)
					delete(s.minerJobs, minerId)
					continue
				}

				// Send the job to the miner
				err = s.lspServer.Write(minerId, marshalledJob)
				if err != nil {
					bitcoin.ERROR.Printf("Error sending job to miner %d: %s\n", minerId, err)
					s.removeActiveMiner(minerId)
					delete(s.minerJobs, minerId)
					// Put the job back into the queue
					heap.Push(s.jobQueue, job)
				} else {
					bitcoin.INFO.Printf("Job sent to miner %d\n", minerId)
				}
			}
		}
	}
}
