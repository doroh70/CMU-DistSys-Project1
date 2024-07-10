package main

import (
	"encoding/json"
	"fmt"
	"github.com/cmu440/bitcoin"
	"github.com/cmu440/lsp"
	"math"
	"math/rand"
	"os"
	"time"
)

// Attempt to connect miner as a client to the server.
func joinWithServer(hostport string) (lsp.Client, error) {
	// You will need this for randomized isn
	seed := rand.NewSource(time.Now().UnixNano())
	isn := rand.New(seed).Intn(int(math.Pow(2, 8)))

	client, err := lsp.NewClient(hostport, isn, lsp.NewParams())

	if err != nil {
		bitcoin.ERROR.Println("Failed to connect to server: ", err)
		return nil, err
	}

	// Send join message to server
	joinMsg := bitcoin.NewJoin()
	marshalledJoin, err := json.Marshal(joinMsg)
	if err != nil {
		bitcoin.ERROR.Printf("Error marshalling miner join message : %s\n", err)
		return nil, err
	}

	err = client.Write(marshalledJoin)
	if err != nil {
		return nil, err
	}

	return client, nil
}

func main() {
	const numArgs = 2
	if len(os.Args) != numArgs {
		fmt.Printf("Usage: ./%s <hostport>", os.Args[0])
		return
	}

	hostport := os.Args[1]

	file, err := bitcoin.InitLoggers(fmt.Sprintf("miner%s", hostport))
	if err != nil {
		fmt.Printf("Failed to initialize loggers %s.\n", err)
		return
	}
	defer file.Close()

	miner, err := joinWithServer(hostport)
	if err != nil {
		bitcoin.ERROR.Println("Failed to join with server:", err)
		return
	}
	bitcoin.INFO.Println("Joined with server!")

	defer miner.Close()

	// mine loop
	for {
		// Get batch job
		byteRequest, err := miner.Read()
		if err != nil {
			bitcoin.ERROR.Println("Server connection lost:", err)
			return
		}
		bitcoin.INFO.Println("Received a batch job from the server")

		unmarshalledRequest := &bitcoin.Message{}
		err = json.Unmarshal(byteRequest, unmarshalledRequest)
		if err != nil {
			bitcoin.ERROR.Println("Failed to unmarshal batch job:", err)
			// We would like to send an error signal to the server that we had some error processing job, so the miner can stay working. Currently not in documentation, will ignore for now.
			return
		}

		// Obtain response
		response := MineSmallestNonce(unmarshalledRequest)
		if response == nil {
			bitcoin.ERROR.Println("Invalid batch job:", unmarshalledRequest.String())
			return
		}
		bitcoin.INFO.Printf("Mined smallest nonce: %v with hash %v\n", response.Nonce, response.Hash)

		//Send to server
		marshalledResp, _ := json.Marshal(response)
		err = miner.Write(marshalledResp)
		if err != nil {
			bitcoin.ERROR.Println("Failed to send response to server:", err)
			return
		}
		bitcoin.INFO.Println("Sent mined result to server")
	}
}

// MineSmallestNonce launches a go routine to mine each 10k nonce range,
// collects results from each routine and returns a result message with the least hash
func MineSmallestNonce(message *bitcoin.Message) *bitcoin.Message {

	var NONCERANGE uint64 = 10000

	if message.Lower > message.Upper {
		return nil
	}

	numRoutines := (message.Upper - message.Lower + 1) / NONCERANGE
	if (message.Upper-message.Lower+1)%NONCERANGE > 0 {
		numRoutines = numRoutines + 1
	}

	resultChannel := make(chan *bitcoin.Message, numRoutines)

	for i := uint64(0); i < numRoutines; i++ {
		// Calculate the lower and upper bounds for this goroutine
		lower := message.Lower + i*NONCERANGE
		upper := lower + NONCERANGE - 1
		if upper > message.Upper {
			upper = message.Upper
		}

		// Launch the miner goroutine
		go miner(&message.Data, upper, lower, resultChannel)
	}

	// Collect results from all goroutines and find the smallest hash
	var bestResult *bitcoin.Message = nil
	for i := uint64(0); i < numRoutines; i++ {
		result := <-resultChannel
		if bestResult == nil || result.Hash < bestResult.Hash {
			bestResult = result
		}
	}

	return bestResult
}

func miner(message *string, upper uint64, lower uint64, resultChannel chan<- *bitcoin.Message) {

	var finResult uint64 = math.MaxUint64
	var finMessage = bitcoin.NewResult(0, 0)
	// loop through range and mine, return least hash and nonce result message
	for i := lower; i <= upper; i++ {
		result := bitcoin.Hash(*message, i)
		if result < finResult {
			finResult = result
			finMessage.Hash = result
			finMessage.Nonce = i
		}
	}

	resultChannel <- finMessage
}
