package main

import (
	"encoding/json"
	"fmt"
	"github.com/cmu440/bitcoin"
	"math"
	"math/rand"
	"os"
	"time"

	"github.com/cmu440/lsp"
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

	// Write message to client
	err = client.Write(marshalledJoin)
	if err != nil {
		return nil, err
	}

	return client, nil
}

func main() {
	file, err := bitcoin.InitLoggers()
	if err != nil {
		fmt.Printf("Failed to initialize loggers %s.\n", err)
		return
	}
	defer file.Close()

	const numArgs = 2
	if len(os.Args) != numArgs {
		fmt.Printf("Usage: ./%s <hostport>", os.Args[0])
		return
	}

	hostport := os.Args[1]
	miner, err := joinWithServer(hostport)
	if err != nil {
		fmt.Println("Failed to join with server:", err)
		return
	}

	defer miner.Close()

	// mine loop
	// modify the startup commands to specify how many miner threads to use and also therefore the size of the job chunk when starting the server
}
