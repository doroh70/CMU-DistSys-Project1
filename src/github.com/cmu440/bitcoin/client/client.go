package main

import (
	"encoding/json"
	"fmt"
	"github.com/cmu440/bitcoin"
	"math"
	"math/rand"
	"os"
	"strconv"
	"time"

	"github.com/cmu440/lsp"
)

func main() {

	const numArgs = 4
	if len(os.Args) != numArgs {
		fmt.Printf("Usage: ./%s <hostport> <message> <maxNonce>", os.Args[0])
		return
	}

	file, err := bitcoin.InitLoggers()
	if err != nil {
		fmt.Printf("Failed to initialize loggers %s.\n", err)
		return
	}
	defer file.Close()

	hostport := os.Args[1]
	message := os.Args[2]
	maxNonce, err := strconv.ParseUint(os.Args[3], 10, 64)
	if err != nil {
		bitcoin.ERROR.Printf("%s is not a number.\n", os.Args[3])
		return
	}
	seed := rand.NewSource(time.Now().UnixNano())
	isn := rand.New(seed).Intn(int(math.Pow(2, 8)))

	client, err := lsp.NewClient(hostport, isn, lsp.NewParams())
	if err != nil {
		bitcoin.ERROR.Println("Failed to connect to server: ", err)
		return
	}

	defer client.Close()

	// Send client request to server
	request := bitcoin.NewRequest(message, 0, maxNonce)
	marshalledReq, err := json.Marshal(request)
	if err != nil {
		bitcoin.ERROR.Printf("Error marshalling client request : %s\n", err)
		return
	}
	err = client.Write(marshalledReq)
	if err != nil {
		printDisconnected()
		return
	}

	// Process server response
	byteResponse, err := client.Read()
	if err != nil {
		printDisconnected()
		return
	}

	unmarshalledResponse := &bitcoin.Message{}

	err = json.Unmarshal(byteResponse, unmarshalledResponse)
	if err != nil {
		bitcoin.ERROR.Printf("Error unmarshalling server response: %s\n", err)
		return
	}

	if unmarshalledResponse.Type != bitcoin.Result || unmarshalledResponse.Nonce < 0 || unmarshalledResponse.Nonce > maxNonce {
		bitcoin.ERROR.Printf("Invalid server response: %s\n", unmarshalledResponse.String())
		return
	}

	printResult(unmarshalledResponse.Hash, unmarshalledResponse.Nonce)
}

// printResult prints the final result to stdout.
func printResult(hash, nonce uint64) {
	bitcoin.INFO.Println("Result", hash, nonce)
	fmt.Println("Result", hash, nonce)
}

// printDisconnected prints a disconnected message to stdout.
func printDisconnected() {
	bitcoin.ERROR.Println("Disconnected")
	fmt.Println("Disconnected")
}
