package main

import (
	"encoding/json"
	"fmt"
	"github.com/cmu440/bitcoin"
	"log"
	"math"
	"math/rand"
	"os"
	"strconv"
	"time"

	"github.com/cmu440/lsp"
)

var (
	INFO  *log.Logger
	WARN  *log.Logger
	ERROR *log.Logger
)

func main() {
	// You may need a logger for debug purpose
	const (
		name = "clientLog.txt"
		flag = os.O_RDWR | os.O_CREATE
		perm = os.FileMode(0666)
	)

	const numArgs = 4
	if len(os.Args) != numArgs {
		fmt.Printf("Usage: ./%s <hostport> <message> <maxNonce>", os.Args[0])
		return
	}

	file, err := os.OpenFile(name, flag, perm)
	if err != nil {
		return
	}
	defer file.Close()

	INFO = log.New(file, "INFO: ", log.Ldate|log.Ltime|log.Lshortfile)
	WARN = log.New(file, "WARN: ", log.Ldate|log.Ltime|log.Lshortfile)
	ERROR = log.New(file, "ERROR: ", log.Ldate|log.Ltime|log.Lshortfile)

	hostport := os.Args[1]
	message := os.Args[2]
	maxNonce, err := strconv.ParseUint(os.Args[3], 10, 64)
	if err != nil {
		ERROR.Printf("%s is not a number.\n", os.Args[3])
		return
	}
	seed := rand.NewSource(time.Now().UnixNano())
	isn := rand.New(seed).Intn(int(math.Pow(2, 8)))

	client, err := lsp.NewClient(hostport, isn, lsp.NewParams())
	if err != nil {
		ERROR.Println("Failed to connect to server: ", err)
		return
	}

	defer client.Close()

	// Send client request to server
	request := bitcoin.NewRequest(message, 0, maxNonce)
	marshalledReq, err := json.Marshal(request)
	if err != nil {
		ERROR.Printf("Error marshalling client request : %s\n", err)
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
		ERROR.Printf("Error unmarshalling server response: %s\n", err)
		return
	}

	printResult(unmarshalledResponse.Hash, unmarshalledResponse.Nonce)
}

// printResult prints the final result to stdout.
func printResult(hash, nonce uint64) {
	INFO.Println("Result", hash, nonce)
	fmt.Println("Result", hash, nonce)
}

// printDisconnected prints a disconnected message to stdout.
func printDisconnected() {
	ERROR.Println("Disconnected")
	fmt.Println("Disconnected")
}
