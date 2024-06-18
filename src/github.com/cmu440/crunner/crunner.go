// Implementation of an echo client based on LSP.

package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"strconv"
	"time"

	"github.com/cmu440/lsp"
	"github.com/cmu440/lspnet"
)

var (
	port               = flag.Int("port", 9999, "server port number")
	host               = flag.String("host", "localhost", "server host address")
	readDrop           = flag.Int("rdrop", 0, "network read drop percent")
	writeDrop          = flag.Int("wdrop", 0, "network write drop percent")
	epochLimit         = flag.Int("elim", lsp.DefaultEpochLimit, "epoch limit")
	epochMillis        = flag.Int("ems", lsp.DefaultEpochMillis, "epoch duration (ms)")
	windowSize         = flag.Int("wsize", lsp.DefaultWindowSize, "window size")
	maxUnackedMessages = flag.Int("maxUnackMessages", lsp.DefaultMaxUnackedMessages, "max unacknowledged messages")
	maxBackoff         = flag.Int("maxbackoff", lsp.DefaultMaxBackOffInterval, "maximum interval epoch")
	showLogs           = flag.Bool("v", false, "show crunner logs")
)

func init() {
	// Display time, file, and line number in log messages.
	log.SetFlags(log.Lmicroseconds | log.Lshortfile)
}

func main() {
	flag.Parse()
	if !*showLogs {
		log.SetOutput(ioutil.Discard)
	} else {
		lspnet.EnableDebugLogs(true)
	}
	lspnet.SetClientReadDropPercent(*readDrop)
	lspnet.SetClientWriteDropPercent(*writeDrop)
	params := &lsp.Params{
		EpochLimit:         *epochLimit,
		EpochMillis:        *epochMillis,
		WindowSize:         *windowSize,
		MaxBackOffInterval: *maxBackoff,
		MaxUnackedMessages: *maxUnackedMessages,
	}
	hostport := lspnet.JoinHostPort(*host, strconv.Itoa(*port))
	fmt.Printf("Connecting to server at '%s'...\n", hostport)
	cli, err := lsp.NewClient(hostport, 0, params)
	if err != nil {
		fmt.Printf("Failed to connect to server at %s: %s\n", hostport, err)
		return
	}
	runClient(cli)
}

func runClient(cli lsp.Client) {
	defer fmt.Println("Exiting...")

	number := 1 // Start from 1

	for {
		// Convert the number to a byte slice
		data := []byte(strconv.Itoa(number))

		// Send message to server.
		if err := cli.Write(data); err != nil {
			fmt.Printf("Client %d failed to write to server: %s\n", cli.ConnID(), err)
			return
		}
		log.Printf("Client %d wrote '%d' to server\n", cli.ConnID(), number)

		// Read message from server.
		payload, err := cli.Read()
		if err != nil {
			fmt.Printf("Client %d failed to read from server: %s\n", cli.ConnID(), err)
			return
		}
		fmt.Printf("Server: %s\n", string(payload))

		// Increment the number
		number++

		// Optional: Add a short delay to avoid flooding the server too quickly
		time.Sleep(1 * time.Second) // Adjust the duration as needed
	}
}
