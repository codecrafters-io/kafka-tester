package main

import (
	"bufio"
	"fmt"
	"net"
	"os"
)

func main() {
	fmt.Println("Logs from your program will appear here!")
	fmt.Println("This server will always assume that a fetch request will be sent to it.")
	fmt.Println("This server will always assume that the topic sent in the request exists.")
	fmt.Println("It will deliberately tamper the CRC32 bytes of recordbatch.")

	// Start server
	listener, err := net.Listen("tcp", "0.0.0.0:9092")
	if err != nil {
		fmt.Println("Failed to bind to port 9092")
		os.Exit(1)
	}
	defer listener.Close()

	fmt.Println("Listening on port: 9092")

	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Printf("Error accepting connection: %v\n", err)
			os.Exit(1)
		}
		go handleConnection(conn)
	}
}

func handleConnection(conn net.Conn) {
	defer conn.Close()

	reader := bufio.NewReader(conn)

	for {
		request, err := ReadRequest(reader)
		if err != nil {
			break
		}

		// Find and read log file and get decoded batches
		batches, err := FindAndReadLogFile(request.TopicID, request.PartitionID)
		if err != nil {
			fmt.Printf("Error reading log file: %v\n", err)
			continue
		}

		// Send fetch response
		responseBytes := SendFetchResponse(request, batches)

		_, err = conn.Write(responseBytes)
		if err != nil {
			fmt.Printf("Error writing response: %v\n", err)
			break
		}
	}
}
