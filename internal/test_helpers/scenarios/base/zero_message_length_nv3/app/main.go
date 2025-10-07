package main

import (
	"encoding/binary"
	"fmt"
	"net"
	"os"
)

func main() {
	fmt.Println("Starting Kafka broker on port 9092...")

	// Listen on port 9092
	listener, err := net.Listen("tcp", ":9092")
	if err != nil {
		fmt.Printf("Error starting server: %v\n", err)
		os.Exit(1)
	}
	defer listener.Close()

	fmt.Println("Broker listening on port 9092")

	for {
		// Accept incoming connections
		conn, err := listener.Accept()
		if err != nil {
			fmt.Printf("Error accepting connection: %v\n", err)
			continue
		}

		// Handle connection in a goroutine
		go handleConnection(conn)
	}
}

func handleConnection(conn net.Conn) {
	defer conn.Close()

	// Read the incoming request (we don't need to parse it for this stage)
	buffer := make([]byte, 1024)
	_, err := conn.Read(buffer)
	if err != nil {
		fmt.Printf("Error reading request: %v\n", err)
		return
	}

	// Prepare response: message_size (4 bytes) + correlation_id (4 bytes)
	response := make([]byte, 8)

	// message_size: 0 (any value works according to requirements)
	binary.BigEndian.PutUint32(response[0:4], 0)

	// correlation_id: 7 (as required)
	binary.BigEndian.PutUint32(response[4:8], 7)

	// Send response
	_, err = conn.Write(response)
	if err != nil {
		fmt.Printf("Error writing response: %v\n", err)
		return
	}
}
