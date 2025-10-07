package main

import (
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

	fmt.Println("Broker listening on port 9092.")
	fmt.Println("This server always sends response with insufficient bytes to accomodate message length.")

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

	// Prepare a malformed response intentionally: insufficient message length to accomodate message length
	response := []byte{0x00, 0x00, 0x00}

	// Send response
	_, err = conn.Write(response)
	if err != nil {
		fmt.Printf("Error writing response: %v\n", err)
		return
	}
}
