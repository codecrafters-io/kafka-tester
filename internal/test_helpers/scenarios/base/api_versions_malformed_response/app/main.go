package main

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"os"
)

func main() {
	fmt.Println("Logs from your program will appear here!")

	// Listen on port 9092
	listener, err := net.Listen("tcp", "0.0.0.0:9092")
	if err != nil {
		fmt.Printf("Failed to bind to port 9092\n")
		os.Exit(1)
	}
	defer listener.Close()

	fmt.Println("This server responds with a malformed response. See 'INTENTIONAL ERROR' comments in response.go")

	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Printf("Error accepting connection: %v\n", err)
			continue
		}

		go handleConnection(conn)
	}
}

func handleConnection(conn net.Conn) {
	defer conn.Close()

	reader := bufio.NewReader(conn)

	request, err := ParseRequest(reader)
	if err != nil {
		if err == io.EOF {
			return
		}
		fmt.Printf("Error reading request: %v\n", err)
		return
	}

	response := ProcessRequest(request)
	_, err = conn.Write(response.Encode())

	if err != nil {
		fmt.Printf("Error sending response: %v\n", err)
		return
	}
}
