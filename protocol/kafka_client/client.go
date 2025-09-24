package kafka_client

import (
	"bytes"
	"fmt"
	"io"
	"net"
	"time"

	"github.com/codecrafters-io/kafka-tester/internal/kafka_executable"
	"github.com/codecrafters-io/kafka-tester/protocol/utils"
	"github.com/codecrafters-io/tester-utils/logger"
)

type Response struct {
	RawBytes []byte
	Payload  []byte
}

func (r *Response) createFrom(rawBytes []byte) Response {
	var payload []byte

	if len(rawBytes) > 4 {
		payload = make([]byte, len(rawBytes)-4)
		copy(payload, rawBytes[4:])
	}

	return Response{
		RawBytes: rawBytes,
		Payload:  payload,
	}
}

type KafkaClientCallbacks struct {
	BeforeMessageSend           func(bytes []byte, apiName string)
	AfterResponseReceived       func(response Response, apiName string)
	BeforeConnectAttempt        func(addr string)
	AfterConnected              func(addr string)
	AfterConnectRetriesExceeded func(addr string)
}

// Client represents a single connection to the Kafka broker.
type Client struct {
	Addr      string
	Conn      net.Conn
	Callbacks KafkaClientCallbacks
}

// NewFromAddr creates and returns a Client targeting the given host:port address.
// This does not attempt to actually connect, you have to call Open() for that.
func NewFromAddr(addr string, callbacks KafkaClientCallbacks) *Client {
	return &Client{
		Addr:      addr,
		Callbacks: callbacks,
	}
}

func (c *Client) ConnectWithRetries(executable *kafka_executable.KafkaExecutable, logger *logger.Logger) error {
	c.Callbacks.BeforeConnectAttempt(c.Addr)

	maxRetries := 10
	retries := 0
	var err error
	var conn net.Conn

	for {
		conn, err = net.Dial("tcp", c.Addr)

		if err != nil && retries > maxRetries {
			c.Callbacks.AfterConnectRetriesExceeded(c.Addr)
			return err
		}

		if err != nil {
			if executable.HasExited() {
				return fmt.Errorf("Looks like your program has terminated. A Kafka server is expected to be a long-running process.")
			}

			// Don't print errors in the first second
			// ToDo: fixtures fail
			// if retries > 2 {
			// logger.Infof("Failed to connect to broker at %s, retrying in 1s", b.addr)
			// }

			retries += 1
			time.Sleep(1000 * time.Millisecond)
		} else {
			break
		}
	}

	c.Conn = conn
	c.Callbacks.AfterConnected(c.Addr)

	return nil
}

func (c *Client) Close() error {
	err := c.Conn.Close()
	if err != nil {
		return fmt.Errorf("Failed to close connection to broker at %s: %s", c.Addr, err)
	}
	return nil
}

func (c *Client) SendAndReceive(message []byte, apiKey int16, stageLogger *logger.Logger) (Response, error) {
	err := c.Send(message, utils.APIKeyToName(apiKey), stageLogger)

	if err != nil {
		return Response{}, err
	}

	response, err := c.Receive(utils.APIKeyToName(apiKey), stageLogger)

	if err != nil {
		return response, err
	}

	return response, nil
}

func (c *Client) Send(message []byte, apiName string, stageLogger *logger.Logger) error {
	c.Callbacks.BeforeMessageSend(message, apiName)

	// Set a deadline for the write operation
	err := c.Conn.SetWriteDeadline(time.Now().Add(100 * time.Millisecond))
	if err != nil {
		return fmt.Errorf("failed to set write deadline: %v", err)
	}

	_, err = c.Conn.Write(message)

	// Reset the write deadline
	c.Conn.SetWriteDeadline(time.Time{})

	if err != nil {
		if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
			return fmt.Errorf("write operation timed out")
		}

		return fmt.Errorf("error writing to connection: %v", err)
	}

	return nil
}

func (c *Client) Receive(apiName string, stageLogger *logger.Logger) (response Response, err error) {
	defer func() {
		// Reset connection read deadline
		c.Conn.SetReadDeadline(time.Time{})

		// If response was successfully received, invoke callback
		if err == nil {
			c.Callbacks.AfterResponseReceived(response, apiName)
		}
	}()

	var entireMessage bytes.Buffer

	// We wait for the initial bytes outside of the loop because in some stages, Kafka is not guaranteed to send response within the
	// usual deadline used below (100MS)
	lengthResponse := make([]byte, 4)
	_, err = c.Conn.Read(lengthResponse)
	if err != nil {
		return response, err
	}

	entireMessage.Write(lengthResponse)

	// Set a time of 100MS for connection deadline
	err = c.Conn.SetReadDeadline(time.Now().Add(100 * time.Millisecond))

	for {
		tempBuffer := make([]byte, 4096)
		n, err := c.Conn.Read(tempBuffer)

		if n > 0 {
			entireMessage.Write(tempBuffer[:n])
		}

		if err != nil {
			// All message has been read
			if err == io.EOF {
				return response.createFrom(entireMessage.Bytes()), nil
			}

			// Connection deadline has been reached, we have read all the response there is.
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {

				// In tcp stream, we cannot guarantee when all the message will be received.
				// We could have stopped at reading 'messagelgnth' bytes like the previous approach
				// But we also need to check for cases where the user's implementation may send extra bytes
				// So, we wait for 100MS and read everything that's available.
				// Any errors will be surfaced later by assertions.
				return response.createFrom(entireMessage.Bytes()), nil
			}
			return response, fmt.Errorf("error reading from connection: %v", err)
		}
	}
}
