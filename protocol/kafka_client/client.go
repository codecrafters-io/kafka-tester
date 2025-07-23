package kafka_client

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"time"

	"github.com/codecrafters-io/kafka-tester/internal/kafka_executable"
	kafka_interface "github.com/codecrafters-io/kafka-tester/protocol/interface"
	"github.com/codecrafters-io/kafka-tester/protocol/utils"
	"github.com/codecrafters-io/tester-utils/logger"
)

type Response struct {
	RawBytes []byte
	Payload  []byte
}

func (r *Response) createFrom(lengthResponse []byte, bodyResponse []byte) Response {
	return Response{
		RawBytes: append(lengthResponse, bodyResponse...),
		Payload:  bodyResponse,
	}
}

// Client represents a single connection to the Kafka broker.
type Client struct {
	id   int32
	addr string
	conn net.Conn
}

// NewClient creates and returns a Client targeting the given host:port address.
// This does not attempt to actually connect, you have to call Open() for that.
func NewClient(addr string) *Client {
	return &Client{id: -1, addr: addr}
}

func (c *Client) ConnectWithRetries(executable *kafka_executable.KafkaExecutable, logger *logger.Logger) error {
	RETRIES := 10
	logger.Debugf("Connecting to broker at: %s", c.addr)

	retries := 0
	var err error
	var conn net.Conn
	for {
		conn, err = net.Dial("tcp", c.addr)
		if err != nil && retries > RETRIES {
			logger.Infof("All retries failed. Exiting.")
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
	logger.Debugf("Connection to broker at %s successful", c.addr)
	c.conn = conn

	return nil
}

func (c *Client) Close() error {
	err := c.conn.Close()
	if err != nil {
		return fmt.Errorf("Failed to close connection to broker at %s: %s", c.addr, err)
	}
	return nil
}

func (c *Client) SendAndReceive(request kafka_interface.RequestI, stageLogger *logger.Logger) (Response, error) {
	header := request.GetHeader()
	apiType := utils.APIKeyToName(header.ApiKey)
	apiVersion := header.ApiVersion
	correlationId := header.CorrelationId
	message := request.Encode()

	stageLogger.Infof("Sending \"%s\" (version: %v) request (Correlation id: %v)", apiType, apiVersion, correlationId)
	stageLogger.Debugf("Hexdump of sent \"%s\" request: \n%v\n", apiType, utils.GetFormattedHexdump(message))

	response := Response{}

	err := c.Send(message)
	if err != nil {
		return response, err
	}

	response, err = c.Receive()
	if err != nil {
		return response, err
	}

	stageLogger.Debugf("Hexdump of received \"%s\" response: \n%v\n", apiType, utils.GetFormattedHexdump(response.RawBytes))

	return response, nil
}

func (c *Client) Send(message []byte) error {
	// Set a deadline for the write operation
	err := c.conn.SetWriteDeadline(time.Now().Add(100 * time.Millisecond))
	if err != nil {
		return fmt.Errorf("failed to set write deadline: %v", err)
	}

	_, err = c.conn.Write(message)

	// Reset the write deadline
	c.conn.SetWriteDeadline(time.Time{})

	if err != nil {
		if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
			return fmt.Errorf("write operation timed out")
		}
		return fmt.Errorf("error writing to connection: %v", err)
	}

	return nil
}

func (c *Client) Receive() (Response, error) {
	response := Response{}

	lengthResponse := make([]byte, 4) // length
	_, err := c.conn.Read(lengthResponse)
	if err != nil {
		return response, err
	}
	length := int32(binary.BigEndian.Uint32(lengthResponse))

	bodyResponse := make([]byte, length)

	// Set a deadline for the read operation
	err = c.conn.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
	if err != nil {
		return response, fmt.Errorf("failed to set read deadline: %v", err)
	}

	_, err = io.ReadFull(c.conn, bodyResponse)

	// Reset the read deadline
	c.conn.SetReadDeadline(time.Time{})

	if err != nil {
		if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
			// If the read timed out, return the partial response we have so far
			// This way we can surface a better error message to help w debugging
			return response.createFrom(lengthResponse, bodyResponse), nil
		}
		return response, fmt.Errorf("error reading from connection: %v", err)
	}

	return response.createFrom(lengthResponse, bodyResponse), nil
}

func (c *Client) ReceiveRaw() ([]byte, error) {
	var buf bytes.Buffer

	// Set a deadline for the read operation
	err := c.conn.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
	if err != nil {
		return nil, fmt.Errorf("failed to set read deadline: %v", err)
	}

	// Use a limited reader to prevent reading indefinitely
	limitedReader := io.LimitReader(c.conn, 1024*1024) // Limit to 1MB, adjust as needed
	_, err = io.Copy(&buf, limitedReader)

	// Reset the read deadline
	c.conn.SetReadDeadline(time.Time{})

	if err != nil && err != io.EOF {
		if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
		} else {
			return nil, fmt.Errorf("error reading from connection: %v", err)
		}
	}

	return buf.Bytes(), nil
}
