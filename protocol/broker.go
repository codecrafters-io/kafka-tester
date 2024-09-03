package protocol

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"time"

	"github.com/codecrafters-io/kafka-tester/internal/kafka_executable"
	"github.com/codecrafters-io/tester-utils/logger"
)

// Broker represents a single Kafka broker connection. All operations on this object are entirely concurrency-safe.
type Broker struct {
	id   int32
	addr string
	conn net.Conn
}

// NewBroker creates and returns a Broker targeting the given host:port address.
// This does not attempt to actually connect, you have to call Open() for that.
func NewBroker(addr string) *Broker {
	return &Broker{id: -1, addr: addr}
}

func (b *Broker) Connect() error {
	RETRIES := 10

	retries := 0
	var err error
	var conn net.Conn
	for {
		conn, err = net.Dial("tcp", b.addr)
		if err != nil && retries > RETRIES {
			return err
		}
		if err != nil {
			retries += 1
			time.Sleep(1000 * time.Millisecond)
		} else {
			break
		}
	}
	b.conn = conn

	return nil
}

func (b *Broker) ConnectWithRetries(executable *kafka_executable.KafkaExecutable, logger *logger.Logger) error {
	RETRIES := 10
	logger.Debugf("Connecting to broker at: %s", b.addr)

	retries := 0
	var err error
	var conn net.Conn
	for {
		conn, err = net.Dial("tcp", b.addr)
		if err != nil && retries > RETRIES {
			logger.Infof("All retries failed. Exiting.")
			return err
		}

		if err != nil {
			if executable.HasExited() {
				return fmt.Errorf("Looks like your program has terminated. A Kafka server is expected to be a long-running process.")
			}

			// Don't print errors in the first second
			if retries > 2 {
				logger.Infof("Failed to connect to broker at %s, retrying in 1s", b.addr)
			}

			retries += 1
			time.Sleep(1000 * time.Millisecond)
		} else {
			break
		}
	}
	logger.Debugf("Connection to broker at %s successful", b.addr)
	b.conn = conn

	return nil
}

func (b *Broker) Close() error {
	err := b.conn.Close()
	if err != nil {
		return fmt.Errorf("Failed to close connection to broker at %s: %s", b.addr, err)
	}
	return nil
}

func (b *Broker) SendAndReceive(request []byte) ([]byte, error) {
	err := b.Send(request)
	if err != nil {
		return nil, err
	}

	response, err := b.receive()
	if err != nil {
		return nil, err
	}

	return response, nil
}

func (b *Broker) Send(message []byte) error {
	_, err := b.conn.Write(message) // ToDo possible errors ?
	return err
}

func (b *Broker) receive() ([]byte, error) {
	response := make([]byte, 4) // length
	_, err := b.conn.Read(response)
	if err != nil {
		return nil, err
	}
	length := int32(binary.BigEndian.Uint32(response))
	// fmt.Printf("Length of response: %d\n", length)

	// ToDo ReadUntilOrTimeout
	time.Sleep(1000 * time.Millisecond) // ToDo: Remove this ? How ?
	response = make([]byte, length)
	_, err = b.conn.Read(response)
	if err != nil {
		return nil, err
	}

	return response, nil
}

func (b *Broker) ReceiveRaw() ([]byte, error) {
	// We don't read the length of the response first,
	// We read the entire response first and then decode it
	var buf bytes.Buffer
	_, err := io.Copy(&buf, b.conn)
	if err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}
