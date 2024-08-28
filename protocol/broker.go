package protocol

import (
	"encoding/binary"
	"fmt"
	"net"
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
	conn, err := net.Dial("tcp", b.addr)
	if err != nil {
		fmt.Printf("Failed to connect to broker %s: %s\n", b.addr, err)
		return err
	}
	b.conn = conn

	return nil
}

func (b *Broker) Close() error {
	return b.conn.Close()
}

func (b *Broker) SendAndReceive(request []byte) ([]byte, error) {
	err := b.send(request)
	if err != nil {
		return nil, err
	}

	response, err := b.receive()
	if err != nil {
		return nil, err
	}

	return response, nil
}

// b.lock must be held by caller
func (b *Broker) send(message []byte) error {
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

	response = make([]byte, length)
	_, err = b.conn.Read(response)
	if err != nil {
		return nil, err
	}

	return response, nil
}
