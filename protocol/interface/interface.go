package kafka_interface

import "github.com/codecrafters-io/kafka-tester/protocol/api/headers"

type RequestI interface {
	Encode() []byte
	GetHeader() headers.RequestHeader
}
