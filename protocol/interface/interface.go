package kafka_interface

import (
	"github.com/codecrafters-io/kafka-tester/protocol/api/headers"
	"github.com/codecrafters-io/kafka-tester/protocol/encoder"
)

type RequestI interface {
	Encode() []byte
	GetHeader() headers.RequestHeader
}

type Encodable interface {
	Encode(pe *encoder.Encoder)
}
