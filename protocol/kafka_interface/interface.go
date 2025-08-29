package kafka_interface

import (
	"github.com/codecrafters-io/kafka-tester/protocol/encoder"
	"github.com/codecrafters-io/kafka-tester/protocol/kafkaapi/headers"
)

type RequestI interface {
	Encode() []byte
	GetHeader() headers.RequestHeader
}

type Encodable interface {
	Encode(pe *encoder.Encoder)
}
