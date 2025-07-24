package kafka_interface

import (
	headers "github.com/codecrafters-io/kafka-tester/protocol/api/headers"
	"github.com/codecrafters-io/kafka-tester/protocol/encoder"
)

type RequestI interface {
	Encode() []byte
	GetHeader() headers.RequestHeader
	GetBody() RequestBodyI
}

type RequestBodyI interface {
	Encode(pe *encoder.Encoder)
}
