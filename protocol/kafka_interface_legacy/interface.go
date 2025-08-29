package kafka_interface_legacy

import (
	"github.com/codecrafters-io/kafka-tester/protocol/encoder"
	"github.com/codecrafters-io/kafka-tester/protocol/kafkaapi_legacy/headers_legacy"
)

type RequestI interface {
	Encode() []byte
	GetHeader() headers_legacy.RequestHeader
}

type Encodable interface {
	Encode(pe *encoder.Encoder)
}
