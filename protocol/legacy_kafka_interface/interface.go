package legacy_kafka_interface

import (
	"github.com/codecrafters-io/kafka-tester/protocol/legacy_encoder"
	"github.com/codecrafters-io/kafka-tester/protocol/legacy_kafkaapi/legacy_headers"
)

type RequestI interface {
	Encode() []byte
	GetHeader() legacy_headers.RequestHeader
}

type Encodable interface {
	Encode(pe *legacy_encoder.Encoder)
}
