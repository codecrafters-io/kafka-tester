package builder

import (
	headers "github.com/codecrafters-io/kafka-tester/protocol/api/headers"
	"github.com/codecrafters-io/kafka-tester/protocol/encoder"
)

type RequestI interface {
	Encode() []byte
	GetBody() RequestBodyI
	GetHeader() headers.RequestHeader
}

type RequestHeaderI interface {
	Encode(pe *encoder.Encoder)
}

type RequestBodyI interface {
	Encode(pe *encoder.Encoder)
}

// TODO
//type Assertion struct {
// actualResponse kafkaResponseI
// expectedResponse kafkaResponseI
//	logger *logger.Logger
//	err    error
//}

type Assertion interface {
	Run() error
}
