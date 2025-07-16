package builder

import (
	"github.com/codecrafters-io/kafka-tester/protocol/encoder"
)

type RequestI interface {
	Encode() []byte
	GetBody() RequestBodyI
	GetHeader() RequestHeaderI
}

type RequestHeaderI interface {
	Encode(pe *encoder.Encoder)
	GetApiKey() int16
	GetApiVersion() int16
	GetCorrelationId() int32
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
