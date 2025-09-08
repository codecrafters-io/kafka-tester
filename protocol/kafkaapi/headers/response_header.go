package headers

import (
	"github.com/codecrafters-io/kafka-tester/protocol/value"
)

type ResponseHeader struct {
	Version       int
	CorrelationId value.Int32
}
