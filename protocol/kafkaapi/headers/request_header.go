package headers

import (
	"github.com/codecrafters-io/kafka-tester/protocol/value"
)

// RequestHeader defines the header for a Kafka request
type RequestHeader struct {
	// ApiKey defines the API key for the request
	ApiKey value.Int16
	// ApiVersion defines the API version for the request
	ApiVersion value.Int16
	// CorrelationId defines the correlation ID for the request
	CorrelationId value.Int32
	// ClientId defines the client ID for the request
	ClientId value.String
}
