package headers

import (
	"github.com/codecrafters-io/kafka-tester/protocol/encoder"
)

// RequestHeader defines the header for a Kafka request
type RequestHeader struct {
	// ApiKey defines the API key for the request
	ApiKey int16
	// ApiVersion defines the API version for the request
	ApiVersion int16
	// CorrelationId defines the correlation ID for the request
	CorrelationId int32
	// ClientId defines the client ID for the request
	ClientId string
}

func (h RequestHeader) Encode() []byte {
	encoder := encoder.NewEncoder()
	encoder.WriteInt16(h.ApiKey)
	encoder.WriteInt16(h.ApiVersion)
	encoder.WriteInt32(h.CorrelationId)
	encoder.WriteString(h.ClientId)
	encoder.WriteEmptyTagBuffer()
	return encoder.Bytes()
}
