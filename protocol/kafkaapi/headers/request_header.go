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

// encode v2
func (h RequestHeader) encode(enc *encoder.Encoder) {
	enc.WriteInt16(h.ApiKey)
	enc.WriteInt16(h.ApiVersion)
	enc.WriteInt32(h.CorrelationId)
	enc.WriteString(h.ClientId)
	enc.WriteEmptyTagBuffer()
}

func (h RequestHeader) Encode() []byte {
	encoder := encoder.NewEncoder(make([]byte, 4096))

	h.encode(encoder)

	return encoder.EncodedBytes()
}
