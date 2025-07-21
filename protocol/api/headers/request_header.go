package kafkaapi

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

func (h RequestHeader) Encode(enc *encoder.Encoder) {
	h.encodeV2(enc)
}

func (h RequestHeader) encodeV2(enc *encoder.Encoder) {
	enc.PutInt16(h.ApiKey)
	enc.PutInt16(h.ApiVersion)
	enc.PutInt32(h.CorrelationId)
	enc.PutString(h.ClientId)
	enc.PutEmptyTaggedFieldArray()
}
