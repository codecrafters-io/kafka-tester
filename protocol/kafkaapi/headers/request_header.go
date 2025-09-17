package headers

import (
	"github.com/codecrafters-io/kafka-tester/internal/field_encoder"
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
	ClientId value.KafkaString
}

func (h RequestHeader) Encode(encoder *field_encoder.FieldEncoder) {
	encoder.PushPathContext("Header")
	defer encoder.PopPathContext()
	encoder.WriteInt16Field("APIKey", h.ApiKey)
	encoder.WriteInt16Field("APIVersion", h.ApiVersion)
	encoder.WriteInt32Field("CorrelationID", h.CorrelationId)
	encoder.WriteStringField("ClientID", h.ClientId)
	encoder.WriteEmptyTagBuffer()
}
