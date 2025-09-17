package headers

import (
	"github.com/codecrafters-io/kafka-tester/internal/field_encoder"
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

func (h RequestHeader) Encode(encoder *field_encoder.FieldEncoder) {
	encoder.PushPathContext("Header")
	defer encoder.PopPathContext()
	encoder.WriteInt16("APIKey", h.ApiKey)
	encoder.WriteInt16("APIVersion", h.ApiVersion)
	encoder.WriteInt32("CorrelationID", h.CorrelationId)
	encoder.WriteString("ClientID", h.ClientId)
	encoder.WriteEmptyTagBuffer()
}
