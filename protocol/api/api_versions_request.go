package kafkaapi

import (
	"github.com/codecrafters-io/kafka-tester/protocol/encoder"
)

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

func (h *RequestHeader) Encode(enc *encoder.RealEncoder) {
	enc.PutInt16(h.ApiKey)
	enc.PutInt16(h.ApiVersion)
	enc.PutInt32(h.CorrelationId)
	enc.PutString(h.ClientId)
}

type ApiVersionsRequest struct {
	// Version defines the protocol version to use for encode and decode
	Version int16
	// ClientSoftwareName contains the name of the client.
	ClientSoftwareName string
	// ClientSoftwareVersion contains the version of the client.
	ClientSoftwareVersion string
}

func (r *ApiVersionsRequest) Encode(enc *encoder.RealEncoder) {
	if r.Version >= 3 {
		enc.PutEmptyTaggedFieldArray()
		enc.PutCompactString(r.ClientSoftwareName)
		enc.PutCompactString(r.ClientSoftwareVersion)
		enc.PutEmptyTaggedFieldArray()
	}
}
