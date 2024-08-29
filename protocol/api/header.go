package kafkaapi

import (
	"fmt"

	"github.com/codecrafters-io/kafka-tester/protocol/decoder"
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

func (h *RequestHeader) EncodeV1(enc *encoder.RealEncoder) {
	enc.PutInt16(h.ApiKey)
	enc.PutInt16(h.ApiVersion)
	enc.PutInt32(h.CorrelationId)
	enc.PutString(h.ClientId)
}

func (h *RequestHeader) EncodeV2(enc *encoder.RealEncoder) {
	enc.PutInt16(h.ApiKey)
	enc.PutInt16(h.ApiVersion)
	enc.PutInt32(h.CorrelationId)
	enc.PutString(h.ClientId)
	enc.PutEmptyTaggedFieldArray()
}

// ResponseHeader defines the header for a Kafka response
type ResponseHeader struct {
	CorrelationId int32
}

func (h *ResponseHeader) DecodeV0(decoder *decoder.RealDecoder) error {
	correlation_id, err := decoder.GetInt32()
	if err != nil {
		return fmt.Errorf("failed to decode correlation_id in response header: %w", err)
	}
	h.CorrelationId = correlation_id
	return nil
}

func (h *ResponseHeader) DecodeV1(decoder *decoder.RealDecoder) error {
	correlation_id, err := decoder.GetInt32()
	if err != nil {
		return fmt.Errorf("failed to decode correlation_id in response header: %w", err)
	}
	h.CorrelationId = correlation_id

	decoder.GetEmptyTaggedFieldArray()

	return nil
}
