package kafkaapi

import (
	"github.com/codecrafters-io/kafka-tester/protocol"
	"github.com/codecrafters-io/kafka-tester/protocol/decoder"
	"github.com/codecrafters-io/kafka-tester/protocol/encoder"
	"github.com/codecrafters-io/kafka-tester/protocol/errors"
	"github.com/codecrafters-io/tester-utils/logger"
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

func (h RequestHeader) GetApiKey() int16 {
	return h.ApiKey
}

func (h RequestHeader) GetApiVersion() int16 {
	return h.ApiVersion
}

func (h RequestHeader) GetCorrelationId() int32 {
	return h.CorrelationId
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

// ResponseHeader defines the header for a Kafka response
type ResponseHeader struct {
	CorrelationId int32
}

func (h *ResponseHeader) DecodeV0(decoder *decoder.Decoder, logger *logger.Logger, indentation int) error {
	correlation_id, err := decoder.GetInt32()
	if err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return decodingErr.WithAddedContext("correlation_id")
		}
		return err
	}
	h.CorrelationId = correlation_id
	protocol.LogWithIndentation(logger, indentation, "- .correlation_id (%d)", correlation_id)

	return nil
}

func (h *ResponseHeader) DecodeV1(decoder *decoder.Decoder, logger *logger.Logger, indentation int) error {
	correlation_id, err := decoder.GetInt32()
	if err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return decodingErr.WithAddedContext("correlation_id")
		}
		return err
	}
	h.CorrelationId = correlation_id
	protocol.LogWithIndentation(logger, indentation, "- .correlation_id (%d)", correlation_id)

	_, err = decoder.GetEmptyTaggedFieldArray()
	if err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return decodingErr.WithAddedContext("TAG_BUFFER")
		}
		return err
	}
	protocol.LogWithIndentation(logger, indentation, "- .TAG_BUFFER")

	return nil
}
