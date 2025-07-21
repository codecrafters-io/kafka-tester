package kafkaapi

import (
	"github.com/codecrafters-io/kafka-tester/protocol"
	"github.com/codecrafters-io/kafka-tester/protocol/decoder"
	"github.com/codecrafters-io/kafka-tester/protocol/errors"
	"github.com/codecrafters-io/tester-utils/logger"
)

// ResponseHeader defines the header for a Kafka response
type ResponseHeader struct {
	CorrelationId int32
}

// TODO: Add version to struct, and based on that decide which internal function to call
// Expose only Decode()

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
