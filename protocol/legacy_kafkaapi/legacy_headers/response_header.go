package legacy_headers

import (
	"fmt"

	"github.com/codecrafters-io/kafka-tester/protocol"
	"github.com/codecrafters-io/kafka-tester/protocol/legacy_decoder"
	"github.com/codecrafters-io/kafka-tester/protocol/legacy_errors"
	"github.com/codecrafters-io/tester-utils/logger"
)

// ResponseHeader defines the header for a Kafka response
type ResponseHeader struct {
	Version       int
	CorrelationId int32
}

func (h *ResponseHeader) Decode(decoder *legacy_decoder.Decoder, logger *logger.Logger, indentation int) error {
	switch h.Version {
	case 0:
		return h.decodeV0(decoder, logger, indentation)
	case 1:
		return h.decodeV1(decoder, logger, indentation)
	default:
		panic(fmt.Sprintf("CodeCrafters Internal Error: Unsupported response header version: %d", h.Version))
	}
}

func (h *ResponseHeader) decodeV0(decoder *legacy_decoder.Decoder, logger *logger.Logger, indentation int) error {
	correlation_id, err := decoder.GetInt32()
	if err != nil {
		if decodingErr, ok := err.(*legacy_errors.PacketDecodingError); ok {
			return decodingErr.WithAddedContext("correlation_id")
		}
		return err
	}
	h.CorrelationId = correlation_id
	protocol.LogWithIndentation(logger, indentation, "- .correlation_id (%d)", correlation_id)

	return nil
}

func (h *ResponseHeader) decodeV1(decoder *legacy_decoder.Decoder, logger *logger.Logger, indentation int) error {
	err := h.decodeV0(decoder, logger, indentation)
	if err != nil {
		return err
	}

	_, err = decoder.GetEmptyTaggedFieldArray()
	if err != nil {
		if decodingErr, ok := err.(*legacy_errors.PacketDecodingError); ok {
			return decodingErr.WithAddedContext("TAG_BUFFER")
		}
		return err
	}
	protocol.LogWithIndentation(logger, indentation, "- .TAG_BUFFER")

	return nil
}
