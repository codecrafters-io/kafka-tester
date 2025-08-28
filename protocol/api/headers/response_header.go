package headers

import (
	"fmt"

	"github.com/codecrafters-io/kafka-tester/protocol"
	"github.com/codecrafters-io/kafka-tester/protocol/decoder"
	"github.com/codecrafters-io/kafka-tester/protocol/errors"
	"github.com/codecrafters-io/tester-utils/logger"
)

// ResponseHeader defines the header for a Kafka response
type ResponseHeader struct {
	Version       int
	CorrelationId int32
}

func (h *ResponseHeader) Decode(decoder *decoder.Decoder, logger *logger.Logger, indentation int) error {
	switch h.Version {
	case 0:
		return h.decodeV0(decoder, logger, indentation)
	case 1:
		return h.decodeV1(decoder, logger, indentation)
	default:
		panic(fmt.Sprintf("CodeCrafters Internal Error: Unsupported response header version: %d", h.Version))
	}
}

func (h *ResponseHeader) decodeV0(decoder *decoder.Decoder, logger *logger.Logger, indentation int) error {
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

func (h *ResponseHeader) decodeV1(decoder *decoder.Decoder, logger *logger.Logger, indentation int) error {
	err := h.decodeV0(decoder, logger, indentation)
	if err != nil {
		return err
	}

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

/***************** Updated Interface ****************/

// ResponseHeader__Updated is the parallels version of ResponseHeader
type ResponseHeader__Updated struct {
	Version       int
	CorrelationId int32
}

func (h *ResponseHeader__Updated) Decode(decoder *decoder.Decoder) error {
	decoder.BeginSubSection("response_header")
	defer decoder.EndCurrentSubSection()
	switch h.Version {
	case 0:
		return h.decodeV0(decoder)
	case 1:
		return h.decodeV1(decoder)
	default:
		panic(fmt.Sprintf("CodeCrafters Internal Error: Unsupported response header version: %d", h.Version))
	}
}

func (h *ResponseHeader__Updated) decodeV0(decoder *decoder.Decoder) error {
	correlation_id, err := decoder.GetInt32_Updated("correlation_id")

	if err != nil {
		return err
	}

	h.CorrelationId = correlation_id
	return nil
}

func (h *ResponseHeader__Updated) decodeV1(decoder *decoder.Decoder) error {
	err := h.decodeV0(decoder)

	if err != nil {
		return err
	}

	return decoder.ConsumeTagBuffer()
}
