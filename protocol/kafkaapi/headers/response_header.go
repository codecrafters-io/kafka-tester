package headers

import (
	"fmt"

	"github.com/codecrafters-io/kafka-tester/protocol/decoder"
)

// ResponseHeader is the parallels version of ResponseHeader
type ResponseHeader struct {
	Version       int
	CorrelationId int32
}

func (h *ResponseHeader) Decode(decoder *decoder.Decoder) error {
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

func (h *ResponseHeader) decodeV0(decoder *decoder.Decoder) error {
	correlation_id, err := decoder.ReadInt32("correlation_id")

	if err != nil {
		return err
	}

	h.CorrelationId = correlation_id
	return nil
}

func (h *ResponseHeader) decodeV1(decoder *decoder.Decoder) error {
	err := h.decodeV0(decoder)

	if err != nil {
		return err
	}

	return decoder.ConsumeTagBuffer()
}
