package headers

import (
	"fmt"

	"github.com/codecrafters-io/kafka-tester/protocol/instrumented_decoder"
	"github.com/codecrafters-io/kafka-tester/protocol/value"
)

// ResponseHeader is the parallels version of ResponseHeader
type ResponseHeader struct {
	Version       int
	CorrelationId value.Int32
}

func (h *ResponseHeader) Decode(decoder *instrumented_decoder.InstrumentedDecoder) error {
	decoder.BeginSubSection("ResponseHeader")
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

func (h *ResponseHeader) decodeV0(decoder *instrumented_decoder.InstrumentedDecoder) (err error) {
	h.CorrelationId, err = decoder.ReadInt32("CorrelationID")
	return err
}

func (h *ResponseHeader) decodeV1(decoder *instrumented_decoder.InstrumentedDecoder) error {
	if err := h.decodeV0(decoder); err != nil {
		return err
	}

	return decoder.ConsumeTagBuffer()
}
