package headers

import (
	"fmt"

	// TODO[PaulRefactor]: Avoid the import of value_storing_decoder from protocol?
	"github.com/codecrafters-io/kafka-tester/internal/value_storing_decoder"
	"github.com/codecrafters-io/kafka-tester/protocol/value"
)

// ResponseHeader is the parallels version of ResponseHeader
type ResponseHeader struct {
	Version       int
	CorrelationId value.Int32
}

func (h *ResponseHeader) Decode(decoder *value_storing_decoder.ValueStoringDecoder) error {
	decoder.PushLocatorSegment("Header")
	defer decoder.PopLocatorSegment()

	switch h.Version {
	case 0:
		return h.decodeV0(decoder)
	case 1:
		return h.decodeV1(decoder)
	default:
		panic(fmt.Sprintf("CodeCrafters Internal Error: Unsupported response header version: %d", h.Version))
	}
}

func (h *ResponseHeader) decodeV0(decoder *value_storing_decoder.ValueStoringDecoder) (err error) {
	h.CorrelationId, err = decoder.ReadInt32("CorrelationID")
	return err
}

func (h *ResponseHeader) decodeV1(decoder *value_storing_decoder.ValueStoringDecoder) error {
	if err := h.decodeV0(decoder); err != nil {
		return err
	}

	return decoder.ConsumeTagBuffer()
}
