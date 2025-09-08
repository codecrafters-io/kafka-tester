package headers

import (
	"fmt"

	// TODO[PaulRefactor]: Avoid the import of value_storing_decoder from protocol?
	"github.com/codecrafters-io/kafka-tester/internal/field_decoder"
	"github.com/codecrafters-io/kafka-tester/protocol/value"
)

type ResponseHeader struct {
	Version       int
	CorrelationId value.Int32
}

func (h *ResponseHeader) Decode(decoder *field_decoder.FieldDecoder) error {
	decoder.PushPathSegment("Header")
	defer decoder.PopPathSegment()

	switch h.Version {
	case 0:
		return h.decodeV0(decoder)
	case 1:
		return h.decodeV1(decoder)
	default:
		panic(fmt.Sprintf("CodeCrafters Internal Error: Unsupported response header version: %d", h.Version))
	}
}

func (h *ResponseHeader) decodeV0(decoder *field_decoder.FieldDecoder) (err error) {
	h.CorrelationId, err = decoder.ReadInt32("CorrelationID")
	return err
}

func (h *ResponseHeader) decodeV1(decoder *field_decoder.FieldDecoder) error {
	if err := h.decodeV0(decoder); err != nil {
		return err
	}

	return decoder.ConsumeTagBuffer()
}
