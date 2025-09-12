package response_decoders

import (
	"fmt"

	"github.com/codecrafters-io/kafka-tester/internal/field_decoder"
	"github.com/codecrafters-io/kafka-tester/protocol/kafkaapi/headers"
	"github.com/codecrafters-io/kafka-tester/protocol/value"
)

func decodeV0Header(decoder *field_decoder.FieldDecoder) (headers.ResponseHeader, field_decoder.FieldDecoderError) {
	correlationId, err := decoder.ReadInt32Field("Header.CorrelationID")
	if err != nil {
		return headers.ResponseHeader{}, err
	}

	return headers.ResponseHeader{
		Version:       0,
		CorrelationId: correlationId,
	}, nil
}

func decodeV1Header(decoder *field_decoder.FieldDecoder) (headers.ResponseHeader, field_decoder.FieldDecoderError) {
	correlationId, err := decoder.ReadInt32Field("Header.CorrelationID")
	if err != nil {
		return headers.ResponseHeader{}, err
	}

	if err := decoder.ConsumeTagBufferField(); err != nil {
		return headers.ResponseHeader{}, err
	}

	return headers.ResponseHeader{
		Version:       1,
		CorrelationId: correlationId,
	}, nil
}

func decodeCompactArray[T any](decoder *field_decoder.FieldDecoder, decodeFunc func(*field_decoder.FieldDecoder) (T, field_decoder.FieldDecoderError), path string) ([]T, field_decoder.FieldDecoderError) {
	decoder.PushPathContext(path)
	defer decoder.PopPathContext()

	lengthValue, err := decoder.ReadCompactArrayLengthField("Length")
	if err != nil {
		return nil, err
	}

	elements := make([]T, lengthValue.ActualLength())

	for i := 0; i < int(lengthValue.ActualLength()); i++ {
		decoder.PushPathContext(fmt.Sprintf("%s[%d]", path, i))
		element, err := decodeFunc(decoder)
		decoder.PopPathContext()

		if err != nil {
			return nil, err
		}

		elements[i] = element
	}

	return elements, nil
}

// decodeInt32 decodes int32 field using the passed-in decoder
// see usage
func decodeInt32(decoder *field_decoder.FieldDecoder) (value.Int32, field_decoder.FieldDecoderError) {
	decodedValue, err := decoder.ReadInt32Field("")
	if err != nil {
		return value.Int32{}, err
	}
	return decodedValue, nil
}
