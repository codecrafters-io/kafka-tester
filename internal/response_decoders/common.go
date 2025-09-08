package response_decoders

import (
	"fmt"

	"github.com/codecrafters-io/kafka-tester/internal/field_decoder"
	"github.com/codecrafters-io/kafka-tester/protocol/kafkaapi/headers"
)

func decodeV0Header(decoder *field_decoder.FieldDecoder) (headers.ResponseHeader, *field_decoder.FieldDecoderError) {
	correlationId, err := decoder.ReadInt32("Header.CorrelationID")
	if err != nil {
		return headers.ResponseHeader{}, err
	}

	return headers.ResponseHeader{
		Version:       0,
		CorrelationId: correlationId,
	}, nil
}

// func decodeV1Header(decoder *field_decoder.FieldDecoder) (headers.ResponseHeader, error) {
// 	decoder.PushPathSegment("Header")
// 	defer decoder.PopPathSegment()

// 	correlationId, err := decoder.ReadInt32("CorrelationID")
// 	if err != nil {
// 		return headers.ResponseHeader{}, err
// 	}

// 	if err := decoder.ConsumeTagBuffer(); err != nil {
// 		return headers.ResponseHeader{}, err
// 	}

// 	return headers.ResponseHeader{
// 		Version:       1,
// 		CorrelationId: correlationId,
// 	}, nil
// }

func decodeCompactArray[T any](decoder *field_decoder.FieldDecoder, decodeFunc func(*field_decoder.FieldDecoder) (T, *field_decoder.FieldDecoderError), path string) ([]T, *field_decoder.FieldDecoderError) {
	decoder.PushPathSegment(path)
	defer decoder.PopPathSegment()

	lengthValue, err := decoder.ReadCompactArrayLength("Length")
	if err != nil {
		return nil, err
	}

	elements := make([]T, lengthValue.ActualLength())

	for i := 0; i < int(lengthValue.ActualLength()); i++ {
		decoder.PushPathSegment(fmt.Sprintf("%s[%d]", path, i))
		element, err := decodeFunc(decoder)
		decoder.PopPathSegment()

		if err != nil {
			return nil, err
		}

		elements[i] = element
	}

	return elements, nil
}
