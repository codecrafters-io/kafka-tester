package response_decoders

import (
	"github.com/codecrafters-io/kafka-tester/internal/field_decoder"
	"github.com/codecrafters-io/kafka-tester/protocol/kafkaapi/headers"
)

func decodeV0Header(decoder *field_decoder.FieldDecoder) (headers.ResponseHeader, error) {
	// TODO[PaulRefactor]: Allow reading values like Header.CorrelationId directly?
	decoder.PushLocatorSegment("Header")
	defer decoder.PopLocatorSegment()

	correlationId, err := decoder.ReadInt32("CorrelationID")
	if err != nil {
		return headers.ResponseHeader{}, err
	}

	return headers.ResponseHeader{
		Version:       0,
		CorrelationId: correlationId,
	}, nil
}

func decodeV1Header(decoder *field_decoder.FieldDecoder) (headers.ResponseHeader, error) {
	// TODO[PaulRefactor]: Allow reading values like Header.CorrelationId directly?
	decoder.PushLocatorSegment("Header")
	defer decoder.PopLocatorSegment()

	correlationId, err := decoder.ReadInt32("CorrelationID")
	if err != nil {
		return headers.ResponseHeader{}, err
	}

	if err := decoder.ConsumeTagBuffer(); err != nil {
		return headers.ResponseHeader{}, err
	}

	return headers.ResponseHeader{
		Version:       1,
		CorrelationId: correlationId,
	}, nil
}
