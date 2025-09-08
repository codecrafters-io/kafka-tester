package response_decoders

import (
	"fmt"

	// TODO[PaulRefactor]: Avoid the import of value_storing_decoder from protocol?
	"github.com/codecrafters-io/kafka-tester/internal/field_decoder"
	"github.com/codecrafters-io/kafka-tester/protocol/kafkaapi"
)

func DecodeApiVersionsResponse(decoder *field_decoder.FieldDecoder) (kafkaapi.ApiVersionsResponse, error) {
	decoder.PushLocatorSegment("ApiVersionsResponse")
	defer decoder.PopLocatorSegment()

	header, err := decodeV0Header(decoder)
	if err != nil {
		return kafkaapi.ApiVersionsResponse{}, err
	}

	body, err := decodeApiVersionsResponseBody(decoder)
	if err != nil {
		return kafkaapi.ApiVersionsResponse{}, err
	}

	// Check if there are any remaining bytes in the decoder
	// TODO[PaulRefactor]: See if we can extract this outside decoders?
	if decoder.RemainingBytesCount() != 0 {
		return kafkaapi.ApiVersionsResponse{}, fmt.Errorf("unexpected %d bytes remaining in decoder after decoding ApiVersionsResponseBody", decoder.RemainingBytesCount())
	}

	return kafkaapi.ApiVersionsResponse{
		Header: header,
		Body:   body,
	}, nil
}

func decodeApiVersionsResponseBody(decoder *field_decoder.FieldDecoder) (kafkaapi.ApiVersionsResponseBody, error) {
	decoder.PushLocatorSegment("Body")
	defer decoder.PopLocatorSegment()

	errorCode, err := decoder.ReadInt16("ErrorCode")
	if err != nil {
		return kafkaapi.ApiVersionsResponseBody{}, err
	}

	apiKeyEntries, err := decodeApiVersionsResponseApiKeyEntries(decoder)
	if err != nil {
		return kafkaapi.ApiVersionsResponseBody{}, err
	}

	throttleTimeMs, err := decoder.ReadInt32("ThrottleTimeMs")
	if err != nil {
		return kafkaapi.ApiVersionsResponseBody{}, err
	}

	if err := decoder.ConsumeTagBuffer(); err != nil {
		return kafkaapi.ApiVersionsResponseBody{}, err
	}

	return kafkaapi.ApiVersionsResponseBody{
		Version:        4,
		ErrorCode:      errorCode,
		ApiKeys:        apiKeyEntries,
		ThrottleTimeMs: throttleTimeMs,
	}, nil
}

func decodeApiVersionsResponseApiKeyEntries(decoder *field_decoder.FieldDecoder) ([]kafkaapi.ApiKeyEntry, error) {
	lengthValue, err := decoder.ReadCompactArrayLength("ApiKeys.Length")
	if err != nil {
		return nil, err
	}

	apiKeyEntries := make([]kafkaapi.ApiKeyEntry, lengthValue.ActualLength())

	for i := 0; i < int(lengthValue.ActualLength()); i++ {
		decoder.PushLocatorSegment(fmt.Sprintf("ApiKeys[%d]", i))
		apiKeyEntry, err := decodeApiVersionsResponseApiKeyEntry(decoder)
		decoder.PopLocatorSegment()

		if err != nil {
			return nil, err
		}

		apiKeyEntries[i] = apiKeyEntry
	}

	return apiKeyEntries, nil
}

func decodeApiVersionsResponseApiKeyEntry(decoder *field_decoder.FieldDecoder) (kafkaapi.ApiKeyEntry, error) {
	apiKey, err := decoder.ReadInt16("APIKey")
	if err != nil {
		return kafkaapi.ApiKeyEntry{}, err
	}

	minVersion, err := decoder.ReadInt16("MinVersion")
	if err != nil {
		return kafkaapi.ApiKeyEntry{}, err
	}

	maxVersion, err := decoder.ReadInt16("MaxVersion")
	if err != nil {
		return kafkaapi.ApiKeyEntry{}, err
	}

	if err := decoder.ConsumeTagBuffer(); err != nil {
		return kafkaapi.ApiKeyEntry{}, err
	}

	return kafkaapi.ApiKeyEntry{
		ApiKey:     apiKey,
		MinVersion: minVersion,
		MaxVersion: maxVersion,
	}, nil
}
