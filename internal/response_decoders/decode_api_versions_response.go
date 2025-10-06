package response_decoders

import (
	"github.com/codecrafters-io/kafka-tester/internal/field_decoder"
	"github.com/codecrafters-io/kafka-tester/protocol/kafkaapi"
)

func DecodeApiVersionsResponse(decoder *field_decoder.FieldDecoder) (kafkaapi.ApiVersionsResponse, field_decoder.FieldDecoderError) {
	decoder.PushPathContext("ApiVersionsResponse")
	defer decoder.PopPathContext()

	header, err := decodeV0Header(decoder)
	if err != nil {
		return kafkaapi.ApiVersionsResponse{}, err
	}

	body, err := decodeApiVersionsResponseBody(decoder)
	if err != nil {
		return kafkaapi.ApiVersionsResponse{}, err
	}

	return kafkaapi.ApiVersionsResponse{
		Header: header,
		Body:   body,
	}, nil
}

func decodeApiVersionsResponseBody(decoder *field_decoder.FieldDecoder) (kafkaapi.ApiVersionsResponseBody, field_decoder.FieldDecoderError) {
	decoder.PushPathContext("Body")
	defer decoder.PopPathContext()

	errorCode, err := decoder.ReadInt16Field("ErrorCode")
	if err != nil {
		return kafkaapi.ApiVersionsResponseBody{}, err
	}

	apiKeyEntries, err := decodeCompactArray(decoder, decodeApiVersionsResponseApiKeyEntry, "ApiKeys")
	if err != nil {
		return kafkaapi.ApiVersionsResponseBody{}, err
	}

	throttleTimeMs, err := decoder.ReadInt32Field("ThrottleTimeMs")
	if err != nil {
		return kafkaapi.ApiVersionsResponseBody{}, err
	}

	if err := decoder.ConsumeTagBufferField(); err != nil {
		return kafkaapi.ApiVersionsResponseBody{}, err
	}

	return kafkaapi.ApiVersionsResponseBody{
		Version:        4,
		ErrorCode:      errorCode.KafkaValue,
		ApiKeys:        apiKeyEntries,
		ThrottleTimeMs: throttleTimeMs.KafkaValue,
	}, nil
}

func decodeApiVersionsResponseApiKeyEntry(decoder *field_decoder.FieldDecoder) (kafkaapi.ApiKeyEntry, field_decoder.FieldDecoderError) {
	apiKey, err := decoder.ReadInt16Field("APIKey")
	if err != nil {
		return kafkaapi.ApiKeyEntry{}, err
	}

	minVersion, err := decoder.ReadInt16Field("MinVersion")
	if err != nil {
		return kafkaapi.ApiKeyEntry{}, err
	}

	maxVersion, err := decoder.ReadInt16Field("MaxVersion")
	if err != nil {
		return kafkaapi.ApiKeyEntry{}, err
	}

	if err := decoder.ConsumeTagBufferField(); err != nil {
		return kafkaapi.ApiKeyEntry{}, err
	}

	return kafkaapi.ApiKeyEntry{
		ApiKey:     apiKey.KafkaValue,
		MinVersion: minVersion.KafkaValue,
		MaxVersion: maxVersion.KafkaValue,
	}, nil
}

// decoder for early stages where we decode only a portion of the response

func DecodeApiVersionsResponseUpToHeader(decoder *field_decoder.FieldDecoder) (kafkaapi.ApiVersionsResponse, field_decoder.FieldDecoderError) {
	decoder.PushPathContext("ApiVersionsResponse")
	defer decoder.PopPathContext()

	header, err := decodeV0Header(decoder)
	if err != nil {
		return kafkaapi.ApiVersionsResponse{}, err
	}

	return kafkaapi.ApiVersionsResponse{
		Header: header,
		Body:   kafkaapi.ApiVersionsResponseBody{},
	}, nil
}

func DecodeApiVersionsResponseUpToErrorCode(decoder *field_decoder.FieldDecoder) (kafkaapi.ApiVersionsResponse, field_decoder.FieldDecoderError) {
	decoder.PushPathContext("ApiVersionsResponse")
	defer decoder.PopPathContext()

	header, err := decodeV0Header(decoder)
	if err != nil {
		return kafkaapi.ApiVersionsResponse{}, err
	}

	decoder.PushPathContext("Body")
	defer decoder.PopPathContext()

	errorCode, err := decoder.ReadInt16Field("ErrorCode")
	if err != nil {
		return kafkaapi.ApiVersionsResponse{}, err
	}

	return kafkaapi.ApiVersionsResponse{
		Header: header,
		Body: kafkaapi.ApiVersionsResponseBody{
			ErrorCode: errorCode.KafkaValue,
		},
	}, nil
}
