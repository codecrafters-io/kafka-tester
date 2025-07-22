package builder

import (
	kafkaapi "github.com/codecrafters-io/kafka-tester/protocol/api"
)

type ApiVersionsResponseBuilder struct {
	version        int16
	errorCode      int16
	apiKeys        []kafkaapi.ApiKeyEntry
	throttleTimeMs int32
}

func NewApiVersionsResponseBuilder() *ApiVersionsResponseBuilder {
	return &ApiVersionsResponseBuilder{
		version:        3,
		errorCode:      0,
		throttleTimeMs: 0,
		apiKeys:        []kafkaapi.ApiKeyEntry{},
	}
}

func (b *ApiVersionsResponseBuilder) WithErrorCode(errorCode int16) *ApiVersionsResponseBuilder {
	b.errorCode = errorCode
	return b
}

func (b *ApiVersionsResponseBuilder) WithThrottleTimeMs(throttleTimeMs int32) *ApiVersionsResponseBuilder {
	b.throttleTimeMs = throttleTimeMs
	return b
}

func (b *ApiVersionsResponseBuilder) AddApiKeyEntry(apiKey int16, minVersion int16, maxVersion int16) *ApiVersionsResponseBuilder {
	b.apiKeys = append(b.apiKeys, kafkaapi.ApiKeyEntry{
		Version:    b.version,
		ApiKey:     apiKey,
		MinVersion: minVersion,
		MaxVersion: maxVersion,
	})
	return b
}

func (b *ApiVersionsResponseBuilder) Build(correlationId int32) kafkaapi.ApiVersionsResponse {
	return kafkaapi.ApiVersionsResponse{
		// TODO: Add ResponseHeaderBuilder
		Header: kafkaapi.ResponseHeader{
			CorrelationId: correlationId,
		},
		Body: kafkaapi.ApiVersionsResponseBody{
			Version:        b.version,
			ErrorCode:      b.errorCode,
			ApiKeys:        b.apiKeys,
			ThrottleTimeMs: b.throttleTimeMs,
		},
	}
}
