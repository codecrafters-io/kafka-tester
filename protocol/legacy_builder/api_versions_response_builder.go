package legacy_builder

import (
	"github.com/codecrafters-io/kafka-tester/protocol/legacy_kafkaapi"
)

type ApiVersionsResponseBuilder struct {
	version        int16
	correlationId  int32
	errorCode      int16
	apiKeys        []legacy_kafkaapi.ApiKeyEntry
	throttleTimeMs int32
}

func NewApiVersionsResponseBuilder() *ApiVersionsResponseBuilder {
	return &ApiVersionsResponseBuilder{
		version:        4,
		correlationId:  -1,
		errorCode:      0,
		throttleTimeMs: 0,
		apiKeys:        []legacy_kafkaapi.ApiKeyEntry{},
	}
}

func (b *ApiVersionsResponseBuilder) WithCorrelationId(correlationId int32) *ApiVersionsResponseBuilder {
	b.correlationId = correlationId
	return b
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
	b.apiKeys = append(b.apiKeys, legacy_kafkaapi.ApiKeyEntry{
		Version:    b.version,
		ApiKey:     apiKey,
		MinVersion: minVersion,
		MaxVersion: maxVersion,
	})
	return b
}

func (b *ApiVersionsResponseBuilder) Build() legacy_kafkaapi.ApiVersionsResponse {
	return legacy_kafkaapi.ApiVersionsResponse{
		Header: NewResponseHeaderBuilder().WithCorrelationId(b.correlationId).WithVersion(0).Build(),
		Body: legacy_kafkaapi.ApiVersionsResponseBody{
			Version:        b.version,
			ErrorCode:      b.errorCode,
			ApiKeys:        b.apiKeys,
			ThrottleTimeMs: b.throttleTimeMs,
		},
	}
}

func (b *ApiVersionsResponseBuilder) BuildEmpty() legacy_kafkaapi.ApiVersionsResponse {
	return legacy_kafkaapi.ApiVersionsResponse{
		Header: BuildEmptyResponseHeaderv0(),
		Body:   legacy_kafkaapi.ApiVersionsResponseBody{Version: 4},
	}
}
