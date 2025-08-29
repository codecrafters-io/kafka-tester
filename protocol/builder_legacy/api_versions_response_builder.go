package builder_legacy

import (
	"github.com/codecrafters-io/kafka-tester/protocol/kafkaapi_legacy"
)

type ApiVersionsResponseBuilder struct {
	version        int16
	correlationId  int32
	errorCode      int16
	apiKeys        []kafkaapi_legacy.ApiKeyEntry
	throttleTimeMs int32
}

func NewApiVersionsResponseBuilder() *ApiVersionsResponseBuilder {
	return &ApiVersionsResponseBuilder{
		version:        4,
		correlationId:  -1,
		errorCode:      0,
		throttleTimeMs: 0,
		apiKeys:        []kafkaapi_legacy.ApiKeyEntry{},
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
	b.apiKeys = append(b.apiKeys, kafkaapi_legacy.ApiKeyEntry{
		Version:    b.version,
		ApiKey:     apiKey,
		MinVersion: minVersion,
		MaxVersion: maxVersion,
	})
	return b
}

func (b *ApiVersionsResponseBuilder) Build() kafkaapi_legacy.ApiVersionsResponse {
	return kafkaapi_legacy.ApiVersionsResponse{
		Header: NewResponseHeaderBuilder().WithCorrelationId(b.correlationId).WithVersion(0).Build(),
		Body: kafkaapi_legacy.ApiVersionsResponseBody{
			Version:        b.version,
			ErrorCode:      b.errorCode,
			ApiKeys:        b.apiKeys,
			ThrottleTimeMs: b.throttleTimeMs,
		},
	}
}

func (b *ApiVersionsResponseBuilder) BuildEmpty() kafkaapi_legacy.ApiVersionsResponse {
	return kafkaapi_legacy.ApiVersionsResponse{
		Header: BuildEmptyResponseHeaderv0(),
		Body:   kafkaapi_legacy.ApiVersionsResponseBody{Version: 4},
	}
}
