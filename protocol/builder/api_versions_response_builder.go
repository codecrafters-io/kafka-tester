package builder

import "github.com/codecrafters-io/kafka-tester/protocol/kafkaapi"

type ApiVersionsResponseBuilder struct {
	version        int16
	correlationId  int32
	errorCode      int16
	apiKeys        []kafkaapi.ApiKeyEntry
	throttleTimeMs int32
}

func NewApiVersionsResponseBuilder() *ApiVersionsResponseBuilder {
	return &ApiVersionsResponseBuilder{
		version:        4,
		correlationId:  -1,
		errorCode:      0,
		throttleTimeMs: 0,
		apiKeys:        []kafkaapi.ApiKeyEntry{},
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
	b.apiKeys = append(b.apiKeys, kafkaapi.ApiKeyEntry{
		ApiKey:     apiKey,
		MinVersion: minVersion,
		MaxVersion: maxVersion,
	})

	return b
}

func (b *ApiVersionsResponseBuilder) Build() kafkaapi.ApiVersionsResponse {
	return kafkaapi.ApiVersionsResponse{
		Header: NewResponseHeaderBuilder().WithCorrelationId(b.correlationId).WithVersion(0).Build(),
		Body: kafkaapi.ApiVersionsResponseBody{
			Version:        b.version,
			ErrorCode:      b.errorCode,
			ApiKeys:        b.apiKeys,
			ThrottleTimeMs: b.throttleTimeMs,
		},
	}
}

func (b *ApiVersionsResponseBuilder) BuildEmpty() kafkaapi.ApiVersionsResponse {
	return kafkaapi.ApiVersionsResponse{
		Header: BuildEmptyResponseHeaderv0(),
		Body:   kafkaapi.ApiVersionsResponseBody{Version: 4},
	}
}
