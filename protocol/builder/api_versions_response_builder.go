package builder

import kafkaapi "github.com/codecrafters-io/kafka-tester/protocol/api"

type ApiVersionsResponseBuilder struct {
	version        int16
	errorCode      int16
	apiKeys        []kafkaapi.ApiVersionsResponseKey
	throttleTimeMs int32
}

func NewApiVersionsResponseBuilder() ApiVersionsResponseBuilderI {
	return &ApiVersionsResponseBuilder{
		version:        3,
		errorCode:      0,
		apiKeys:        []kafkaapi.ApiVersionsResponseKey{},
		throttleTimeMs: 0,
	}
}

func (b *ApiVersionsResponseBuilder) WithVersion(version int16) ApiVersionsResponseBuilderI {
	b.version = version
	return b
}

func (b *ApiVersionsResponseBuilder) WithErrorCode(errorCode int16) ApiVersionsResponseBuilderI {
	b.errorCode = errorCode
	return b
}

func (b *ApiVersionsResponseBuilder) AddApiKey(apiKey int16, minVersion int16, maxVersion int16) ApiVersionsResponseBuilderI {
	b.apiKeys = append(b.apiKeys, kafkaapi.ApiVersionsResponseKey{
		Version:    b.version,
		ApiKey:     apiKey,
		MinVersion: minVersion,
		MaxVersion: maxVersion,
	})
	return b
}

func (b *ApiVersionsResponseBuilder) WithThrottleTimeMs(throttleTimeMs int32) ApiVersionsResponseBuilderI {
	b.throttleTimeMs = throttleTimeMs
	return b
}

func (b *ApiVersionsResponseBuilder) Build(correlationId int32) kafkaapi.ApiVersionsResponse {
	return kafkaapi.ApiVersionsResponse{
		Version:        b.version,
		ErrorCode:      b.errorCode,
		ApiKeys:        b.apiKeys,
		ThrottleTimeMs: b.throttleTimeMs,
	}
}