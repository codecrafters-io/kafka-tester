package builder

import (
	kafkaapi "github.com/codecrafters-io/kafka-tester/protocol/api"
)

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
		Version:    b.version,
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

/***************************** Updated Interface ***************************/

type ApiVersionsResponseBuilder__Updated struct {
	version        int16
	correlationId  int32
	errorCode      int16
	apiKeys        []kafkaapi.ApiKeyEntry__Updated
	throttleTimeMs int32
}

func NewApiVersionsResponseBuilder__Updated() *ApiVersionsResponseBuilder__Updated {
	return &ApiVersionsResponseBuilder__Updated{
		version:        4,
		correlationId:  -1,
		errorCode:      0,
		throttleTimeMs: 0,
		apiKeys:        []kafkaapi.ApiKeyEntry__Updated{},
	}
}

func (b *ApiVersionsResponseBuilder__Updated) WithCorrelationId(correlationId int32) *ApiVersionsResponseBuilder__Updated {
	b.correlationId = correlationId
	return b
}

func (b *ApiVersionsResponseBuilder__Updated) WithErrorCode(errorCode int16) *ApiVersionsResponseBuilder__Updated {
	b.errorCode = errorCode
	return b
}

func (b *ApiVersionsResponseBuilder__Updated) WithThrottleTimeMs(throttleTimeMs int32) *ApiVersionsResponseBuilder__Updated {
	b.throttleTimeMs = throttleTimeMs
	return b
}

func (b *ApiVersionsResponseBuilder__Updated) AddApiKeyEntry(apiKey int16, minVersion int16, maxVersion int16) *ApiVersionsResponseBuilder__Updated {
	b.apiKeys = append(b.apiKeys, kafkaapi.ApiKeyEntry__Updated{
		Version:    b.version,
		ApiKey:     apiKey,
		MinVersion: minVersion,
		MaxVersion: maxVersion,
	})
	return b
}

func (b *ApiVersionsResponseBuilder__Updated) Build() kafkaapi.ApiVersionsResponse__Updated {
	return kafkaapi.ApiVersionsResponse__Updated{
		Header: NewResponseHeaderBuilder__Updated().WithCorrelationId(b.correlationId).WithVersion(0).Build(),
		Body: kafkaapi.ApiVersionsResponseBody__Updated{
			Version:        b.version,
			ErrorCode:      b.errorCode,
			ApiKeys:        b.apiKeys,
			ThrottleTimeMs: b.throttleTimeMs,
		},
	}
}

func (b *ApiVersionsResponseBuilder__Updated) BuildEmpty() kafkaapi.ApiVersionsResponse__Updated {
	return kafkaapi.ApiVersionsResponse__Updated{
		Header: BuildEmptyResponseHeaderv0__Updated(),
		Body:   kafkaapi.ApiVersionsResponseBody__Updated{Version: 4},
	}
}
