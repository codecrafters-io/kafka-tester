package builder

import (
	kafkaapi "github.com/codecrafters-io/kafka-tester/protocol/api"
	"github.com/codecrafters-io/kafka-tester/protocol/api/headers"
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
		version:        3,
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
	if b.correlationId == -1 {
		panic("CodeCrafters Internal Error: CorrelationId is required")
	}

	return kafkaapi.ApiVersionsResponse{
		// TODO: Add ResponseHeaderBuilder
		Header: headers.ResponseHeader{
			Version:       0,
			CorrelationId: b.correlationId,
		},
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
		Header: headers.ResponseHeader{
			Version: 0,
		},
		Body: kafkaapi.ApiVersionsResponseBody{Version: 3},
	}
}
