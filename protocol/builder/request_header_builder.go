package builder

import (
	"github.com/codecrafters-io/kafka-tester/protocol/kafkaapi/headers"
)

type RequestHeaderBuilder struct {
	apiKey        int16
	apiVersion    int16
	correlationId int32
}

func NewRequestHeaderBuilder() *RequestHeaderBuilder {
	return &RequestHeaderBuilder{
		apiKey:        0,
		apiVersion:    0,
		correlationId: -1,
	}
}

func (b *RequestHeaderBuilder) WithApiKey(apiKey int16) *RequestHeaderBuilder {
	b.apiKey = apiKey
	return b
}

func (b *RequestHeaderBuilder) WithApiVersion(apiVersion int16) *RequestHeaderBuilder {
	b.apiVersion = apiVersion
	return b
}

func (b *RequestHeaderBuilder) WithCorrelationId(correlationId int32) *RequestHeaderBuilder {
	b.correlationId = correlationId
	return b
}

func (b *RequestHeaderBuilder) Build() headers.RequestHeader {
	if b.correlationId == -1 {
		panic("CodeCrafters Internal Error: Correlation ID is required")
	}

	return headers.RequestHeader{
		ApiKey:        b.apiKey,
		ApiVersion:    b.apiVersion,
		CorrelationId: b.correlationId,
		ClientId:      "kafka-tester",
	}
}

func (b *RequestHeaderBuilder) BuildApiVersionsRequestHeader(correlationId int32, apiVersion int16) headers.RequestHeader {
	return b.WithApiKey(18).WithApiVersion(apiVersion).WithCorrelationId(correlationId).Build()
}

func (b *RequestHeaderBuilder) BuildDescribeTopicPartitionsHeader(correlationID int32) headers.RequestHeader {
	return b.WithApiKey(75).WithApiVersion(0).WithCorrelationId(correlationID).Build()
}
