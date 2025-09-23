package builder

import (
	"github.com/codecrafters-io/kafka-tester/protocol/kafkaapi/headers"
	"github.com/codecrafters-io/kafka-tester/protocol/value"
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
		ApiKey:        value.Int16{Value: b.apiKey},
		ApiVersion:    value.Int16{Value: b.apiVersion},
		CorrelationId: value.Int32{Value: b.correlationId},
		ClientId:      value.String{Value: "kafka-tester"},
	}
}

func (b *RequestHeaderBuilder) BuildApiVersionsRequestHeader(correlationId int32, apiVersion int16) headers.RequestHeader {
	return b.WithApiKey(18).WithApiVersion(apiVersion).WithCorrelationId(correlationId).Build()
}

func (b *RequestHeaderBuilder) BuildDescribeTopicPartitionsHeader(correlationID int32) headers.RequestHeader {
	return b.WithApiKey(75).WithApiVersion(0).WithCorrelationId(correlationID).Build()
}

func (b *RequestHeaderBuilder) BuildFetchRequestHeader(correlationID int32) headers.RequestHeader {
	return b.WithApiKey(1).WithApiVersion(16).WithCorrelationId(correlationID).Build()
}

func (b *RequestHeaderBuilder) BuildProduceRequestHeader(correlationId int32) headers.RequestHeader {
	return b.WithApiKey(0).WithApiVersion(11).WithCorrelationId(correlationId).Build()
}
