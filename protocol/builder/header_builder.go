package builder

import kafkaapi "github.com/codecrafters-io/kafka-tester/protocol/api"

type HeaderBuilder struct {
	apiKey        int16
	apiVersion    int16
	correlationId int32
}

func NewHeaderBuilder() *HeaderBuilder {
	return &HeaderBuilder{
		apiKey:        0,
		apiVersion:    0,
		correlationId: 0,
	}
}

func (b *HeaderBuilder) WithApiKey(apiKey int16) *HeaderBuilder {
	b.apiKey = apiKey
	return b
}

func (b *HeaderBuilder) WithApiVersion(apiVersion int16) *HeaderBuilder {
	b.apiVersion = apiVersion
	return b
}

func (b *HeaderBuilder) WithCorrelationId(correlationId int32) *HeaderBuilder {
	b.correlationId = correlationId
	return b
}

func (b *HeaderBuilder) Build() kafkaapi.RequestHeader {
	return kafkaapi.RequestHeader{
		ApiKey:        b.apiKey,
		ApiVersion:    b.apiVersion,
		CorrelationId: b.correlationId,
		ClientId:      "kafka-tester",
	}
}

func (b *HeaderBuilder) BuildProduceRequestHeader(correlationId int32) kafkaapi.RequestHeader {
	return b.WithApiKey(0).WithApiVersion(11).WithCorrelationId(correlationId).Build()
}
