package builder

import (
	"math"

	"github.com/codecrafters-io/kafka-tester/protocol/kafkaapi"
)

type FetchRequstWithEmptyTopicsBuilder struct {
	correlationId int32
}

func NewFetchRequstWithEmptyTopicsBuilder() *FetchRequstWithEmptyTopicsBuilder {
	return &FetchRequstWithEmptyTopicsBuilder{}
}

func (b *FetchRequstWithEmptyTopicsBuilder) WithCorrelationId(correlationId int32) *FetchRequstWithEmptyTopicsBuilder {
	b.correlationId = correlationId
	return b
}

func (b *FetchRequstWithEmptyTopicsBuilder) Build() kafkaapi.FetchRequest {
	return kafkaapi.FetchRequest{
		Header: NewRequestHeaderBuilder().BuildFetchRequestHeader(b.correlationId),
		Body: kafkaapi.FetchRequestBody{
			MaxWaitMS:       500,
			MinBytes:        1,
			MaxBytes:        math.MaxInt32,
			Topics:          []kafkaapi.Topic{},
			ForgottenTopics: []kafkaapi.ForgottenTopic{},
		},
	}
}
