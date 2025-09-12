package builder

import (
	"github.com/codecrafters-io/kafka-tester/protocol/kafkaapi"
)

type FetchRequstWithEmptyTopicsBuilder struct {
	correlationId int32
	sessionId     int32
}

func NewFetchRequstWithEmptyTopicsBuilder() *FetchRequstWithEmptyTopicsBuilder {
	return &FetchRequstWithEmptyTopicsBuilder{}
}

func (b *FetchRequstWithEmptyTopicsBuilder) WithCorrelationId(correlationId int32) *FetchRequstWithEmptyTopicsBuilder {
	b.correlationId = correlationId
	return b
}

func (b *FetchRequstWithEmptyTopicsBuilder) WithSessionId(sessionId int32) *FetchRequstWithEmptyTopicsBuilder {
	b.sessionId = sessionId
	return b
}

func (b *FetchRequstWithEmptyTopicsBuilder) Build() kafkaapi.FetchRequest {
	return kafkaapi.FetchRequest{
		Header: NewRequestHeaderBuilder().BuildFetchRequestHeader(b.correlationId),
		Body: kafkaapi.FetchRequestBody{
			SessionId:       b.sessionId,
			Topics:          []kafkaapi.Topic{},
			ForgottenTopics: []kafkaapi.ForgottenTopic{},
		},
	}
}
