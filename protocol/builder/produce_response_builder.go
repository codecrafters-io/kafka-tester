package builder

import (
	kafkaapi "github.com/codecrafters-io/kafka-tester/protocol/api"
	"github.com/codecrafters-io/kafka-tester/protocol/api/headers"
)

type ProduceResponseBuilder struct {
	correlationId int32
	// topicName -> partitionIndex -> partitionResponse
	topics         map[string]map[int32]kafkaapi.ProducePartitionResponse
	throttleTimeMs int32
}

func NewProduceResponseBuilder() *ProduceResponseBuilder {
	return &ProduceResponseBuilder{
		topics:         make(map[string]map[int32]kafkaapi.ProducePartitionResponse),
		throttleTimeMs: 0,
	}
}

func (b *ProduceResponseBuilder) WithCorrelationId(correlationId int32) *ProduceResponseBuilder {
	b.correlationId = correlationId
	return b
}

func (b *ProduceResponseBuilder) WithThrottleTimeMs(throttleTimeMs int32) *ProduceResponseBuilder {
	b.throttleTimeMs = throttleTimeMs
	return b
}

func (b *ProduceResponseBuilder) AddPartitionResponse(topicName string, partitionIndex int32, partitionResponse kafkaapi.ProducePartitionResponse) *ProduceResponseBuilder {
	if b.topics[topicName] == nil {
		b.topics[topicName] = make(map[int32]kafkaapi.ProducePartitionResponse)
	}
	b.topics[topicName][partitionIndex] = partitionResponse
	return b
}

func (b *ProduceResponseBuilder) CreateAndAddErrorPartitionResponse(topicName string, partitionIndex int32, errorCode int16) *ProduceResponseBuilder {
	partitionResponse := NewPartitionResponseBuilder().
		WithError(errorCode).
		WithIndex(partitionIndex).
		Build()
	return b.AddPartitionResponse(topicName, partitionIndex, partitionResponse)
}

func (b *ProduceResponseBuilder) Build() kafkaapi.ProduceResponse {
	if len(b.topics) == 0 {
		panic("CodeCrafters Internal Error: At least one topic response is required")
	}

	topicResponses := make([]kafkaapi.ProduceTopicResponse, 0, len(b.topics))

	for topicName := range b.topics {
		partitions := b.topics[topicName]
		partitionResponses := make([]kafkaapi.ProducePartitionResponse, 0, len(partitions))
		for _, partitionResponse := range partitions {
			partitionResponses = append(partitionResponses, partitionResponse)
		}

		topicResponses = append(topicResponses, kafkaapi.ProduceTopicResponse{
			Name:               topicName,
			PartitionResponses: partitionResponses,
		})
	}

	return kafkaapi.ProduceResponse{
		Header: headers.ResponseHeader{
			Version:       1,
			CorrelationId: b.correlationId,
		},
		Body: kafkaapi.ProduceResponseBody{
			TopicResponses: topicResponses,
			ThrottleTimeMs: b.throttleTimeMs,
		},
	}
}

func (b *ProduceResponseBuilder) BuildEmpty() kafkaapi.ProduceResponse {
	return kafkaapi.ProduceResponse{
		Header: headers.ResponseHeader{
			Version: 1,
		},
		Body: kafkaapi.ProduceResponseBody{},
	}
}
