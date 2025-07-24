package builder

import (
	kafkaapi "github.com/codecrafters-io/kafka-tester/protocol/api"
	"github.com/codecrafters-io/kafka-tester/protocol/api/headers"
)

type ProduceResponseBuilder struct {
	correlationId int32
	// topicName -> partitionIndex -> partitionResponse
	topicData      map[string]map[int32]kafkaapi.ProducePartitionResponse
	throttleTimeMs int32
}

func NewProduceResponseBuilder() *ProduceResponseBuilder {
	return &ProduceResponseBuilder{
		topicData:      make(map[string]map[int32]kafkaapi.ProducePartitionResponse),
		throttleTimeMs: 0,
	}
}

func (b *ProduceResponseBuilder) WithCorrelationId(correlationId int32) *ProduceResponseBuilder {
	b.correlationId = correlationId
	return b
}

func (b *ProduceResponseBuilder) addPartitionResponse(topicName string, partitionIndex int32, partitionResponse kafkaapi.ProducePartitionResponse) *ProduceResponseBuilder {
	if b.topicData[topicName] == nil {
		b.topicData[topicName] = make(map[int32]kafkaapi.ProducePartitionResponse)
	}
	b.topicData[topicName][partitionIndex] = partitionResponse
	return b
}

func (b *ProduceResponseBuilder) AddErrorPartitionResponse(topicName string, partitionIndex int32, errorCode int16) *ProduceResponseBuilder {
	if errorCode == 0 {
		panic("CodeCrafters Internal Error: Error code must be non-zero")
	}

	partitionResponse := NewProducePartitionResponseBuilder().
		WithError(errorCode).
		WithIndex(partitionIndex).
		Build()
	return b.addPartitionResponse(topicName, partitionIndex, partitionResponse)
}

func (b *ProduceResponseBuilder) AddSuccessPartitionResponse(topicName string, partitionIndex int32) *ProduceResponseBuilder {
	partitionResponse := NewProducePartitionResponseBuilder().
		WithError(0).
		WithIndex(partitionIndex).
		Build()
	return b.addPartitionResponse(topicName, partitionIndex, partitionResponse)
}

func (b *ProduceResponseBuilder) Build() kafkaapi.ProduceResponse {
	if len(b.topicData) == 0 {
		panic("CodeCrafters Internal Error: At least one topic response is required")
	}

	topicResponses := make([]kafkaapi.ProduceTopicResponse, 0, len(b.topicData))

	for topicName := range b.topicData {
		partitions := b.topicData[topicName]
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
