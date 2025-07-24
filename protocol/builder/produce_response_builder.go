package builder

import (
	kafkaapi "github.com/codecrafters-io/kafka-tester/protocol/api"
	"github.com/codecrafters-io/kafka-tester/protocol/api/headers"
)

type ProduceResponseBuilder struct {
	correlationId  int32
	topicData      []kafkaapi.ProduceTopicResponse
	throttleTimeMs int32
}

func NewProduceResponseBuilder() *ProduceResponseBuilder {
	return &ProduceResponseBuilder{
		correlationId:  -1,
		topicData:      make([]kafkaapi.ProduceTopicResponse, 0),
		throttleTimeMs: 0,
	}
}

func (b *ProduceResponseBuilder) WithCorrelationId(correlationId int32) *ProduceResponseBuilder {
	b.correlationId = correlationId
	return b
}

func (b *ProduceResponseBuilder) addPartitionResponses(topicName string, partitionResponses ...kafkaapi.ProducePartitionResponse) *ProduceResponseBuilder {
	topicResponse := kafkaapi.ProduceTopicResponse{
		Name:               topicName,
		PartitionResponses: partitionResponses,
	}

	b.topicData = append(b.topicData, topicResponse)
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
	return b.addPartitionResponses(topicName, partitionResponse)
}

func (b *ProduceResponseBuilder) AddSuccessPartitionResponses(topicName string, partitionIndexes ...int32) *ProduceResponseBuilder {
	partitionResponses := make([]kafkaapi.ProducePartitionResponse, 0, len(partitionIndexes))
	for _, partitionIndex := range partitionIndexes {
		partitionResponse := NewProducePartitionResponseBuilder().
			WithError(0).
			WithIndex(partitionIndex).
			Build()
		partitionResponses = append(partitionResponses, partitionResponse)
	}
	return b.addPartitionResponses(topicName, partitionResponses...)
}

func (b *ProduceResponseBuilder) Build() kafkaapi.ProduceResponse {
	if len(b.topicData) == 0 {
		panic("CodeCrafters Internal Error: At least one topic response is required")
	}

	if b.correlationId == -1 {
		panic("CodeCrafters Internal Error: Correlation ID is required")
	}

	return kafkaapi.ProduceResponse{
		Header: headers.ResponseHeader{
			Version:       1,
			CorrelationId: b.correlationId,
		},
		Body: kafkaapi.ProduceResponseBody{
			TopicResponses: b.topicData,
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
