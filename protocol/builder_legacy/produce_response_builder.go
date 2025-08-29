package builder_legacy

import (
	"github.com/codecrafters-io/kafka-tester/protocol/kafkaapi_legacy"
)

type ProduceResponseBuilder struct {
	correlationId int32
	topicData     []kafkaapi_legacy.ProduceTopicResponse
}

func NewProduceResponseBuilder() *ProduceResponseBuilder {
	return &ProduceResponseBuilder{
		correlationId: -1,
		topicData:     make([]kafkaapi_legacy.ProduceTopicResponse, 0),
	}
}

func (b *ProduceResponseBuilder) WithCorrelationId(correlationId int32) *ProduceResponseBuilder {
	b.correlationId = correlationId
	return b
}

func (b *ProduceResponseBuilder) addPartitionResponse(topicName string, partitionResponse kafkaapi_legacy.ProducePartitionResponse) *ProduceResponseBuilder {
	for i := range b.topicData {
		if b.topicData[i].Name == topicName {
			b.topicData[i].PartitionResponses = append(b.topicData[i].PartitionResponses, partitionResponse)
			return b
		}
	}

	topicResponse := kafkaapi_legacy.ProduceTopicResponse{
		Name:               topicName,
		PartitionResponses: []kafkaapi_legacy.ProducePartitionResponse{partitionResponse},
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
	return b.addPartitionResponse(topicName, partitionResponse)
}

func (b *ProduceResponseBuilder) AddSuccessPartitionResponse(topicName string, partitionIndex int32) *ProduceResponseBuilder {
	partitionResponse := NewProducePartitionResponseBuilder().
		WithIndex(partitionIndex).
		Build()
	return b.addPartitionResponse(topicName, partitionResponse)
}

func (b *ProduceResponseBuilder) Build() kafkaapi_legacy.ProduceResponse {
	if len(b.topicData) == 0 {
		panic("CodeCrafters Internal Error: At least one topic response is required")
	}

	if b.correlationId == -1 {
		panic("CodeCrafters Internal Error: Correlation ID is required")
	}

	return kafkaapi_legacy.ProduceResponse{
		Header: BuildResponseHeader(b.correlationId),
		Body: kafkaapi_legacy.ProduceResponseBody{
			TopicResponses: b.topicData,
			ThrottleTimeMs: 0,
		},
	}
}

func NewEmptyProduceResponse() kafkaapi_legacy.ProduceResponse {
	return kafkaapi_legacy.ProduceResponse{
		Header: BuildEmptyResponseHeaderv1(),
		Body:   kafkaapi_legacy.ProduceResponseBody{},
	}
}
