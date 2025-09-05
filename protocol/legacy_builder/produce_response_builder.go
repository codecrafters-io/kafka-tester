package legacy_builder

import (
	"github.com/codecrafters-io/kafka-tester/protocol/legacy_kafkaapi"
)

type ProduceResponseBuilder struct {
	correlationId int32
	topicData     []legacy_kafkaapi.ProduceTopicResponse
}

func NewProduceResponseBuilder() *ProduceResponseBuilder {
	return &ProduceResponseBuilder{
		correlationId: -1,
		topicData:     make([]legacy_kafkaapi.ProduceTopicResponse, 0),
	}
}

func (b *ProduceResponseBuilder) WithCorrelationId(correlationId int32) *ProduceResponseBuilder {
	b.correlationId = correlationId
	return b
}

func (b *ProduceResponseBuilder) addPartitionResponse(topicName string, partitionResponse legacy_kafkaapi.ProducePartitionResponse) *ProduceResponseBuilder {
	for i := range b.topicData {
		if b.topicData[i].Name == topicName {
			b.topicData[i].PartitionResponses = append(b.topicData[i].PartitionResponses, partitionResponse)
			return b
		}
	}

	topicResponse := legacy_kafkaapi.ProduceTopicResponse{
		Name:               topicName,
		PartitionResponses: []legacy_kafkaapi.ProducePartitionResponse{partitionResponse},
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

func (b *ProduceResponseBuilder) Build() legacy_kafkaapi.ProduceResponse {
	if len(b.topicData) == 0 {
		panic("CodeCrafters Internal Error: At least one topic response is required")
	}

	if b.correlationId == -1 {
		panic("CodeCrafters Internal Error: Correlation ID is required")
	}

	return legacy_kafkaapi.ProduceResponse{
		Header: BuildResponseHeader(b.correlationId),
		Body: legacy_kafkaapi.ProduceResponseBody{
			TopicResponses: b.topicData,
			ThrottleTimeMs: 0,
		},
	}
}

func NewEmptyProduceResponse() legacy_kafkaapi.ProduceResponse {
	return legacy_kafkaapi.ProduceResponse{
		Header: BuildEmptyResponseHeaderv1(),
		Body:   legacy_kafkaapi.ProduceResponseBody{},
	}
}
