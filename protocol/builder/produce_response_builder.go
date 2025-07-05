package builder

import (
	kafkaapi "github.com/codecrafters-io/kafka-tester/protocol/api"
)

type ProduceResponseBuilder struct {
	responseType string
	topics       map[string]map[int32]*PartitionResponseConfig
}

type PartitionResponseConfig struct {
	ErrorCode       int16
	BaseOffset      int64
	LogAppendTimeMs int64
	LogStartOffset  int64
	RecordErrors    []kafkaapi.RecordError
	ErrorMessage    *string
}

// AddTopicPartitionResponse adds a partition response for a specific topic and partition
func (rb *ProduceResponseBuilder) AddTopicPartitionResponse(
	topicName string,
	partitionIndex int32,
	errorCode int16,
) *ProduceResponseBuilder {
	if rb.topics[topicName] == nil {
		rb.topics[topicName] = make(map[int32]*PartitionResponseConfig)
	}

	logAppendTimeMs := int64(-1)
	var baseOffset, logStartOffset int64
	if errorCode == 0 {
		baseOffset = int64(0)
		logStartOffset = int64(0)
	} else {
		baseOffset = int64(-1)
		logStartOffset = int64(-1)
	}

	rb.topics[topicName][partitionIndex] = &PartitionResponseConfig{
		ErrorCode:       errorCode,
		BaseOffset:      baseOffset,
		LogAppendTimeMs: logAppendTimeMs,
		LogStartOffset:  logStartOffset,
		RecordErrors:    []kafkaapi.RecordError{},
		ErrorMessage:    nil,
	}

	return rb
}

// Build builds the expected Produce response
func (rb *ProduceResponseBuilder) Build(correlationId int32) kafkaapi.ProduceResponse {
	if len(rb.topics) == 0 {
		panic("CodeCrafters Internal Error: At least one topic response is required")
	}

	topicResponses := make([]kafkaapi.ProduceTopicResponse, 0, len(rb.topics))

	for topicName, partitions := range rb.topics {
		partitionResponses := make([]kafkaapi.ProducePartitionResponse, 0, len(partitions))
		for partitionIndex, config := range partitions {
			partitionResponses = append(partitionResponses, kafkaapi.ProducePartitionResponse{
				Index:           partitionIndex,
				ErrorCode:       config.ErrorCode,
				BaseOffset:      config.BaseOffset,
				LogAppendTimeMs: config.LogAppendTimeMs,
				LogStartOffset:  config.LogStartOffset,
				RecordErrors:    config.RecordErrors,
				ErrorMessage:    config.ErrorMessage,
			})
		}

		topicResponses = append(topicResponses, kafkaapi.ProduceTopicResponse{
			Name:               topicName,
			PartitionResponses: partitionResponses,
		})
	}

	return kafkaapi.ProduceResponse{
		Header: NewResponseHeaderBuilder().WithCorrelationId(correlationId).Build(),
		Body: kafkaapi.ProduceResponseBody{
			Version:        11,
			Responses:      topicResponses,
			ThrottleTimeMs: 0,
		},
	}
}

// Predefined response builders for different test scenarios

// BuildUnknownTopicResponse - Stage P1, P2, P3: Error code 3 (unknown topic)
func BuildUnknownTopicResponse(topicName string, partitionIndex int32, correlationId int32) kafkaapi.ProduceResponse {
	return NewProduceResponseBuilder().
		AddTopicPartitionResponse(topicName, partitionIndex, 3).
		Build(correlationId)
}

// BuildSuccessfulResponse - Stage P4, P5: Error code 0 (success)
func BuildSuccessfulResponse(topicName string, partitionIndex int32, correlationId int32) kafkaapi.ProduceResponse {
	return NewProduceResponseBuilder().
		AddTopicPartitionResponse(topicName, partitionIndex, 0).
		Build(correlationId)
}

// BuildMultiPartitionResponse - Stage P6: Multiple partitions for same topic
func BuildMultiPartitionResponse(topicName string, correlationId int32) kafkaapi.ProduceResponse {
	return NewProduceResponseBuilder().
		AddTopicPartitionResponse(topicName, 0, 0).
		AddTopicPartitionResponse(topicName, 1, 0).
		Build(correlationId)
}

// BuildMultiTopicResponse - Stage P7: Multiple topics
func BuildMultiTopicResponse(correlationId int32) kafkaapi.ProduceResponse {
	return NewProduceResponseBuilder().
		AddTopicPartitionResponse("qux", 0, 0).
		AddTopicPartitionResponse("quz", 1, 0).
		Build(correlationId)
}
