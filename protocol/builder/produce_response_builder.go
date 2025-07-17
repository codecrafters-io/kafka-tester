package builder

import (
	kafkaapi "github.com/codecrafters-io/kafka-tester/protocol/api"
)

type ProduceResponseBuilder struct {
	responseType string
	topics       map[string]map[int32]*PartitionResponseConfig
}

func NewProduceResponseBuilder() *ProduceResponseBuilder {
	return &ProduceResponseBuilder{
		responseType: "produce",
		topics:       make(map[string]map[int32]*PartitionResponseConfig),
	}
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
	return rb.AddTopicPartitionResponseWithBaseOffset(topicName, partitionIndex, errorCode, 0)
}

// AddTopicPartitionResponseWithBaseOffset adds a partition response with a specific base offset
func (rb *ProduceResponseBuilder) AddTopicPartitionResponseWithBaseOffset(
	topicName string,
	partitionIndex int32,
	errorCode int16,
	baseOffset int64,
) *ProduceResponseBuilder {
	if rb.topics[topicName] == nil {
		rb.topics[topicName] = make(map[int32]*PartitionResponseConfig)
	}

	logAppendTimeMs := int64(-1)
	var logStartOffset int64
	if errorCode == 0 {
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
