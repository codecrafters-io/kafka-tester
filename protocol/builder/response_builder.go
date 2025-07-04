package builder

import (
	kafkaapi "github.com/codecrafters-io/kafka-tester/protocol/api"
)

type ResponseBuilder struct {
	responseType string
	topics       map[string]map[int32]*PartitionResponseConfig
}

type PartitionResponseConfig struct {
	ErrorCode         int16
	BaseOffset        int64
	LogAppendTimeMs   int64
	LogStartOffset    int64
	RecordErrors      []kafkaapi.RecordError
	ErrorMessage      *string
}

func NewResponseBuilder(responseType string) *ResponseBuilder {
	return &ResponseBuilder{
		responseType: responseType,
		topics:       make(map[string]map[int32]*PartitionResponseConfig),
	}
}

// AddTopicPartitionResponse adds a partition response for a specific topic and partition
func (rb *ResponseBuilder) AddTopicPartitionResponse(
	topicName string,
	partitionIndex int32,
	errorCode int16,
	baseOffset int64,
	logAppendTimeMs int64,
	logStartOffset int64,
) *ResponseBuilder {
	if rb.topics[topicName] == nil {
		rb.topics[topicName] = make(map[int32]*PartitionResponseConfig)
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

// WithErrorMessage sets an error message for the last added partition
func (rb *ResponseBuilder) WithErrorMessage(topicName string, partitionIndex int32, errorMessage string) *ResponseBuilder {
	if rb.topics[topicName] != nil && rb.topics[topicName][partitionIndex] != nil {
		rb.topics[topicName][partitionIndex].ErrorMessage = &errorMessage
	}
	return rb
}

// BuildProduceResponse builds the expected Produce response
func (rb *ResponseBuilder) BuildProduceResponse(correlationId int32, throttleTimeMs int32) kafkaapi.ProduceResponse {
	if len(rb.topics) == 0 {
		panic("CodeCrafters Internal Error: At least one topic response is required")
	}

	// Convert topics map to slice
	topicResponses := make([]kafkaapi.ProduceTopicResponse, 0, len(rb.topics))
	for topicName, partitions := range rb.topics {
		// Convert partitions map to slice for this topic
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
		Header: kafkaapi.ResponseHeader{
			CorrelationId: correlationId,
		},
		Body: kafkaapi.ProduceResponseBody{
			Version:        11,
			Responses:      topicResponses,
			ThrottleTimeMs: throttleTimeMs,
		},
	}
}

// Predefined response builders for different test scenarios

// BuildUnknownTopicResponse - Stage P1, P2, P3: Error code 3 (unknown topic)
func BuildUnknownTopicResponse(topicName string, partitionIndex int32, correlationId int32) kafkaapi.ProduceResponse {
	return NewResponseBuilder("produce").
		AddTopicPartitionResponse(topicName, partitionIndex, 3, -1, -1, -1).
		BuildProduceResponse(correlationId, 0)
}

// BuildSuccessfulResponse - Stage P4, P5: Error code 0 (success)
func BuildSuccessfulResponse(topicName string, partitionIndex int32, baseOffset int64, correlationId int32) kafkaapi.ProduceResponse {
	return NewResponseBuilder("produce").
		AddTopicPartitionResponse(topicName, partitionIndex, 0, baseOffset, -1, 0).
		BuildProduceResponse(correlationId, 0)
}

// BuildMultiPartitionResponse - Stage P6: Multiple partitions for same topic
func BuildMultiPartitionResponse(topicName string, partition0BaseOffset int64, partition1BaseOffset int64, correlationId int32) kafkaapi.ProduceResponse {
	return NewResponseBuilder("produce").
		AddTopicPartitionResponse(topicName, 0, 0, partition0BaseOffset, -1, 0).
		AddTopicPartitionResponse(topicName, 1, 0, partition1BaseOffset, -1, 0).
		BuildProduceResponse(correlationId, 0)
}

// BuildMultiTopicResponse - Stage P7: Multiple topics
func BuildMultiTopicResponse(correlationId int32) kafkaapi.ProduceResponse {
	return NewResponseBuilder("produce").
		AddTopicPartitionResponse("qux", 0, 0, 0, -1, 0).
		AddTopicPartitionResponse("quz", 1, 0, 0, -1, 0).
		BuildProduceResponse(correlationId, 0)
}

// GetExpectedResponseForStage returns the expected response pattern for each test stage
func GetExpectedResponseForStage(stage string, correlationId int32) kafkaapi.ProduceResponse {
	switch stage {
	case "P1", "P2":
		return BuildUnknownTopicResponse("test-topic", 0, correlationId)
	case "P3":
		return BuildUnknownTopicResponse("quz", 4, correlationId)
	case "P4":
		return BuildSuccessfulResponse("quz", 1, 0, correlationId)
	case "P5":
		return BuildSuccessfulResponse("quz", 0, 2, correlationId)
	case "P6":
		return BuildMultiPartitionResponse("quz", 2, 0, correlationId)
	case "P7":
		return BuildMultiTopicResponse(correlationId)
	default:
		panic("Unknown stage: " + stage)
	}
}