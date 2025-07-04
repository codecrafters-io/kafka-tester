package builder

import (
	"time"

	kafkaapi "github.com/codecrafters-io/kafka-tester/protocol/api"
)

type RequestBuilder struct {
	requestType string
	topics      map[string]map[int32][]kafkaapi.RecordBatch // topicName -> partitionIndex -> recordBatches
}

func NewRequestBuilder(requestType string) *RequestBuilder {
	return &RequestBuilder{
		requestType: requestType,
		topics:      make(map[string]map[int32][]kafkaapi.RecordBatch),
	}
}

// Use a RecordBatchBuilder maybe ?
func (b *RequestBuilder) AddRecordBatchToTopicPartition(topicName string, partitionIndex int32, messages []string) *RequestBuilder {
	if b.requestType != "produce" {
		panic("CodeCrafters Internal Error: Record batch can only be added to a produce request")
	}

	// Initialize topic if it doesn't exist
	if b.topics[topicName] == nil {
		b.topics[topicName] = make(map[int32][]kafkaapi.RecordBatch)
	}

	records := make([]kafkaapi.Record, len(messages))
	for i, message := range messages {
		records[i] = kafkaapi.Record{
			Attributes:     0,
			TimestampDelta: 0,
			Key:            nil, // No key
			Value:          []byte(message),
			Headers:        []kafkaapi.RecordHeader{},
		}
	}

	recordBatch := kafkaapi.RecordBatch{
		BaseOffset:           0,
		PartitionLeaderEpoch: -1,
		Attributes:           0,
		// Number of records - 1
		LastOffsetDelta: int32(len(messages) - 1),
		FirstTimestamp:  time.Now().UnixMilli(),
		MaxTimestamp:    time.Now().UnixMilli(),
		ProducerId:      0,
		ProducerEpoch:   0,
		BaseSequence:    0,
		Records:         records,
	}

	b.topics[topicName][partitionIndex] = append(b.topics[topicName][partitionIndex], recordBatch)
	return b
}

func (b *RequestBuilder) Build() RequestBodyI {
	switch b.requestType {
	// case "produce":
	// 	return b.BuildProduceRequest()
	case "fetch":
		return b.buildFetchRequest()
	default:
		panic("Invalid request type")
	}
}

func (b *RequestBuilder) BuildProduceRequest() kafkaapi.ProduceRequestBody {
	if len(b.topics) == 0 {
		panic("CodeCrafters Internal Error: At least one topic with partitions and record batches is required")
	}

	// Convert topics map to slice
	topicData := make([]kafkaapi.TopicData, 0, len(b.topics))
	for topicName, partitions := range b.topics {
		// Convert partitions map to slice for this topic
		partitionData := make([]kafkaapi.PartitionData, 0, len(partitions))
		for partitionIndex, recordBatches := range partitions {
			partitionData = append(partitionData, kafkaapi.PartitionData{
				Index:   partitionIndex,
				Records: recordBatches,
			})
		}

		topicData = append(topicData, kafkaapi.TopicData{
			Name:       topicName,
			Partitions: partitionData,
		})
	}

	requestBody := kafkaapi.ProduceRequestBody{
		TransactionalID: "", // Empty string for non-transactional
		Acks:            1,  // Wait for leader acknowledgment
		TimeoutMs:       0,
		Topics:          topicData,
	}

	return requestBody
}

func (b *RequestBuilder) buildFetchRequest() RequestBodyI {
	return nil
}
