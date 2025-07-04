package builder

import (
	"time"

	kafkaapi "github.com/codecrafters-io/kafka-tester/protocol/api"
)

type RequestBuilder struct {
	requestType string
	topicName   string
	partitions  map[int32][]kafkaapi.RecordBatch
}

func NewRequestBuilder(requestType string) *RequestBuilder {
	return &RequestBuilder{
		requestType: requestType,
		partitions:  make(map[int32][]kafkaapi.RecordBatch),
	}
}

func (b *RequestBuilder) WithTopic(topicName string) *RequestBuilder {
	b.topicName = topicName
	return b
}

// Use a RecordBatchBuilder maybe ?
func (b *RequestBuilder) AddRecordBatchToPartition(partitionIndex int32, messages []string) *RequestBuilder {
	if b.requestType != "produce" {
		panic("CodeCrafters Internal Error: Record batch can only be added to a produce request")
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

	b.partitions[partitionIndex] = append(b.partitions[partitionIndex], recordBatch)
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
	if b.topicName == "" {
		panic("CodeCrafters Internal Error: Topic name is required to build a produce request")
	}

	if len(b.partitions) == 0 {
		panic("CodeCrafters Internal Error: At least one partition with record batches is required")
	}

	// Convert partitions map to slice
	partitionData := make([]kafkaapi.PartitionData, 0, len(b.partitions))
	for partitionIndex, recordBatches := range b.partitions {
		partitionData = append(partitionData, kafkaapi.PartitionData{
			Index:   partitionIndex,
			Records: recordBatches,
		})
	}

	requestBody := kafkaapi.ProduceRequestBody{
		TransactionalID: "", // Empty string for non-transactional
		Acks:            1,  // Wait for leader acknowledgment
		TimeoutMs:       0,
		Topics: []kafkaapi.TopicData{
			{
				Name:       b.topicName,
				Partitions: partitionData,
			},
		},
	}

	return requestBody
}

func (b *RequestBuilder) buildFetchRequest() RequestBodyI {
	return nil
}
