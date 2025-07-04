package builder

import (
	"time"

	kafkaapi "github.com/codecrafters-io/kafka-tester/protocol/api"
)

type RequestBuilder struct {
	requestType string
	topicName   string
	recordBatch kafkaapi.RecordBatch
}

func NewRequestBuilder(requestType string) *RequestBuilder {
	return &RequestBuilder{
		requestType: requestType,
	}
}

func (b *RequestBuilder) WithTopic(topicName string) *RequestBuilder {
	b.topicName = topicName
	return b
}

func (b *RequestBuilder) WithRecordBatch(message string) *RequestBuilder {
	// Use a RecordBatchBuilder maybe ?
	if b.requestType != "produce" {
		panic("CodeCrafters Internal Error: Record batch can only be added to a produce request")
	}

	recordBatch := kafkaapi.RecordBatch{
		BaseOffset:           0,
		PartitionLeaderEpoch: -1,
		Attributes:           0,
		LastOffsetDelta:      0,
		FirstTimestamp:       time.Now().UnixMilli(),
		MaxTimestamp:         time.Now().UnixMilli(),
		ProducerId:           0,
		ProducerEpoch:        0,
		BaseSequence:         0,
		Records: []kafkaapi.Record{
			{
				Attributes:     0,
				TimestampDelta: 0,
				Key:            nil, // No key
				Value:          []byte(message),
				Headers:        []kafkaapi.RecordHeader{},
			},
		},
	}

	b.recordBatch = recordBatch
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

	requestBody := kafkaapi.ProduceRequestBody{
		TransactionalID: "", // Empty string for non-transactional
		Acks:            1,  // Wait for leader acknowledgment
		TimeoutMs:       0,
		Topics: []kafkaapi.TopicData{
			{
				Name: b.topicName, // Try to produce to a test topic
				Partitions: []kafkaapi.PartitionData{
					{
						Index:   0,
						Records: []kafkaapi.RecordBatch{},
					},
				},
			},
		},
	}

	return requestBody
}

func (b *RequestBuilder) buildFetchRequest() RequestBodyI {
	return nil
}
