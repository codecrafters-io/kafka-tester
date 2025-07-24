package builder

import (
	kafkaapi "github.com/codecrafters-io/kafka-tester/protocol/api"
)

type ProduceRequestBuilder struct {
	// topicName -> partitionIndex -> recordBatches
	topics          map[string]map[int32]kafkaapi.RecordBatches
	transactionalID *string
	acks            int16
	timeoutMs       int32
}

func NewProduceRequestBuilder() *ProduceRequestBuilder {
	return &ProduceRequestBuilder{
		topics:          make(map[string]map[int32]kafkaapi.RecordBatches),
		transactionalID: nil,
		acks:            1,
		timeoutMs:       0,
	}
}

func (b *ProduceRequestBuilder) WithTransactionalID(transactionalID *string) *ProduceRequestBuilder {
	b.transactionalID = transactionalID
	return b
}

func (b *ProduceRequestBuilder) WithAcks(acks int16) *ProduceRequestBuilder {
	b.acks = acks
	return b
}

func (b *ProduceRequestBuilder) WithTimeoutMs(timeoutMs int32) *ProduceRequestBuilder {
	b.timeoutMs = timeoutMs
	return b
}

func (b *ProduceRequestBuilder) AddRecordBatch(topicName string, partitionIndex int32, recordBatch kafkaapi.RecordBatch) *ProduceRequestBuilder {
	if b.topics[topicName] == nil {
		b.topics[topicName] = make(map[int32]kafkaapi.RecordBatches)
	}

	b.topics[topicName][partitionIndex] = append(b.topics[topicName][partitionIndex], recordBatch)
	return b
}

func (b *ProduceRequestBuilder) Build(correlationId int32) kafkaapi.ProduceRequest {
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
				Index:         partitionIndex,
				RecordBatches: recordBatches,
			})
		}

		topicData = append(topicData, kafkaapi.TopicData{
			Name:       topicName,
			Partitions: partitionData,
		})
	}

	requestBody := kafkaapi.ProduceRequestBody{
		TransactionalID: b.transactionalID,
		Acks:            b.acks,
		TimeoutMs:       b.timeoutMs,
		Topics:          topicData,
	}

	return kafkaapi.ProduceRequest{
		Header: NewRequestHeaderBuilder().
			BuildProduceRequestHeader(correlationId),
		Body: requestBody,
	}
}
