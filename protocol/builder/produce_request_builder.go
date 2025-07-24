package builder

import (
	kafkaapi "github.com/codecrafters-io/kafka-tester/protocol/api"
)

type ProduceRequestBuilder struct {
	correlationId int32
	// topicName -> partitionIndex -> recordBatches
	topicData       map[string]map[int32]kafkaapi.RecordBatches
	transactionalID *string
	acks            int16
	timeoutMs       int32
}

func NewProduceRequestBuilder() *ProduceRequestBuilder {
	return &ProduceRequestBuilder{
		correlationId:   -1,
		topicData:       make(map[string]map[int32]kafkaapi.RecordBatches),
		transactionalID: nil,
		acks:            1,
		timeoutMs:       0,
	}
}

func (b *ProduceRequestBuilder) WithCorrelationId(correlationId int32) *ProduceRequestBuilder {
	b.correlationId = correlationId
	return b
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
	if b.topicData[topicName] == nil {
		b.topicData[topicName] = make(map[int32]kafkaapi.RecordBatches)
	}

	b.topicData[topicName][partitionIndex] = append(b.topicData[topicName][partitionIndex], recordBatch)
	return b
}

func (b *ProduceRequestBuilder) Build() kafkaapi.ProduceRequest {
	if len(b.topicData) == 0 {
		panic("CodeCrafters Internal Error: At least one topic with partitions and record batches is required")
	}

	// Convert topicData map to slice
	topicData := make([]kafkaapi.ProduceTopicData, 0, len(b.topicData))
	for topicName := range b.topicData {
		partitions := b.topicData[topicName]
		// Convert partitions map to slice for this topic
		partitionData := make([]kafkaapi.ProducePartitionData, 0, len(partitions))
		for partitionIndex, recordBatches := range partitions {
			partitionData = append(partitionData, kafkaapi.ProducePartitionData{
				Index:         partitionIndex,
				RecordBatches: recordBatches,
			})
		}

		topicData = append(topicData, kafkaapi.ProduceTopicData{
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
			BuildProduceRequestHeader(b.correlationId),
		Body: requestBody,
	}
}
