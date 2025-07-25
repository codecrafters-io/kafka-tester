package builder

import (
	kafkaapi "github.com/codecrafters-io/kafka-tester/protocol/api"
)

type ProduceRequestBuilder struct {
	correlationId int32
	topicData     []kafkaapi.ProduceTopicData
}

func NewProduceRequestBuilder() *ProduceRequestBuilder {
	return &ProduceRequestBuilder{
		correlationId: -1,
		topicData:     make([]kafkaapi.ProduceTopicData, 0),
	}
}

func (b *ProduceRequestBuilder) WithCorrelationId(correlationId int32) *ProduceRequestBuilder {
	b.correlationId = correlationId
	return b
}

func (b *ProduceRequestBuilder) AddRecordBatch(topicName string, partitionIndex int32, recordBatch kafkaapi.RecordBatch) *ProduceRequestBuilder {
	topicData := kafkaapi.ProduceTopicData{
		Name: topicName,
		Partitions: []kafkaapi.ProducePartitionData{
			{
				Index:         partitionIndex,
				RecordBatches: []kafkaapi.RecordBatch{recordBatch},
			},
		},
	}

	b.topicData = append(b.topicData, topicData)
	return b
}

func (b *ProduceRequestBuilder) Build() kafkaapi.ProduceRequest {
	if len(b.topicData) == 0 {
		panic("CodeCrafters Internal Error: At least one topic with partitions and record batches is required")
	}

	requestBody := kafkaapi.ProduceRequestBody{
		TransactionalID: nil,
		Acks:            1,
		TimeoutMs:       0,
		Topics:          b.topicData,
	}

	return kafkaapi.ProduceRequest{
		Header: NewRequestHeaderBuilder().
			BuildProduceRequestHeader(b.correlationId),
		Body: requestBody,
	}
}
