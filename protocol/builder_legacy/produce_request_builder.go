package builder_legacy

import (
	"github.com/codecrafters-io/kafka-tester/protocol/kafkaapi_legacy"
)

type ProduceRequestBuilder struct {
	correlationId int32
	topicData     []kafkaapi_legacy.ProduceTopicData
}

func NewProduceRequestBuilder() *ProduceRequestBuilder {
	return &ProduceRequestBuilder{
		correlationId: -1,
		topicData:     make([]kafkaapi_legacy.ProduceTopicData, 0),
	}
}

func (b *ProduceRequestBuilder) WithCorrelationId(correlationId int32) *ProduceRequestBuilder {
	b.correlationId = correlationId
	return b
}

func (b *ProduceRequestBuilder) AddRecordBatch(topicName string, partitionIndex int32, recordBatch kafkaapi_legacy.RecordBatch) *ProduceRequestBuilder {
	partitionData := kafkaapi_legacy.ProducePartitionData{
		Index:         partitionIndex,
		RecordBatches: []kafkaapi_legacy.RecordBatch{recordBatch},
	}

	for i := range b.topicData {
		if b.topicData[i].Name == topicName {
			b.topicData[i].Partitions = append(b.topicData[i].Partitions, partitionData)
			return b
		}
	}
	topicData := kafkaapi_legacy.ProduceTopicData{
		Name: topicName,
		Partitions: []kafkaapi_legacy.ProducePartitionData{
			partitionData,
		},
	}

	b.topicData = append(b.topicData, topicData)
	return b
}

func (b *ProduceRequestBuilder) Build() kafkaapi_legacy.ProduceRequest {
	if len(b.topicData) == 0 {
		panic("CodeCrafters Internal Error: At least one topic with partitions and record batches is required")
	}

	requestBody := kafkaapi_legacy.ProduceRequestBody{
		TransactionalID: nil,
		Acks:            1,
		TimeoutMs:       0,
		Topics:          b.topicData,
	}

	return kafkaapi_legacy.ProduceRequest{
		Header: NewRequestHeaderBuilder().
			BuildProduceRequestHeader(b.correlationId),
		Body: requestBody,
	}
}
