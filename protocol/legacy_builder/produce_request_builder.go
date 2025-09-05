package legacy_builder

import (
	"github.com/codecrafters-io/kafka-tester/protocol/legacy_kafkaapi"
)

type ProduceRequestBuilder struct {
	correlationId int32
	topicData     []legacy_kafkaapi.ProduceTopicData
}

func NewProduceRequestBuilder() *ProduceRequestBuilder {
	return &ProduceRequestBuilder{
		correlationId: -1,
		topicData:     make([]legacy_kafkaapi.ProduceTopicData, 0),
	}
}

func (b *ProduceRequestBuilder) WithCorrelationId(correlationId int32) *ProduceRequestBuilder {
	b.correlationId = correlationId
	return b
}

func (b *ProduceRequestBuilder) AddRecordBatch(topicName string, partitionIndex int32, recordBatch legacy_kafkaapi.RecordBatch) *ProduceRequestBuilder {
	partitionData := legacy_kafkaapi.ProducePartitionData{
		Index:         partitionIndex,
		RecordBatches: []legacy_kafkaapi.RecordBatch{recordBatch},
	}

	for i := range b.topicData {
		if b.topicData[i].Name == topicName {
			b.topicData[i].Partitions = append(b.topicData[i].Partitions, partitionData)
			return b
		}
	}
	topicData := legacy_kafkaapi.ProduceTopicData{
		Name: topicName,
		Partitions: []legacy_kafkaapi.ProducePartitionData{
			partitionData,
		},
	}

	b.topicData = append(b.topicData, topicData)
	return b
}

func (b *ProduceRequestBuilder) Build() legacy_kafkaapi.ProduceRequest {
	if len(b.topicData) == 0 {
		panic("CodeCrafters Internal Error: At least one topic with partitions and record batches is required")
	}

	requestBody := legacy_kafkaapi.ProduceRequestBody{
		TransactionalID: nil,
		Acks:            1,
		TimeoutMs:       0,
		Topics:          b.topicData,
	}

	return legacy_kafkaapi.ProduceRequest{
		Header: NewRequestHeaderBuilder().
			BuildProduceRequestHeader(b.correlationId),
		Body: requestBody,
	}
}
