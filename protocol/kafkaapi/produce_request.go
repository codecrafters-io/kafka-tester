package kafkaapi

import (
	"github.com/codecrafters-io/kafka-tester/protocol/kafkaapi/headers"
	"github.com/codecrafters-io/kafka-tester/protocol/value"
)

type ProduceRequestPartitionData struct {
	Id            value.Int32
	RecordBatches RecordBatches
}

type ProduceRequestTopicData struct {
	Name       value.CompactString
	Partitions []ProduceRequestPartitionData
}

type ProduceRequestBody struct {
	TransactionalId value.CompactNullableString
	Acks            value.Int16
	TimeoutMs       value.Int32
	Topics          []ProduceRequestTopicData
}

type ProduceRequest struct {
	Header headers.RequestHeader
	Body   ProduceRequestBody
}

// GetHeader implements the RequestI interface
func (r ProduceRequest) GetHeader() headers.RequestHeader {
	return r.Header
}
