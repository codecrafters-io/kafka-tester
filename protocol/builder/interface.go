package builder

import (
	kafkaapi "github.com/codecrafters-io/kafka-tester/protocol/api"
	"github.com/codecrafters-io/kafka-tester/protocol/encoder"
)

type RequestHeaderI interface {
	Encode(*encoder.RealEncoder)
}

type RequestBodyI interface {
	Encode(*encoder.RealEncoder)
}

type KafkaRequestI interface {
	GetHeader() RequestHeaderI
	GetBody() RequestBodyI
}

// type ResponseBuilderI interface {
// 	// TODO: ResponseI
// 	Build(correlationId int32) kafkaapi.ProduceResponse
// }

type ProduceResponseBuilderI interface {
	AddTopicPartitionResponse(topicName string, partitionIndex int32, errorCode int16) *ProduceResponseBuilder
	AddTopicPartitionResponseWithBaseOffset(topicName string, partitionIndex int32, errorCode int16, baseOffset int64) *ProduceResponseBuilder
	Build(correlationId int32) kafkaapi.ProduceResponse
}

type ApiVersionsResponseBuilderI interface {
	Build(correlationId int32) kafkaapi.ApiVersionsResponse
}

type ResponseHeaderBuilderI interface {
	Build() kafkaapi.ResponseHeader
}

// type RequestBuilderI interface {
// 	Build() RequestBodyI
// }

type ApiVersionsRequestBuilderI interface {
	Build() kafkaapi.ApiVersionsRequestBody
}

type ProduceRequestBuilderI interface {
	AddRecordBatchToTopicPartition(topicName string, partitionIndex int32, messages []string) ProduceRequestBuilderI
	WithBaseSequence(baseSequence int32) ProduceRequestBuilderI
	Build() kafkaapi.ProduceRequestBody
}

type RecordBuilderI interface {
	WithAttributes(attributes int8) RecordBuilderI
	WithTimestampDelta(timestampDelta int64) RecordBuilderI
	WithOffsetDelta(offsetDelta int32) RecordBuilderI
	WithKey(key []byte) RecordBuilderI
	WithStringKey(key string) RecordBuilderI
	WithValue(value []byte) RecordBuilderI
	WithStringValue(value string) RecordBuilderI
	WithHeader(key string, value []byte) RecordBuilderI
	WithStringHeader(key string, value string) RecordBuilderI
	Build() kafkaapi.Record
}

type RecordBatchBuilderI interface {
	WithBaseOffset(baseOffset int64) RecordBatchBuilderI
	WithPartitionLeaderEpoch(epoch int32) RecordBatchBuilderI
	WithAttributes(attributes int16) RecordBatchBuilderI
	WithTimestamps(firstTimestamp, maxTimestamp int64) RecordBatchBuilderI
	WithFirstTimestamp(timestamp int64) RecordBatchBuilderI
	WithMaxTimestamp(timestamp int64) RecordBatchBuilderI
	WithProducer(producerId int64, producerEpoch int16) RecordBatchBuilderI
	WithProducerId(producerId int64) RecordBatchBuilderI
	WithProducerEpoch(epoch int16) RecordBatchBuilderI
	WithBaseSequence(sequence int32) RecordBatchBuilderI
	AddRecord(record kafkaapi.Record) RecordBatchBuilderI
	AddRecordFromBuilder(recordBuilder RecordBuilderI) RecordBatchBuilderI
	AddSimpleRecord(value string) RecordBatchBuilderI
	AddRecordWithKeyValue(key, value string) RecordBatchBuilderI
	AddRecords(values []string) RecordBatchBuilderI
	Build() kafkaapi.RecordBatch
}
