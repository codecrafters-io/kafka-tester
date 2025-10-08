package kafkaapi

import (
	"github.com/codecrafters-io/kafka-tester/protocol/kafkaapi/headers"
	"github.com/codecrafters-io/kafka-tester/protocol/value"
)

type ProduceResponse struct {
	Header headers.ResponseHeader
	Body   ProduceResponseBody
}

type ProduceResponseBody struct {
	Topics         []ProduceResponseTopicData
	ThrottleTimeMs value.Int32
}

type ProduceResponseTopicData struct {
	Name       value.CompactString
	Partitions []ProduceResponsePartitionData
}

type ProduceResponsePartitionData struct {
	Id              value.Int32
	ErrorCode       value.Int16
	BaseOffset      value.Int64
	LogAppendTimeMs value.Int64
	LogStartOffset  value.Int64
	RecordErrors    []ProduceResponseRecordErrorData
	ErrorMessage    value.CompactNullableString
}

type ProduceResponseRecordErrorData struct {
	BatchIndex   value.Int32
	ErrorMessage value.CompactNullableString
}
