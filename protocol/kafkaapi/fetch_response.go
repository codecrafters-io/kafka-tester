package kafkaapi

import "github.com/codecrafters-io/kafka-tester/protocol/value"

type FetchResponse struct {
	ThrottleTimeMs value.Int32
	ErrorCode      value.Int16
	SessionId      value.Int32
	TopicResponses []TopicResponse
}

type TopicResponse struct {
	UUID               value.UUID
	PartitionResponses []PartitionResponse
}

type PartitionResponse struct {
	Id                  value.Int32
	ErrorCode           value.Int16
	HighWatermark       value.Int64
	LastStableOffset    value.Int64
	LogStartOffset      value.Int64
	AbortedTransactions []AbortedTransaction
	RecordBatches       []RecordBatch
	PreferedReadReplica value.Int64
}

type AbortedTransaction struct {
	ProducerID  value.Int64
	FirstOffset value.Int64
}
