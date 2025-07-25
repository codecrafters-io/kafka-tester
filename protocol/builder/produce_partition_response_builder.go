package builder

import (
	kafkaapi "github.com/codecrafters-io/kafka-tester/protocol/api"
)

type producePartitionResponseBuilder struct {
	index           int32
	errorCode       int16
	baseOffset      int64
	logAppendTimeMs int64
	logStartOffset  int64
	recordErrors    []kafkaapi.ProduceRecordError
	errorMessage    *string
}

func NewProducePartitionResponseBuilder() *producePartitionResponseBuilder {
	return &producePartitionResponseBuilder{
		index:           -1,
		errorCode:       0,
		baseOffset:      0,
		logAppendTimeMs: -1,
		logStartOffset:  0,
		recordErrors:    []kafkaapi.ProduceRecordError{},
		errorMessage:    nil,
	}
}

func (b *producePartitionResponseBuilder) WithIndex(index int32) *producePartitionResponseBuilder {
	b.index = index
	return b
}

func (b *producePartitionResponseBuilder) WithError(errorCode int16) *producePartitionResponseBuilder {
	if errorCode == 0 {
		panic("CodeCrafters Internal Error: errorCode can't be set to 0 (NO_ERROR)")
	}

	b.errorCode = errorCode
	b.baseOffset = -1
	b.logStartOffset = -1

	return b
}

func (b *producePartitionResponseBuilder) Build() kafkaapi.ProducePartitionResponse {
	if b.index == -1 {
		panic("CodeCrafters Internal Error: index must be set before building PartitionResponse")
	}

	return kafkaapi.ProducePartitionResponse{
		Index:           b.index,
		ErrorCode:       b.errorCode,
		BaseOffset:      b.baseOffset,
		LogAppendTimeMs: b.logAppendTimeMs,
		LogStartOffset:  b.logStartOffset,
		RecordErrors:    b.recordErrors,
		ErrorMessage:    b.errorMessage,
	}
}
