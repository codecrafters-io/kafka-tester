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
	b.errorCode = errorCode
	if errorCode != 0 {
		b.baseOffset = -1
		b.logStartOffset = -1
	}
	return b
}

func (b *producePartitionResponseBuilder) WithBaseOffset(baseOffset int64) *producePartitionResponseBuilder {
	b.baseOffset = baseOffset
	return b
}

func (b *producePartitionResponseBuilder) WithLogAppendTimeMs(logAppendTimeMs int64) *producePartitionResponseBuilder {
	b.logAppendTimeMs = logAppendTimeMs
	return b
}

func (b *producePartitionResponseBuilder) WithLogStartOffset(logStartOffset int64) *producePartitionResponseBuilder {
	b.logStartOffset = logStartOffset
	return b
}

func (b *producePartitionResponseBuilder) WithRecordErrors(recordErrors []kafkaapi.ProduceRecordError) *producePartitionResponseBuilder {
	b.recordErrors = recordErrors
	return b
}

func (b *producePartitionResponseBuilder) WithErrorMessage(errorMessage string) *producePartitionResponseBuilder {
	b.errorMessage = &errorMessage
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
