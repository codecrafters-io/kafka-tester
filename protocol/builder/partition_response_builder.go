package builder

import (
	kafkaapi "github.com/codecrafters-io/kafka-tester/protocol/api"
)

type PartitionResponseBuilder struct {
	errorCode       int16
	baseOffset      int64
	logAppendTimeMs int64
	logStartOffset  int64
	recordErrors    []kafkaapi.RecordError
	errorMessage    *string
}

func NewPartitionResponseBuilder() *PartitionResponseBuilder {
	return &PartitionResponseBuilder{
		errorCode:       0,
		baseOffset:      0,
		logAppendTimeMs: -1,
		logStartOffset:  0,
		recordErrors:    []kafkaapi.RecordError{},
		errorMessage:    nil,
	}
}

func (b *PartitionResponseBuilder) WithError(errorCode int16) *PartitionResponseBuilder {
	b.errorCode = errorCode
	if errorCode != 0 {
		// When there's an error, set offsets to -1 (this matches the fixtures)
		b.baseOffset = -1
		b.logStartOffset = -1
	}
	return b
}

func (b *PartitionResponseBuilder) WithBaseOffset(baseOffset int64) *PartitionResponseBuilder {
	b.baseOffset = baseOffset
	return b
}

func (b *PartitionResponseBuilder) WithLogAppendTimeMs(logAppendTimeMs int64) *PartitionResponseBuilder {
	b.logAppendTimeMs = logAppendTimeMs
	return b
}

func (b *PartitionResponseBuilder) WithLogStartOffset(logStartOffset int64) *PartitionResponseBuilder {
	b.logStartOffset = logStartOffset
	return b
}

func (b *PartitionResponseBuilder) WithRecordErrors(recordErrors []kafkaapi.RecordError) *PartitionResponseBuilder {
	b.recordErrors = recordErrors
	return b
}

func (b *PartitionResponseBuilder) WithErrorMessage(errorMessage string) *PartitionResponseBuilder {
	b.errorMessage = &errorMessage
	return b
}

func (b *PartitionResponseBuilder) Build(partitionIndex int32) kafkaapi.ProducePartitionResponse {
	return kafkaapi.ProducePartitionResponse{
		Index:           partitionIndex,
		ErrorCode:       b.errorCode,
		BaseOffset:      b.baseOffset,
		LogAppendTimeMs: b.logAppendTimeMs,
		LogStartOffset:  b.logStartOffset,
		RecordErrors:    b.recordErrors,
		ErrorMessage:    b.errorMessage,
	}
}
