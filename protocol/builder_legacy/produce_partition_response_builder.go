package builder_legacy

import (
	"github.com/codecrafters-io/kafka-tester/protocol/kafkaapi_legacy"
)

type producePartitionResponseBuilder struct {
	index          int32
	errorCode      int16
	baseOffset     int64
	logStartOffset int64
}

func NewProducePartitionResponseBuilder() *producePartitionResponseBuilder {
	return &producePartitionResponseBuilder{
		index:          -1,
		errorCode:      0,
		baseOffset:     0,
		logStartOffset: 0,
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

func (b *producePartitionResponseBuilder) Build() kafkaapi_legacy.ProducePartitionResponse {
	if b.index == -1 {
		panic("CodeCrafters Internal Error: index must be set before building PartitionResponse")
	}

	return kafkaapi_legacy.ProducePartitionResponse{
		Index:           b.index,
		ErrorCode:       b.errorCode,
		BaseOffset:      b.baseOffset,
		LogAppendTimeMs: -1,
		LogStartOffset:  b.logStartOffset,
		RecordErrors:    []kafkaapi_legacy.ProduceRecordError{},
		ErrorMessage:    nil,
	}
}
