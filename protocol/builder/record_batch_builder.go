package builder

import (
	"time"

	"github.com/codecrafters-io/kafka-tester/protocol/kafkaapi_legacy"
)

type RecordBatchBuilder struct {
	baseOffset int64
	records    []kafkaapi_legacy.Record
}

func NewRecordBatchBuilder() *RecordBatchBuilder {
	return &RecordBatchBuilder{
		baseOffset: 0,
		records:    []kafkaapi_legacy.Record{},
	}
}

func (b *RecordBatchBuilder) WithBaseOffset(baseOffset int64) *RecordBatchBuilder {
	b.baseOffset = baseOffset
	return b
}

func (b *RecordBatchBuilder) AddRecord(key []byte, value []byte, headers []kafkaapi_legacy.RecordHeader) *RecordBatchBuilder {
	record := kafkaapi_legacy.Record{
		Attributes:     0,
		TimestampDelta: 0,
		Key:            key,
		Value:          value,
		Headers:        headers,
	}
	b.records = append(b.records, record)
	return b
}

func (b *RecordBatchBuilder) AddStringRecord(value string) *RecordBatchBuilder {
	return b.AddRecord(nil, []byte(value), []kafkaapi_legacy.RecordHeader{})
}

func (b *RecordBatchBuilder) Build() kafkaapi_legacy.RecordBatch {
	if len(b.records) == 0 {
		panic("CodeCrafters Internal Error: At least one record is required in RecordBatch")
	}

	now := time.Now().UnixMilli()
	return kafkaapi_legacy.RecordBatch{
		BaseOffset:           b.baseOffset,
		PartitionLeaderEpoch: 0,
		Attributes:           0,
		LastOffsetDelta:      int32(len(b.records) - 1),
		FirstTimestamp:       now,
		MaxTimestamp:         now,
		ProducerId:           0,
		ProducerEpoch:        0,
		BaseSequence:         0,
		Records:              b.records,
	}
}
