package builder

import (
	"time"

	kafkaapi "github.com/codecrafters-io/kafka-tester/protocol/api"
)

type RecordBatchBuilder struct {
	baseOffset           int64
	partitionLeaderEpoch int32
	attributes           int16
	baseTimestamp        int64
	maxTimestamp         int64
	producerId           int64
	producerEpoch        int16
	baseSequence         int32
	records              []kafkaapi.Record
}

func NewRecordBatchBuilder() *RecordBatchBuilder {
	now := time.Now().UnixMilli()
	return &RecordBatchBuilder{
		baseOffset:           0,
		partitionLeaderEpoch: -1,
		attributes:           0,
		baseTimestamp:        now,
		maxTimestamp:         now,
		producerId:           0,
		producerEpoch:        0,
		baseSequence:         0,
		records:              []kafkaapi.Record{},
	}
}

func (b *RecordBatchBuilder) WithBaseOffset(baseOffset int64) *RecordBatchBuilder {
	b.baseOffset = baseOffset
	return b
}

func (b *RecordBatchBuilder) WithPartitionLeaderEpoch(epoch int32) *RecordBatchBuilder {
	b.partitionLeaderEpoch = epoch
	return b
}

func (b *RecordBatchBuilder) WithTimestamp(timestamp int64) *RecordBatchBuilder {
	b.baseTimestamp = timestamp
	b.maxTimestamp = timestamp
	return b
}

func (b *RecordBatchBuilder) WithBaseSequence(sequence int32) *RecordBatchBuilder {
	b.baseSequence = sequence
	return b
}

func (b *RecordBatchBuilder) AddRecord(key []byte, value []byte, headers []kafkaapi.RecordHeader) *RecordBatchBuilder {
	record := kafkaapi.Record{
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
	return b.AddRecord(nil, []byte(value), []kafkaapi.RecordHeader{})
}

func (b *RecordBatchBuilder) AddStringRecords(messages []string) *RecordBatchBuilder {
	for _, message := range messages {
		b.AddStringRecord(message)
	}
	return b
}

func (b *RecordBatchBuilder) Build() kafkaapi.RecordBatch {
	if len(b.records) == 0 {
		panic("CodeCrafters Internal Error: At least one record is required in RecordBatch")
	}

	return kafkaapi.RecordBatch{
		BaseOffset:           b.baseOffset,
		PartitionLeaderEpoch: b.partitionLeaderEpoch,
		Attributes:           b.attributes,
		LastOffsetDelta:      int32(len(b.records) - 1),
		FirstTimestamp:       b.baseTimestamp,
		MaxTimestamp:         b.maxTimestamp,
		ProducerId:           b.producerId,
		ProducerEpoch:        b.producerEpoch,
		BaseSequence:         b.baseSequence,
		Records:              b.records,
	}
}
