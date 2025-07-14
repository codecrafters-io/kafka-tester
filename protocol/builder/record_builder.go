package builder

import (
	"time"

	kafkaapi "github.com/codecrafters-io/kafka-tester/protocol/api"
)

// RecordBuilder

// RecordBuilder builds individual Record objects
type RecordBuilder struct {
	attributes     int8
	timestampDelta int64
	offsetDelta    int32
	key            []byte
	value          []byte
	headers        []kafkaapi.RecordHeader
}

// NewRecordBuilder creates a new RecordBuilder with default values
func NewRecordBuilder() RecordBuilderI {
	return &RecordBuilder{
		attributes:     0,
		timestampDelta: 0,
		offsetDelta:    0,
		key:            nil,
		value:          nil,
		headers:        []kafkaapi.RecordHeader{},
	}
}

// WithAttributes sets the record attributes
func (rb *RecordBuilder) WithAttributes(attributes int8) RecordBuilderI {
	rb.attributes = attributes
	return rb
}

// WithTimestampDelta sets the timestamp delta
func (rb *RecordBuilder) WithTimestampDelta(timestampDelta int64) RecordBuilderI {
	rb.timestampDelta = timestampDelta
	return rb
}

// WithOffsetDelta sets the offset delta
func (rb *RecordBuilder) WithOffsetDelta(offsetDelta int32) RecordBuilderI {
	rb.offsetDelta = offsetDelta
	return rb
}

// WithKey sets the record key
func (rb *RecordBuilder) WithKey(key []byte) RecordBuilderI {
	rb.key = key
	return rb
}

// WithStringKey sets the record key as a string
func (rb *RecordBuilder) WithStringKey(key string) RecordBuilderI {
	if key == "" {
		rb.key = nil
	} else {
		rb.key = []byte(key)
	}
	return rb
}

// WithValue sets the record value
func (rb *RecordBuilder) WithValue(value []byte) RecordBuilderI {
	rb.value = value
	return rb
}

// WithStringValue sets the record value as a string
func (rb *RecordBuilder) WithStringValue(value string) RecordBuilderI {
	if value == "" {
		rb.value = nil
	} else {
		rb.value = []byte(value)
	}
	return rb
}

// WithHeader adds a header to the record
func (rb *RecordBuilder) WithHeader(key string, value []byte) RecordBuilderI {
	rb.headers = append(rb.headers, kafkaapi.RecordHeader{
		Key:   key,
		Value: value,
	})
	return rb
}

// WithStringHeader adds a header with string value to the record
func (rb *RecordBuilder) WithStringHeader(key string, value string) RecordBuilderI {
	rb.headers = append(rb.headers, kafkaapi.RecordHeader{
		Key:   key,
		Value: []byte(value),
	})
	return rb
}

// Build creates the Record
func (rb *RecordBuilder) Build() kafkaapi.Record {
	return kafkaapi.Record{
		Attributes:     rb.attributes,
		TimestampDelta: rb.timestampDelta,
		OffsetDelta:    rb.offsetDelta,
		Key:            rb.key,
		Value:          rb.value,
		Headers:        rb.headers,
	}
}

// RecordBatchBuilder

// RecordBatchBuilder builds RecordBatch objects
type RecordBatchBuilder struct {
	baseOffset           int64
	partitionLeaderEpoch int32
	attributes           int16
	lastOffsetDelta      int32
	firstTimestamp       int64
	maxTimestamp         int64
	producerId           int64
	producerEpoch        int16
	baseSequence         int32
	records              []kafkaapi.Record
}

// NewRecordBatchBuilder creates a new RecordBatchBuilder with default values
func NewRecordBatchBuilder() RecordBatchBuilderI {
	now := time.Now().UnixMilli()
	return &RecordBatchBuilder{
		baseOffset:           0,
		partitionLeaderEpoch: -1,
		attributes:           0,
		lastOffsetDelta:      0,
		firstTimestamp:       now,
		maxTimestamp:         now,
		producerId:           0,
		producerEpoch:        0,
		baseSequence:         0,
		records:              []kafkaapi.Record{},
	}
}

// WithBaseOffset sets the base offset
func (rbb *RecordBatchBuilder) WithBaseOffset(baseOffset int64) RecordBatchBuilderI {
	rbb.baseOffset = baseOffset
	return rbb
}

// WithPartitionLeaderEpoch sets the partition leader epoch
func (rbb *RecordBatchBuilder) WithPartitionLeaderEpoch(epoch int32) RecordBatchBuilderI {
	rbb.partitionLeaderEpoch = epoch
	return rbb
}

// WithAttributes sets the batch attributes
func (rbb *RecordBatchBuilder) WithAttributes(attributes int16) RecordBatchBuilderI {
	rbb.attributes = attributes
	return rbb
}

// WithTimestamps sets both first and max timestamps
func (rbb *RecordBatchBuilder) WithTimestamps(firstTimestamp, maxTimestamp int64) RecordBatchBuilderI {
	rbb.firstTimestamp = firstTimestamp
	rbb.maxTimestamp = maxTimestamp
	return rbb
}

// WithFirstTimestamp sets the first timestamp
func (rbb *RecordBatchBuilder) WithFirstTimestamp(timestamp int64) RecordBatchBuilderI {
	rbb.firstTimestamp = timestamp
	return rbb
}

// WithMaxTimestamp sets the max timestamp
func (rbb *RecordBatchBuilder) WithMaxTimestamp(timestamp int64) RecordBatchBuilderI {
	rbb.maxTimestamp = timestamp
	return rbb
}

// WithProducer sets the producer ID and epoch
func (rbb *RecordBatchBuilder) WithProducer(producerId int64, producerEpoch int16) RecordBatchBuilderI {
	rbb.producerId = producerId
	rbb.producerEpoch = producerEpoch
	return rbb
}

// WithProducerId sets the producer ID
func (rbb *RecordBatchBuilder) WithProducerId(producerId int64) RecordBatchBuilderI {
	rbb.producerId = producerId
	return rbb
}

// WithProducerEpoch sets the producer epoch
func (rbb *RecordBatchBuilder) WithProducerEpoch(epoch int16) RecordBatchBuilderI {
	rbb.producerEpoch = epoch
	return rbb
}

// WithBaseSequence sets the base sequence
func (rbb *RecordBatchBuilder) WithBaseSequence(sequence int32) RecordBatchBuilderI {
	rbb.baseSequence = sequence
	return rbb
}

// AddRecord adds a record to the batch
func (rbb *RecordBatchBuilder) AddRecord(record kafkaapi.Record) RecordBatchBuilderI {
	rbb.records = append(rbb.records, record)
	return rbb
}

// AddRecordFromBuilder adds a record using a RecordBuilder
func (rbb *RecordBatchBuilder) AddRecordFromBuilder(recordBuilder RecordBuilderI) RecordBatchBuilderI {
	rbb.records = append(rbb.records, recordBuilder.Build())
	return rbb
}

// AddSimpleRecord adds a simple record with just a value
func (rbb *RecordBatchBuilder) AddSimpleRecord(value string) RecordBatchBuilderI {
	record := NewRecordBuilder().WithStringValue(value).Build()
	rbb.records = append(rbb.records, record)
	return rbb
}

// AddRecordWithKeyValue adds a record with key and value
func (rbb *RecordBatchBuilder) AddRecordWithKeyValue(key, value string) RecordBatchBuilderI {
	record := NewRecordBuilder().
		WithStringKey(key).
		WithStringValue(value).
		Build()
	rbb.records = append(rbb.records, record)
	return rbb
}

// AddRecords adds multiple records from strings
func (rbb *RecordBatchBuilder) AddRecords(values []string) RecordBatchBuilderI {
	for _, value := range values {
		rbb.AddSimpleRecord(value)
	}
	return rbb
}

// Build creates the RecordBatch
func (rbb *RecordBatchBuilder) Build() kafkaapi.RecordBatch {
	// Calculate last offset delta
	lastOffsetDelta := int32(0)
	if len(rbb.records) > 0 {
		lastOffsetDelta = int32(len(rbb.records) - 1)
	}

	return kafkaapi.RecordBatch{
		BaseOffset:           rbb.baseOffset,
		PartitionLeaderEpoch: rbb.partitionLeaderEpoch,
		Attributes:           rbb.attributes,
		LastOffsetDelta:      lastOffsetDelta,
		FirstTimestamp:       rbb.firstTimestamp,
		MaxTimestamp:         rbb.maxTimestamp,
		ProducerId:           rbb.producerId,
		ProducerEpoch:        rbb.producerEpoch,
		BaseSequence:         rbb.baseSequence,
		Records:              rbb.records,
	}
}
