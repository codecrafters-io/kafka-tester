package main

import (
	"encoding/binary"
	"fmt"
)

type RecordBatch struct {
	BaseOffset           int64
	BatchLength          int32
	PartitionLeaderEpoch int32
	Magic                int8
	CRC                  int32
	Attributes           int16
	LastOffsetDelta      int32
	FirstTimestamp       int64
	MaxTimestamp         int64
	ProducerId           int64
	ProducerEpoch        int16
	BaseSequence         int32
	Records              []Record
}

type Record struct {
	Length         int32
	Attributes     int8
	TimestampDelta int64 // varint
	OffsetDelta    int64 // varint
	Key            []byte
	Value          []byte
	Headers        []RecordHeader
}

type RecordHeader struct {
	Key   []byte
	Value []byte
}

// DecodeRecordBatches decodes array of record batches from bytes
func DecodeRecordBatches(data []byte) ([]RecordBatch, error) {
	var batches []RecordBatch
	offset := 0

	for offset < len(data) {
		if offset+61 > len(data) {
			break
		}

		batch, batchSize, err := DecodeRecordBatch(data[offset:])
		if err != nil {
			return nil, fmt.Errorf("failed to decode record batch at offset %d: %w", offset, err)
		}

		batches = append(batches, *batch)
		offset += batchSize

	}

	return batches, nil
}

// DecodeRecordBatch decodes a single record batch and returns the batch and total size consumed
func DecodeRecordBatch(data []byte) (*RecordBatch, int, error) {
	if len(data) < 61 {
		return nil, 0, fmt.Errorf("insufficient data for record batch header")
	}

	batch := &RecordBatch{}
	offset := 0

	// Read header fields
	batch.BaseOffset = int64(binary.BigEndian.Uint64(data[offset : offset+8]))
	offset += 8

	batch.BatchLength = int32(binary.BigEndian.Uint32(data[offset : offset+4]))
	offset += 4

	batch.PartitionLeaderEpoch = int32(binary.BigEndian.Uint32(data[offset : offset+4]))
	offset += 4

	batch.Magic = int8(data[offset])
	offset += 1

	batch.CRC = int32(binary.BigEndian.Uint32(data[offset : offset+4]))
	offset += 4

	batch.Attributes = int16(binary.BigEndian.Uint16(data[offset : offset+2]))
	offset += 2

	batch.LastOffsetDelta = int32(binary.BigEndian.Uint32(data[offset : offset+4]))
	offset += 4

	batch.FirstTimestamp = int64(binary.BigEndian.Uint64(data[offset : offset+8]))
	offset += 8

	batch.MaxTimestamp = int64(binary.BigEndian.Uint64(data[offset : offset+8]))
	offset += 8

	batch.ProducerId = int64(binary.BigEndian.Uint64(data[offset : offset+8]))
	offset += 8

	batch.ProducerEpoch = int16(binary.BigEndian.Uint16(data[offset : offset+2]))
	offset += 2

	batch.BaseSequence = int32(binary.BigEndian.Uint32(data[offset : offset+4]))
	offset += 4

	// Read records count
	recordsCount := int32(binary.BigEndian.Uint32(data[offset : offset+4]))
	offset += 4

	// Decode records
	for i := int32(0); i < recordsCount; i++ {
		record, recordSize, err := DecodeRecord(data[offset:])
		if err != nil {
			return nil, 0, fmt.Errorf("failed to decode record %d: %w", i, err)
		}

		batch.Records = append(batch.Records, *record)
		offset += recordSize

	}

	totalBatchSize := int(batch.BatchLength) + 12 // +12 for BaseOffset and BatchLength fields
	return batch, totalBatchSize, nil
}

// DecodeRecord decodes a single record and returns the record and size consumed
func DecodeRecord(data []byte) (*Record, int, error) {
	if len(data) < 5 {
		return nil, 0, fmt.Errorf("insufficient data for record")
	}

	record := &Record{}
	offset := 0

	// Read record length (varint)
	length, lengthSize := ReadVarint(data[offset:])
	record.Length = int32(length)
	offset += lengthSize

	// Read attributes
	record.Attributes = int8(data[offset])
	offset += 1

	// Read timestamp delta (varint)
	timestampDelta, timestampSize := ReadVarint(data[offset:])
	record.TimestampDelta = timestampDelta
	offset += timestampSize

	// Read offset delta (varint)
	offsetDelta, offsetDeltaSize := ReadVarint(data[offset:])
	record.OffsetDelta = offsetDelta
	offset += offsetDeltaSize

	// Read key length and key
	keyLength, keyLengthSize := ReadVarint(data[offset:])
	offset += keyLengthSize

	if keyLength == -1 {
		record.Key = nil
	} else {
		record.Key = make([]byte, keyLength)
		copy(record.Key, data[offset:offset+int(keyLength)])
		offset += int(keyLength)
	}

	// Read value length and value
	valueLength, valueLengthSize := ReadVarint(data[offset:])
	offset += valueLengthSize

	if valueLength == -1 {
		record.Value = nil
	} else {
		record.Value = make([]byte, valueLength)
		copy(record.Value, data[offset:offset+int(valueLength)])
		offset += int(valueLength)
	}

	// Read headers count
	headersCount, headersCountSize := ReadVarint(data[offset:])
	offset += headersCountSize

	// Panic if headers are non-empty as requested
	if headersCount != -1 && headersCount != 0 {
		panic(fmt.Sprintf("Headers array is non-empty: count=%d", headersCount))
	}

	return record, int(record.Length) + lengthSize, nil
}

// ReadVarint reads a zigzag varint from data and returns value and bytes consumed
func ReadVarint(data []byte) (int64, int) {
	var result uint64
	var shift uint
	var bytesRead int

	for i := 0; i < len(data); i++ {
		b := data[i]
		bytesRead++

		result |= uint64(b&0x7F) << shift
		if (b & 0x80) == 0 {
			break
		}
		shift += 7
	}

	// ZigZag decode
	return int64((result >> 1) ^ uint64((int64(result&1) * -1))), bytesRead
}

// PrintRecordBatches removed logging
func PrintRecordBatches(batches []RecordBatch) {
	// Removed logging
}
