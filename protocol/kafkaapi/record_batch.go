package kafkaapi

import (
	"hash/crc32"

	"github.com/codecrafters-io/kafka-tester/protocol/encoder"
)

type RecordBatches []RecordBatch

func (rbs RecordBatches) Encode(pe *encoder.Encoder) {
	for _, rb := range rbs {
		rb.Encode(pe)
	}
}

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

func (rb RecordBatch) Encode(pe *encoder.Encoder) {
	propertiesEncoder := encoder.NewEncoder()
	propertiesEncoder.WriteInt16(rb.Attributes)
	propertiesEncoder.WriteInt32(rb.LastOffsetDelta)
	propertiesEncoder.WriteInt64(rb.FirstTimestamp)
	propertiesEncoder.WriteInt64(rb.MaxTimestamp)
	propertiesEncoder.WriteInt64(rb.ProducerId)
	propertiesEncoder.WriteInt16(rb.ProducerEpoch)
	propertiesEncoder.WriteInt32(rb.BaseSequence)
	propertiesEncoder.WriteInt32(int32(len(rb.Records)))

	for i, record := range rb.Records {
		record.OffsetDelta = int32(i) // Offset Deltas are consecutive numerals from 0 to N-1
		// We can set them programmatically as we know the order of the records
		record.Encode(propertiesEncoder)
	}

	propertiesEncoderBytes := propertiesEncoder.Bytes()
	crcData := propertiesEncoder.Bytes()
	computedChecksum := crc32.Checksum(crcData, crc32.MakeTable(crc32.Castagnoli))
	rb.CRC = int32(computedChecksum)
	rb.BatchLength = int32(len(propertiesEncoderBytes) + 4 + 2 + 4) // partitionLeaderEpoch + magic value + CRC

	// Encode everything now
	pe.WriteInt64(rb.BaseOffset)
	pe.WriteInt32(rb.BatchLength)
	pe.WriteInt32(rb.PartitionLeaderEpoch)
	pe.WriteInt8(2)       // Magic value is 2
	pe.WriteInt32(rb.CRC) // CRC placeholder
	pe.WriteRawBytes(propertiesEncoderBytes)
}

type Record struct {
	Length         int32
	Attributes     int8
	TimestampDelta int64
	OffsetDelta    int32
	Key            []byte
	Value          []byte
	Headers        []RecordHeader
}

func (r Record) Encode(pe *encoder.Encoder) {
	propertiesEncoder := encoder.NewEncoder()

	propertiesEncoder.WriteInt8(r.Attributes)
	propertiesEncoder.WriteVarint(r.TimestampDelta)
	propertiesEncoder.WriteVarint(int64(r.OffsetDelta))
	if r.Key == nil {
		propertiesEncoder.WriteCompactBytes([]byte{})
	} else {
		propertiesEncoder.WriteCompactBytes(r.Key)
	}
	propertiesEncoder.WriteVarint(int64(len(r.Value)))
	propertiesEncoder.WriteRawBytes(r.Value)
	propertiesEncoder.WriteVarint(int64(len(r.Headers)))

	for _, header := range r.Headers {
		header.Encode(propertiesEncoder)
	}

	propertiesEncoderBytes := propertiesEncoder.Bytes()

	pe.WriteVarint(int64(len(propertiesEncoderBytes)))
	pe.WriteRawBytes(propertiesEncoderBytes)
}

type RecordHeader struct {
	Key   string
	Value []byte
}

func (rh RecordHeader) Encode(pe *encoder.Encoder) {
	pe.WriteVarint(int64(len(rh.Key)))
	pe.WriteRawBytes([]byte(rh.Key))
	pe.WriteVarint(int64(len(rh.Value)))
	pe.WriteRawBytes(rh.Value)
}
