package kafkaapi

import (
	"hash/crc32"

	"github.com/codecrafters-io/kafka-tester/protocol/encoder"
)

// This data structure is used both in generating log files and also
// In the response of fetch API
// So, decided to include decode method for this data structure here instead

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
	computedChecksum := crc32.Checksum(propertiesEncoderBytes, crc32.MakeTable(crc32.Castagnoli))
	rb.CRC = int32(computedChecksum)
	rb.BatchLength = int32(len(propertiesEncoderBytes) + 4 + 1 + 4) // partitionLeaderEpoch + magic value + CRC

	// Encode everything now
	pe.WriteInt64(rb.BaseOffset)
	pe.WriteInt32(rb.BatchLength)
	pe.WriteInt32(rb.PartitionLeaderEpoch)
	pe.WriteInt8(2)       // Magic value is 2
	pe.WriteInt32(rb.CRC) // CRC placeholder
	pe.WriteRawBytes(propertiesEncoderBytes)
}
