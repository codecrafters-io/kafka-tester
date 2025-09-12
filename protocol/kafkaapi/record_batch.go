package kafkaapi

import (
	"hash/crc32"

	"github.com/codecrafters-io/kafka-tester/protocol/encoder"
	"github.com/codecrafters-io/kafka-tester/protocol/value"
)

type RecordBatches []RecordBatch

func (rbs RecordBatches) Encode(pe *encoder.Encoder) {
	for i := range rbs {
		rbs[i].Encode(pe)
	}
}

type RecordBatch struct {
	BaseOffset           value.Int64
	BatchLength          value.Int32
	PartitionLeaderEpoch value.Int32
	Magic                value.Int8
	CRC                  value.Int32
	Attributes           value.Int16
	LastOffsetDelta      value.Int32
	FirstTimestamp       value.Int64
	MaxTimestamp         value.Int64
	ProducerId           value.Int64
	ProducerEpoch        value.Int16
	BaseSequence         value.Int32
	Records              []Record
}

func (rb *RecordBatch) Encode(pe *encoder.Encoder) {
	propertiesEncoder := encoder.NewEncoder()
	propertiesEncoder.WriteInt16(rb.Attributes.Value)
	propertiesEncoder.WriteInt32(rb.LastOffsetDelta.Value)
	propertiesEncoder.WriteInt64(rb.FirstTimestamp.Value)
	propertiesEncoder.WriteInt64(rb.MaxTimestamp.Value)
	propertiesEncoder.WriteInt64(rb.ProducerId.Value)
	propertiesEncoder.WriteInt16(rb.ProducerEpoch.Value)
	propertiesEncoder.WriteInt32(rb.BaseSequence.Value)
	propertiesEncoder.WriteInt32(int32(len(rb.Records)))

	for i, record := range rb.Records {
		record.OffsetDelta = value.Int32{Value: int32(i)} // Offset Deltas are consecutive numerals from 0 to N-1
		// We can set them programmatically as we know the order of the records
		record.Encode(propertiesEncoder)
	}

	propertiesEncoderBytes := propertiesEncoder.Bytes()
	computedChecksum := crc32.Checksum(propertiesEncoderBytes, crc32.MakeTable(crc32.Castagnoli))
	rb.CRC = value.Int32{
		Value: int32(computedChecksum),
	}
	rb.BatchLength = value.Int32{
		Value: int32(len(propertiesEncoderBytes) + 4 + 1 + 4), // partitionLeaderEpoch + magic value + CRC
	}

	// Encode everything now
	pe.WriteInt64(rb.BaseOffset.Value)
	pe.WriteInt32(rb.BatchLength.Value)
	pe.WriteInt32(rb.PartitionLeaderEpoch.Value)
	pe.WriteInt8(2)             // Magic value is 2
	pe.WriteInt32(rb.CRC.Value) // CRC placeholder
	pe.WriteRawBytes(propertiesEncoderBytes)
}
