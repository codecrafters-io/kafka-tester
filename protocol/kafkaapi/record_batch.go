package kafkaapi

import (
	"hash/crc32"

	"github.com/codecrafters-io/kafka-tester/protocol/encoder"
	"github.com/codecrafters-io/kafka-tester/protocol/value"
)

// This data structure is used both in generating log files and also
// In the response of fetch API
// So, decided to include decode method for this data structure here instead

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
	// Set CRC and batch size
	rb.SetCRC()
	rb.SetBatchLength()

	// Encode everything now
	pe.WriteInt64(rb.BaseOffset.Value)
	pe.WriteInt32(rb.BatchLength.Value)
	pe.WriteInt32(rb.PartitionLeaderEpoch.Value)
	pe.WriteInt8(2)             // Magic value is 2
	pe.WriteInt32(rb.CRC.Value) // CRC placeholder
	pe.WriteRawBytes(rb.getPropertiesAsBytes())
}

func (rb *RecordBatch) IsCRCValueOk() bool {
	return true
}

func (rb *RecordBatch) SetBatchLength() {
	propertiesBytes := rb.getPropertiesAsBytes()

	rb.BatchLength = value.Int32{
		Value: int32(len(propertiesBytes) + 4 + 1 + 4), // partitionLeaderEpoch + magic value + CRC
	}
}

func (rb *RecordBatch) SetCRC() {
	propertiesBytes := rb.getPropertiesAsBytes()

	computedChecksum := crc32.Checksum(propertiesBytes, crc32.MakeTable(crc32.Castagnoli))
	rb.CRC = value.Int32{
		Value: int32(computedChecksum),
	}
}

func (rb *RecordBatch) getPropertiesAsBytes() []byte {
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
		record.OffsetDelta = value.Varint{Value: int64(i)}
		record.Encode(propertiesEncoder)
	}

	return propertiesEncoder.Bytes()
}
