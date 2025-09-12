package kafkaapi

import (
	"github.com/codecrafters-io/kafka-tester/protocol/encoder"
	"github.com/codecrafters-io/kafka-tester/protocol/value"
)

type Record struct {
	Length         value.Int32
	Attributes     value.Int8
	TimestampDelta value.Int64
	OffsetDelta    value.Int32
	Key            value.RawBytes
	Value          value.RawBytes
	Headers        []RecordHeader
}

func (r Record) Encode(pe *encoder.Encoder) {
	propertiesEncoder := encoder.NewEncoder()

	propertiesEncoder.WriteInt8(r.Attributes.Value)
	propertiesEncoder.WriteVarint(r.TimestampDelta.Value)
	propertiesEncoder.WriteVarint(int64(r.OffsetDelta.Value))

	// Special encoding that does not belong to any data type and is only present inside Records
	// similar to protobuf encoding. It is mentioned in the Kafka docs here:  https://kafka.apache.org/documentation/#recordheader
	if r.Key.Value == nil {
		propertiesEncoder.WriteVarint(-1)
	} else {
		propertiesEncoder.WriteVarint(int64(len(r.Key.Value)))
		propertiesEncoder.WriteRawBytes(r.Key.Value)
	}

	propertiesEncoder.WriteVarint(int64(len(r.Value.Value)))
	propertiesEncoder.WriteRawBytes(r.Value.Value)
	propertiesEncoder.WriteVarint(int64(len(r.Headers)))
	for _, header := range r.Headers {
		header.Encode(propertiesEncoder)
	}

	propertiesEncoderBytes := propertiesEncoder.Bytes()

	pe.WriteVarint(int64(len(propertiesEncoderBytes)))
	pe.WriteRawBytes(propertiesEncoderBytes)
}

type RecordHeader struct {
	Key   value.RawBytes
	Value value.RawBytes
}

func (rh RecordHeader) Encode(pe *encoder.Encoder) {
	pe.WriteVarint(int64(len(rh.Key.Value)))
	pe.WriteRawBytes(rh.Key.Value)
	pe.WriteVarint(int64(len(rh.Value.Value)))
	pe.WriteRawBytes(rh.Value.Value)
}
