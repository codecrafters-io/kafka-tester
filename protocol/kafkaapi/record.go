package kafkaapi

import "github.com/codecrafters-io/kafka-tester/protocol/encoder"

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
