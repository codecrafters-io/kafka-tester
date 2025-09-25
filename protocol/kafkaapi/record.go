package kafkaapi

import (
	"github.com/codecrafters-io/kafka-tester/protocol/encoder"
	"github.com/codecrafters-io/kafka-tester/protocol/value"
)

type Record struct {
	Size           value.Varint
	Attributes     value.Int8
	TimestampDelta value.Varint
	OffsetDelta    value.Varint
	Key            value.RawBytes
	Value          value.RawBytes
	Headers        []RecordHeader
}

func (r *Record) Encode(pe *encoder.Encoder) {
	r.SetSize()
	pe.WriteVarint(int64(r.Size.Value))
	pe.WriteRawBytes(r.getPropertiesAsBytes())
}

func (r *Record) SetSize() {
	propertiesBytes := r.getPropertiesAsBytes()
	r.Size = value.Varint{
		Value: int64(len(propertiesBytes)),
	}
}

func (r *Record) getPropertiesAsBytes() []byte {
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

	if r.Value.Value == nil {
		propertiesEncoder.WriteVarint(-1)
	} else {
		propertiesEncoder.WriteVarint(int64(len(r.Value.Value)))
		propertiesEncoder.WriteRawBytes(r.Value.Value)
	}

	// We don't use headers in the stages
	// So, we can panic if it's not empty
	if len(r.Headers) > 0 {
		panic("Codecrafters Internal Error - Record Headers is neither empty nor nil")
	}

	if r.Headers == nil {
		propertiesEncoder.WriteVarint(-1)
	} else {
		propertiesEncoder.WriteVarint(0)
	}

	return propertiesEncoder.Bytes()
}

type RecordHeader struct {
	Key   value.RawBytes
	Value value.RawBytes
}
