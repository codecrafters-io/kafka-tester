package encoder

import (
	"encoding/binary"
)

type Encoder struct {
	bytes  []byte
	offset int
}

func NewEncoder(bytes []byte) *Encoder {
	return &Encoder{
		bytes:  bytes,
		offset: 0,
	}
}

func (re *Encoder) Offset() int {
	return re.offset
}

func (re *Encoder) AllBytes() []byte {
	return re.bytes
}

func (re *Encoder) EncodedBytes() []byte {
	return re.AllBytes()[:re.Offset()]
}

// Primitive types

func (re *Encoder) WriteInt16(in int16) {
	binary.BigEndian.PutUint16(re.bytes[re.offset:], uint16(in))
	re.offset += 2
}

func (re *Encoder) WriteInt32(in int32) {
	binary.BigEndian.PutUint32(re.bytes[re.offset:], uint32(in))
	re.offset += 4
}

func (re *Encoder) WriteUvarint(in uint64) {
	re.offset += binary.PutUvarint(re.bytes[re.offset:], in)
}

func (re *Encoder) WriteCompactArrayLength(in int) {
	// 0 represents a null array, so +1 has to be added
	re.WriteUvarint(uint64(in + 1))
}

// Composite types

func (re *Encoder) WriteRawBytes(in []byte) {
	copy(re.bytes[re.offset:], in)
	re.offset += len(in)
}

func (re *Encoder) WriteCompactString(in string) {
	re.WriteCompactArrayLength(len(in))
	re.WriteRawBytes([]byte(in))
}

func (re *Encoder) WriteString(in string) {
	re.WriteInt16(int16(len(in)))
	copy(re.bytes[re.offset:], in)
	re.offset += len(in)
}

func (re *Encoder) WriteEmptyTagBuffer() {
	re.WriteUvarint(0)
}

func PackMessage(encoded []byte) []byte {
	length := int32(len(encoded))

	message := make([]byte, 4+length)
	binary.BigEndian.PutUint32(message[:4], uint32(length))
	copy(message[4:], encoded)

	return message
}
