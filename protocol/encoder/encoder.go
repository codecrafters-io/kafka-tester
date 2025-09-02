package encoder

import (
	"bytes"
	"encoding/binary"
)

type Encoder struct {
	buffer *bytes.Buffer
}

func NewEncoder() *Encoder {
	// Start with a reasonable size instead of 0
	// to avoid re-grow operations for relatively small size encoding
	buffer := bytes.NewBuffer(make([]byte, 1024))
	return &Encoder{
		buffer: buffer,
	}
}

func (re *Encoder) Bytes() []byte {
	return re.buffer.Bytes()
}

// Primitive types

func (re *Encoder) WriteInt16(in int16) {
	buf := make([]byte, 2)
	binary.BigEndian.PutUint16(buf, uint16(in))
	re.buffer.Write(buf)
}

func (re *Encoder) WriteInt32(in int32) {
	buf := make([]byte, 4)
	binary.BigEndian.PutUint32(buf, uint32(in))
	re.buffer.Write(buf)
}

func (re *Encoder) WriteUvarint(in uint64) {
	buf := make([]byte, binary.MaxVarintLen64)
	n := binary.PutUvarint(buf, in)
	re.buffer.Write(buf[:n])
}

func (re *Encoder) WriteCompactArrayLength(in int) {
	// 0 represents a null array, so +1 has to be added
	re.WriteUvarint(uint64(in + 1))
}

// Composite types

func (re *Encoder) WriteRawBytes(in []byte) {
	re.buffer.Write(in)
}

func (re *Encoder) WriteCompactString(in string) {
	re.WriteCompactArrayLength(len(in))
	re.WriteRawBytes([]byte(in))
}

func (re *Encoder) WriteString(in string) {
	re.WriteInt16(int16(len(in)))
	re.buffer.WriteString(in)
}

func (re *Encoder) WriteEmptyTagBuffer() {
	re.WriteUvarint(0)
}

func PackEncodedBytesAsMessage(encoded []byte) []byte {
	length := int32(len(encoded))

	message := make([]byte, 4+length)
	binary.BigEndian.PutUint32(message[:4], uint32(length))
	copy(message[4:], encoded)

	return message
}
