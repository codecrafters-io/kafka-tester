package encoder

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"strings"
)

type Encoder struct {
	buffer *bytes.Buffer
}

func NewEncoder() *Encoder {
	buffer := bytes.NewBuffer(nil)
	return &Encoder{
		buffer: buffer,
	}
}

func (re *Encoder) Bytes() []byte {
	return re.buffer.Bytes()
}

// Primitive types

func (re *Encoder) WriteInt8(in int8) {
	re.buffer.Write([]byte{byte(in)})
}

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

func (re *Encoder) WriteInt64(in int64) {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, uint64(in))
	re.buffer.Write(buf)
}

func (re *Encoder) WriteUvarint(in uint64) {
	buf := make([]byte, binary.MaxVarintLen64)
	n := binary.PutUvarint(buf, in)
	re.buffer.Write(buf[:n])
}

func (re *Encoder) WriteVarint(in int64) {
	// 64-byte varints can be encoded using 10 bytes at max
	// we allocate a larger buffer for extra safety
	buffer := make([]byte, 16)
	writtenBytesCount := binary.PutVarint(buffer, in)
	re.buffer.Write(buffer[:writtenBytesCount])
}

func (re *Encoder) WriteCompactArrayLength(in int) {
	// 0 represents a null array, so +1 has to be added
	re.WriteUvarint(uint64(in + 1))
}

func (re *Encoder) WriteRawBytes(in []byte) {
	re.buffer.Write(in)
}

// Composite types

func (re *Encoder) WriteCompactBytes(in []byte) {
	re.WriteUvarint(uint64(len(in) + 1))
	re.WriteRawBytes(in)
}

func (re *Encoder) WriteCompactString(in string) {
	re.WriteCompactArrayLength(len(in))
	re.WriteRawBytes([]byte(in))
}

func (re *Encoder) WriteString(in string) {
	re.WriteInt16(int16(len(in)))
	re.buffer.WriteString(in)
}

func (re *Encoder) WriteCompactArrayOfInt32(arr []int32) {
	if arr == nil {
		// Null array is encoded as 0
		re.WriteUvarint(0)
		return
	}

	re.WriteUvarint(uint64(len(arr)) + 1)

	for _, val := range arr {
		re.WriteInt32(val)
	}
}

func (re *Encoder) WriteUUID(uuidString string) {
	uuidString = strings.ReplaceAll(uuidString, "-", "")

	uuidBytes, err := hex.DecodeString(uuidString)
	if err != nil {
		panic(fmt.Sprintf("Codecrafters Internal Error - Invalid UUID string: %v", err))
	}

	if len(uuidBytes) != 16 {
		panic(fmt.Sprintf("Codecrafters Internal Error - Invalid UUID length: expected 16 bytes, got %d", len(uuidBytes)))
	}

	re.WriteRawBytes(uuidBytes)
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
