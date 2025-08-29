package encoder

import (
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"math"
	"strings"

	"github.com/codecrafters-io/kafka-tester/protocol/errors"
)

type Encoder struct {
	bytes  []byte
	offset int
}

func (re *Encoder) Init(raw []byte) {
	re.bytes = raw
	re.offset = 0
}

func (re *Encoder) PutRawBytesAt(in []byte, offset int, length int) {
	copy(re.bytes[offset:offset+length], in)
}

func (re *Encoder) PutInt32At(in int32, offset int, length int) {
	binary.BigEndian.PutUint32(re.bytes[offset:offset+length], uint32(in))
}

func (re *Encoder) PutUVarintAt(in uint64, offset int) {
	binary.PutUvarint(re.bytes[offset:], in)
}

func (re *Encoder) PutVarintAt(in int64, offset int) {
	binary.PutVarint(re.bytes[offset:], in)
}

// primitives

func (re *Encoder) PutInt8(in int8) {
	re.bytes[re.offset] = byte(in)
	re.offset++
}

func (re *Encoder) PutInt16(in int16) {
	binary.BigEndian.PutUint16(re.bytes[re.offset:], uint16(in))
	re.offset += 2
}

func (re *Encoder) PutInt32(in int32) {
	binary.BigEndian.PutUint32(re.bytes[re.offset:], uint32(in))
	re.offset += 4
}

func (re *Encoder) PutInt64(in int64) {
	binary.BigEndian.PutUint64(re.bytes[re.offset:], uint64(in))
	re.offset += 8
}

func (re *Encoder) PutVarint(in int64) {
	re.offset += binary.PutVarint(re.bytes[re.offset:], in)
}

func (re *Encoder) PutUVarint(in uint64) {
	re.offset += binary.PutUvarint(re.bytes[re.offset:], in)
}

func (re *Encoder) PutFloat64(in float64) {
	binary.BigEndian.PutUint64(re.bytes[re.offset:], math.Float64bits(in))
	re.offset += 8
}

func (re *Encoder) PutArrayLength(in int) {
	re.PutInt32(int32(in))
}

func (re *Encoder) PutCompactArrayLength(in int) {
	// 0 represents a null array, so +1 has to be added
	re.PutUVarint(uint64(in + 1))
}

func (re *Encoder) PutBool(in bool) {
	if in {
		re.PutInt8(1)
		return
	}
	re.PutInt8(0)
}

// collection

func (re *Encoder) PutRawBytes(in []byte) {
	copy(re.bytes[re.offset:], in)
	re.offset += len(in)
}

func (re *Encoder) PutBytes(in []byte) {
	if in == nil {
		re.PutInt32(-1)
		return
	}
	re.PutInt32(int32(len(in)))
	re.PutRawBytes(in)
}

func (re *Encoder) PutVarintBytes(in []byte) {
	if in == nil {
		re.PutVarint(-1)
		return
	}
	re.PutVarint(int64(len(in)))
	re.PutRawBytes(in)
}

func (re *Encoder) PutCompactBytes(in []byte) {
	re.PutUVarint(uint64(len(in) + 1))
	re.PutRawBytes(in)
}

func (re *Encoder) PutCompactString(in string) {
	re.PutCompactArrayLength(len(in))
	re.PutRawBytes([]byte(in))
}

func (re *Encoder) PutNullableCompactString(in *string) {
	if in == nil {
		re.PutUVarint(0)
		return
	}
	re.PutCompactString(*in)
}

func (re *Encoder) PutString(in string) {
	re.PutInt16(int16(len(in)))
	copy(re.bytes[re.offset:], in)
	re.offset += len(in)
}

func (re *Encoder) PutNullableString(in *string) {
	if in == nil {
		re.PutInt16(-1)
		return
	}
	re.PutString(*in)
}

func (re *Encoder) PutStringArray(in []string) {
	re.PutArrayLength(len(in))

	for _, val := range in {
		re.PutString(val)
	}
}

func (re *Encoder) PutCompactInt32Array(in []int32) error {
	if in == nil {
		return errors.NewPacketDecodingError("expected int32 array to be non null").AddContexts("PutCompactInt32Array")
	}
	// 0 represents a null array, so +1 has to be added
	re.PutUVarint(uint64(len(in)) + 1)
	for _, val := range in {
		re.PutInt32(val)
	}
	return nil
}

func (re *Encoder) PutNullableCompactInt32Array(in []int32) {
	if in == nil {
		re.PutUVarint(0)
	}
	// 0 represents a null array, so +1 has to be added
	re.PutUVarint(uint64(len(in)) + 1)
	for _, val := range in {
		re.PutInt32(val)
	}
}

func (re *Encoder) PutInt32Array(in []int32) {
	re.PutArrayLength(len(in))
	for _, val := range in {
		re.PutInt32(val)
	}
}

func (re *Encoder) PutInt64Array(in []int64) {
	re.PutArrayLength(len(in))
	for _, val := range in {
		re.PutInt64(val)
	}
}

func (re *Encoder) PutEmptyTaggedFieldArray() {
	re.PutUVarint(0)
}

func (re *Encoder) Offset() int {
	return re.offset
}

func (re *Encoder) Bytes() []byte {
	return re.bytes
}

// Helpers

func (re *Encoder) ToBytes() []byte {
	return re.Bytes()[:re.Offset()]
}

func PackMessage(encoded []byte) []byte {
	length := int32(len(encoded))

	message := make([]byte, 4+length)
	binary.BigEndian.PutUint32(message[:4], uint32(length))
	copy(message[4:], encoded)

	return message
}

func EncodeUUID(uuidString string) ([]byte, error) {
	// Remove any hyphens from the UUID string
	uuidString = strings.ReplaceAll(uuidString, "-", "")

	// Decode the hex string to bytes
	uuid, err := hex.DecodeString(uuidString)
	if err != nil {
		return nil, fmt.Errorf("invalid UUID string: %v", err)
	}

	// Check if the decoded bytes are exactly 16 bytes long
	if len(uuid) != 16 {
		return nil, fmt.Errorf("invalid UUID length: expected 16 bytes, got %d", len(uuid))
	}

	// The UUID is already in network byte order (big-endian),
	// so we don't need to do any byte order conversion

	return uuid, nil
}

func DecodeUUID(encodedUUID []byte) (string, error) {
	// Check if the encoded UUID is exactly 16 bytes long
	if len(encodedUUID) != 16 {
		errorMessage := fmt.Sprintf("invalid UUID length: expected 16 bytes, got %d", len(encodedUUID))
		return "", errors.NewPacketDecodingError(errorMessage).AddContexts("DecodeUUID")
	}

	// Convert the bytes to a hex string
	uuidHex := hex.EncodeToString(encodedUUID)

	// Insert hyphens to format the UUID string
	uuid := fmt.Sprintf("%s-%s-%s-%s-%s",
		uuidHex[0:8],
		uuidHex[8:12],
		uuidHex[12:16],
		uuidHex[16:20],
		uuidHex[20:32])

	return uuid, nil
}
