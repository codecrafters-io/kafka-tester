package encoder

import (
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"strings"
)

type RealEncoder struct {
	raw []byte
	off int
}

func (re *RealEncoder) Init(raw []byte) {
	re.raw = raw
	re.off = 0
}

// primitives

func (re *RealEncoder) PutInt8(in int8) {
	re.raw[re.off] = byte(in)
	re.off++
}

func (re *RealEncoder) PutInt16(in int16) {
	binary.BigEndian.PutUint16(re.raw[re.off:], uint16(in))
	re.off += 2
}

func (re *RealEncoder) PutInt32(in int32) {
	binary.BigEndian.PutUint32(re.raw[re.off:], uint32(in))
	re.off += 4
}

func (re *RealEncoder) PutInt64(in int64) {
	binary.BigEndian.PutUint64(re.raw[re.off:], uint64(in))
	re.off += 8
}

func (re *RealEncoder) PutVarint(in int64) {
	re.off += binary.PutVarint(re.raw[re.off:], in)
}

func (re *RealEncoder) PutUVarint(in uint64) {
	re.off += binary.PutUvarint(re.raw[re.off:], in)
}

func (re *RealEncoder) PutFloat64(in float64) {
	binary.BigEndian.PutUint64(re.raw[re.off:], math.Float64bits(in))
	re.off += 8
}

func (re *RealEncoder) PutArrayLength(in int) error {
	re.PutInt32(int32(in))
	return nil
}

func (re *RealEncoder) PutCompactArrayLength(in int) {
	// 0 represents a null array, so +1 has to be added
	re.PutUVarint(uint64(in + 1))
}

func (re *RealEncoder) PutBool(in bool) {
	if in {
		re.PutInt8(1)
		return
	}
	re.PutInt8(0)
}

// collection

func (re *RealEncoder) PutRawBytes(in []byte) error {
	copy(re.raw[re.off:], in)
	re.off += len(in)
	return nil
}

func (re *RealEncoder) PutBytes(in []byte) error {
	if in == nil {
		re.PutInt32(-1)
		return nil
	}
	re.PutInt32(int32(len(in)))
	return re.PutRawBytes(in)
}

func (re *RealEncoder) PutVarintBytes(in []byte) error {
	if in == nil {
		re.PutVarint(-1)
		return nil
	}
	re.PutVarint(int64(len(in)))
	return re.PutRawBytes(in)
}

func (re *RealEncoder) PutCompactBytes(in []byte) error {
	re.PutUVarint(uint64(len(in) + 1))
	return re.PutRawBytes(in)
}

func (re *RealEncoder) PutCompactString(in string) error {
	re.PutCompactArrayLength(len(in))
	return re.PutRawBytes([]byte(in))
}

func (re *RealEncoder) PutNullableCompactString(in *string) error {
	if in == nil {
		re.PutInt8(0)
		return nil
	}
	return re.PutCompactString(*in)
}

func (re *RealEncoder) PutString(in string) error {
	re.PutInt16(int16(len(in)))
	copy(re.raw[re.off:], in)
	re.off += len(in)
	return nil
}

func (re *RealEncoder) PutNullableString(in *string) error {
	if in == nil {
		re.PutInt16(-1)
		return nil
	}
	return re.PutString(*in)
}

func (re *RealEncoder) PutStringArray(in []string) error {
	err := re.PutArrayLength(len(in))
	if err != nil {
		return err
	}

	for _, val := range in {
		if err := re.PutString(val); err != nil {
			return err
		}
	}

	return nil
}

func (re *RealEncoder) PutCompactInt32Array(in []int32) error {
	if in == nil {
		return errors.New("expected int32 array to be non null")
	}
	// 0 represents a null array, so +1 has to be added
	re.PutUVarint(uint64(len(in)) + 1)
	for _, val := range in {
		re.PutInt32(val)
	}
	return nil
}

func (re *RealEncoder) PutNullableCompactInt32Array(in []int32) error {
	if in == nil {
		re.PutUVarint(0)
		return nil
	}
	// 0 represents a null array, so +1 has to be added
	re.PutUVarint(uint64(len(in)) + 1)
	for _, val := range in {
		re.PutInt32(val)
	}
	return nil
}

func (re *RealEncoder) PutInt32Array(in []int32) error {
	err := re.PutArrayLength(len(in))
	if err != nil {
		return err
	}
	for _, val := range in {
		re.PutInt32(val)
	}
	return nil
}

func (re *RealEncoder) PutInt64Array(in []int64) error {
	err := re.PutArrayLength(len(in))
	if err != nil {
		return err
	}
	for _, val := range in {
		re.PutInt64(val)
	}
	return nil
}

func (re *RealEncoder) PutEmptyTaggedFieldArray() {
	re.PutUVarint(0)
}

func (re *RealEncoder) Offset() int {
	return re.off
}

func (re *RealEncoder) Bytes() []byte {
	return re.raw
}

func (re *RealEncoder) PackMessage() []byte {
	encoded := re.Bytes()[:re.Offset()]
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
		return "", fmt.Errorf("invalid UUID length: expected 16 bytes, got %d", len(encodedUUID))
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
