package encoder

import (
	"encoding/binary"
	"errors"
	"math"
)

type realEncoder struct {
	raw []byte
	off int
}

// primitives

func (re *realEncoder) PutInt8(in int8) {
	re.raw[re.off] = byte(in)
	re.off++
}

func (re *realEncoder) PutInt16(in int16) {
	binary.BigEndian.PutUint16(re.raw[re.off:], uint16(in))
	re.off += 2
}

func (re *realEncoder) PutInt32(in int32) {
	binary.BigEndian.PutUint32(re.raw[re.off:], uint32(in))
	re.off += 4
}

func (re *realEncoder) PutInt64(in int64) {
	binary.BigEndian.PutUint64(re.raw[re.off:], uint64(in))
	re.off += 8
}

func (re *realEncoder) PutVarint(in int64) {
	re.off += binary.PutVarint(re.raw[re.off:], in)
}

func (re *realEncoder) PutUVarint(in uint64) {
	re.off += binary.PutUvarint(re.raw[re.off:], in)
}

func (re *realEncoder) PutFloat64(in float64) {
	binary.BigEndian.PutUint64(re.raw[re.off:], math.Float64bits(in))
	re.off += 8
}

func (re *realEncoder) PutArrayLength(in int) error {
	re.PutInt32(int32(in))
	return nil
}

func (re *realEncoder) PutCompactArrayLength(in int) {
	// 0 represents a null array, so +1 has to be added
	re.PutUVarint(uint64(in + 1))
}

func (re *realEncoder) PutBool(in bool) {
	if in {
		re.PutInt8(1)
		return
	}
	re.PutInt8(0)
}

// collection

func (re *realEncoder) PutRawBytes(in []byte) error {
	copy(re.raw[re.off:], in)
	re.off += len(in)
	return nil
}

func (re *realEncoder) PutBytes(in []byte) error {
	if in == nil {
		re.PutInt32(-1)
		return nil
	}
	re.PutInt32(int32(len(in)))
	return re.PutRawBytes(in)
}

func (re *realEncoder) PutVarintBytes(in []byte) error {
	if in == nil {
		re.PutVarint(-1)
		return nil
	}
	re.PutVarint(int64(len(in)))
	return re.PutRawBytes(in)
}

func (re *realEncoder) PutCompactBytes(in []byte) error {
	re.PutUVarint(uint64(len(in) + 1))
	return re.PutRawBytes(in)
}

func (re *realEncoder) PutCompactString(in string) error {
	re.PutCompactArrayLength(len(in))
	return re.PutRawBytes([]byte(in))
}

func (re *realEncoder) PutNullableCompactString(in *string) error {
	if in == nil {
		re.PutInt8(0)
		return nil
	}
	return re.PutCompactString(*in)
}

func (re *realEncoder) PutString(in string) error {
	re.PutInt16(int16(len(in)))
	copy(re.raw[re.off:], in)
	re.off += len(in)
	return nil
}

func (re *realEncoder) PutNullableString(in *string) error {
	if in == nil {
		re.PutInt16(-1)
		return nil
	}
	return re.PutString(*in)
}

func (re *realEncoder) PutStringArray(in []string) error {
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

func (re *realEncoder) PutCompactInt32Array(in []int32) error {
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

func (re *realEncoder) PutNullableCompactInt32Array(in []int32) error {
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

func (re *realEncoder) PutInt32Array(in []int32) error {
	err := re.PutArrayLength(len(in))
	if err != nil {
		return err
	}
	for _, val := range in {
		re.PutInt32(val)
	}
	return nil
}

func (re *realEncoder) PutInt64Array(in []int64) error {
	err := re.PutArrayLength(len(in))
	if err != nil {
		return err
	}
	for _, val := range in {
		re.PutInt64(val)
	}
	return nil
}

func (re *realEncoder) PutEmptyTaggedFieldArray() {
	re.PutUVarint(0)
}

func (re *realEncoder) offset() int {
	return re.off
}

func (re *RealEncoder) PackMessage() []byte {
	encoded := re.Bytes()[:re.Offset()]
	length := int32(len(encoded))

	message := make([]byte, 4+length)
	binary.BigEndian.PutUint32(message[:4], uint32(length))
	copy(message[4:], encoded)

	return message
}
