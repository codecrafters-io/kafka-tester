package decoder

import (
	"encoding/binary"
	"fmt"
	"math"

	"github.com/codecrafters-io/kafka-tester/protocol/errors"
)

type RealDecoder struct {
	raw []byte
	off int
}

// primitives

func (rd *RealDecoder) Init(raw []byte) {
	rd.raw = raw
	rd.off = 0
}

func (rd *RealDecoder) GetInt8() (int8, error) {
	if rd.Remaining() < 1 {
		rd.off = len(rd.raw)
		return -1, errors.ErrInsufficientData
	}
	tmp := int8(rd.raw[rd.off])
	rd.off++
	return tmp, nil
}

func (rd *RealDecoder) GetInt16() (int16, error) {
	if rd.Remaining() < 2 {
		rd.off = len(rd.raw)
		return -1, errors.ErrInsufficientData
	}
	tmp := int16(binary.BigEndian.Uint16(rd.raw[rd.off:]))
	rd.off += 2
	return tmp, nil
}

func (rd *RealDecoder) GetInt32() (int32, error) {
	if rd.Remaining() < 4 {
		rd.off = len(rd.raw)
		return -1, errors.ErrInsufficientData
	}
	tmp := int32(binary.BigEndian.Uint32(rd.raw[rd.off:]))
	rd.off += 4
	return tmp, nil
}

func (rd *RealDecoder) GetInt64() (int64, error) {
	if rd.Remaining() < 8 {
		rd.off = len(rd.raw)
		return -1, errors.ErrInsufficientData
	}
	tmp := int64(binary.BigEndian.Uint64(rd.raw[rd.off:]))
	rd.off += 8
	return tmp, nil
}

func (rd *RealDecoder) GetFloat64() (float64, error) {
	if rd.Remaining() < 8 {
		rd.off = len(rd.raw)
		return -1, errors.ErrInsufficientData
	}
	tmp := math.Float64frombits(binary.BigEndian.Uint64(rd.raw[rd.off:]))
	rd.off += 8
	return tmp, nil
}

// Confirm that this is the one google protobuf uses
// And write protocol level error messages
// ToDo Tests for all of these
func (rd *RealDecoder) GetUnsignedVarint() (uint64, error) {
	tmp, n := binary.Uvarint(rd.raw[rd.off:])
	if n == 0 {
		rd.off = len(rd.raw)
		return 0, errors.ErrInsufficientData
	}

	if n < 0 {
		rd.off -= n
		return 0, errors.ErrUVarintOverflow
	}

	rd.off += n
	return tmp, nil
}

func (rd *RealDecoder) GetSignedVarint() (int64, error) {
	tmp, n := binary.Varint(rd.raw[rd.off:])
	if n == 0 {
		rd.off = len(rd.raw)
		return -1, errors.ErrInsufficientData
	}
	if n < 0 {
		rd.off -= n
		return -1, errors.ErrVarintOverflow
	}
	rd.off += n
	return tmp, nil
}

func (rd *RealDecoder) GetArrayLength() (int, error) {
	if rd.Remaining() < 4 {
		rd.off = len(rd.raw)
		return -1, errors.ErrInsufficientData
	}
	tmp := int(int32(binary.BigEndian.Uint32(rd.raw[rd.off:])))
	rd.off += 4
	if tmp > rd.Remaining() {
		rd.off = len(rd.raw)
		return -1, errors.ErrInsufficientData
	} else if tmp > 2*math.MaxUint16 {
		return -1, errors.ErrInvalidArrayLength
	}
	return tmp, nil
}

func (rd *RealDecoder) GetCompactArrayLength() (int, error) {
	n, err := rd.GetUnsignedVarint()
	if err != nil {
		return 0, err
	}

	if n == 0 {
		return 0, nil
	}

	return int(n) - 1, nil
}

func (rd *RealDecoder) GetBool() (bool, error) {
	b, err := rd.GetInt8()
	if err != nil || b == 0 {
		return false, err
	}
	if b != 1 {
		return false, errors.ErrInvalidBool
	}
	return true, nil
}

func (rd *RealDecoder) GetEmptyTaggedFieldArray() (int, error) {
	tagCount, err := rd.GetUnsignedVarint()
	if err != nil {
		return 0, err
	}

	// skip over any tagged fields without deserializing them
	// as we don't currently support doing anything with them
	for i := uint64(0); i < tagCount; i++ {
		// fetch and ignore tag identifier
		_, err := rd.GetUnsignedVarint()
		if err != nil {
			return 0, err
		}
		length, err := rd.GetUnsignedVarint()
		if err != nil {
			return 0, err
		}
		_, err = rd.GetRawBytes(int(length))
		if err != nil {
			return 0, err
		}
		// fmt.Printf("Data: %v\n", string(data))
	}

	return 0, nil
}

// collections

func (rd *RealDecoder) GetBytes() ([]byte, error) {
	tmp, err := rd.GetInt32()
	if err != nil {
		return nil, err
	}
	if tmp == -1 {
		return nil, nil
	}

	return rd.GetRawBytes(int(tmp))
}

func (rd *RealDecoder) GetVarintBytes() ([]byte, error) {
	tmp, err := rd.GetSignedVarint()
	if err != nil {
		return nil, err
	}
	if tmp == -1 {
		return nil, nil
	}

	return rd.GetRawBytes(int(tmp))
}

func (rd *RealDecoder) GetCompactBytes() ([]byte, error) {
	n, err := rd.GetUnsignedVarint()
	if err != nil {
		return nil, err
	}

	length := int(n - 1)
	return rd.GetRawBytes(length)
}

func (rd *RealDecoder) GetStringLength() (int, error) {
	length, err := rd.GetInt16()
	if err != nil {
		return 0, err
	}

	n := int(length)

	switch {
	case n < -1:
		return 0, errors.ErrInvalidStringLength
	case n > rd.Remaining():
		rd.off = len(rd.raw)
		return 0, errors.ErrInsufficientData
	}

	return n, nil
}

func (rd *RealDecoder) GetString() (string, error) {
	n, err := rd.GetStringLength()
	if err != nil || n == -1 {
		return "", err
	}

	tmpStr := string(rd.raw[rd.off : rd.off+n])
	rd.off += n
	return tmpStr, nil
}

func (rd *RealDecoder) GetNullableString() (*string, error) {
	n, err := rd.GetStringLength()
	if err != nil || n == -1 {
		return nil, err
	}

	tmpStr := string(rd.raw[rd.off : rd.off+n])
	rd.off += n
	return &tmpStr, err
}

func (rd *RealDecoder) GetCompactString() (string, error) {
	n, err := rd.GetUnsignedVarint()
	if err != nil {
		return "", err
	}

	length := int(n - 1)
	if length < 0 {
		return "", errors.ErrInvalidByteSliceLength
	}
	tmpStr := string(rd.raw[rd.off : rd.off+length])
	rd.off += length
	return tmpStr, nil
}

func (rd *RealDecoder) GetCompactNullableString() (*string, error) {
	n, err := rd.GetUnsignedVarint()
	if err != nil {
		return nil, err
	}

	length := int(n - 1)

	if length < 0 {
		return nil, err
	}

	tmpStr := string(rd.raw[rd.off : rd.off+length])
	rd.off += length
	return &tmpStr, err
}

func (rd *RealDecoder) GetCompactInt32Array() ([]int32, error) {
	n, err := rd.GetUnsignedVarint()
	if err != nil {
		return nil, err
	}

	if n == 0 {
		return nil, nil
	}

	arrayLength := int(n) - 1

	ret := make([]int32, arrayLength)

	for i := range ret {
		ret[i] = int32(binary.BigEndian.Uint32(rd.raw[rd.off:]))
		rd.off += 4
	}
	return ret, nil
}

func (rd *RealDecoder) GetInt32Array() ([]int32, error) {
	if rd.Remaining() < 4 {
		rd.off = len(rd.raw)
		return nil, errors.ErrInsufficientData
	}
	n := int(binary.BigEndian.Uint32(rd.raw[rd.off:]))
	rd.off += 4

	if rd.Remaining() < 4*n {
		rd.off = len(rd.raw)
		return nil, errors.ErrInsufficientData
	}

	if n == 0 {
		return nil, nil
	}

	if n < 0 {
		return nil, errors.ErrInvalidArrayLength
	}

	ret := make([]int32, n)
	for i := range ret {
		ret[i] = int32(binary.BigEndian.Uint32(rd.raw[rd.off:]))
		rd.off += 4
	}
	return ret, nil
}

func (rd *RealDecoder) GetInt64Array() ([]int64, error) {
	if rd.Remaining() < 4 {
		rd.off = len(rd.raw)
		return nil, errors.ErrInsufficientData
	}
	n := int(binary.BigEndian.Uint32(rd.raw[rd.off:]))
	rd.off += 4

	if rd.Remaining() < 8*n {
		rd.off = len(rd.raw)
		return nil, errors.ErrInsufficientData
	}

	if n == 0 {
		return nil, nil
	}

	if n < 0 {
		return nil, errors.ErrInvalidArrayLength
	}

	ret := make([]int64, n)
	for i := range ret {
		ret[i] = int64(binary.BigEndian.Uint64(rd.raw[rd.off:]))
		rd.off += 8
	}
	return ret, nil
}

func (rd *RealDecoder) GetStringArray() ([]string, error) {
	if rd.Remaining() < 4 {
		rd.off = len(rd.raw)
		return nil, errors.ErrInsufficientData
	}
	n := int(binary.BigEndian.Uint32(rd.raw[rd.off:]))
	rd.off += 4

	if n == 0 {
		return nil, nil
	}

	if n < 0 {
		return nil, errors.ErrInvalidArrayLength
	}

	ret := make([]string, n)
	for i := range ret {
		str, err := rd.GetString()
		if err != nil {
			return nil, err
		}

		ret[i] = str
	}
	return ret, nil
}

// subsets

func (rd *RealDecoder) Remaining() int {
	return len(rd.raw) - rd.off
}

func (rd *RealDecoder) GetSubset(length int) (*RealDecoder, error) {
	buf, err := rd.GetRawBytes(length)
	if err != nil {
		return nil, err
	}
	return &RealDecoder{raw: buf}, nil
}

func (rd *RealDecoder) GetRawBytes(length int) ([]byte, error) {
	if length < 0 {
		return nil, errors.ErrInvalidByteSliceLength
	} else if length > rd.Remaining() {
		rd.off = len(rd.raw)
		return nil, errors.ErrInsufficientData
	}

	start := rd.off
	rd.off += length
	return rd.raw[start:rd.off], nil
}

func (rd *RealDecoder) Peek(offset, length int) (*RealDecoder, error) {
	if rd.Remaining() < offset+length {
		return nil, errors.ErrInsufficientData
	}
	off := rd.off + offset
	return &RealDecoder{raw: rd.raw[off : off+length]}, nil
}

func (rd *RealDecoder) PeekInt8(offset int) (int8, error) {
	const byteLen = 1
	if rd.Remaining() < offset+byteLen {
		return -1, errors.ErrInsufficientData
	}
	return int8(rd.raw[rd.off+offset]), nil
}

func (rd *RealDecoder) Offset() int {
	return rd.off
}

func (rd *RealDecoder) GetProtoVarint() (uint64, error) {
	var result []uint8
	var shift uint

	for {
		if rd.Remaining() < 1 {
			rd.off = len(rd.raw)
			return 0, errors.ErrInsufficientData
		}

		b := rd.raw[rd.off]
		fmt.Printf("Byte: %08b\n", b)
		rd.off++

		// Add the 7 least significant bits to the result
		result = append(result, b&0x7F)
		shift += 7

		// If the most significant bit is 0, we're done
		if b&0x80 == 0 {
			fmt.Printf("Break\n")
			break
		}

		// Prevent shift from exceeding 63 (max for uint64)
		if shift >= 64 {
			return 0, errors.ErrVarintOverflow
		}
	}
	fmt.Printf("Result: %d\n", result)
	fmt.Printf("Shift: %d\n", shift)
	reversedResult := make([]uint8, len(result))

	// Reverse the byte order
	for i := 0; i < len(result); i++ {
		fmt.Printf("Result[%d]: %d\n", i, result[i])
		reversedResult[i] = result[len(result)-i-1]
	}

	concatenatedResult := ""
	for i := 0; i < len(reversedResult); i++ {
		fmt.Printf("Reversed Result[%d]: %d\n", i, reversedResult[i])
		concatenatedResult += string(reversedResult[i])
	}

	fmt.Printf("Concatenated Result: %s\n", concatenatedResult)

	// Reverse the byte order
	// size := int(math.Ceil(float64(shift) / 8))
	// buf := make([]byte, size*8)
	fmt.Printf("Result: %v\n", result)

	return 0, nil
}

func (rd *RealDecoder) GetUnsignedVarInt() (int64, error) {
	var bytes []byte

	for {
		if rd.Remaining() < 1 {
			rd.off = len(rd.raw)
			return -1, errors.ErrInsufficientData
		}

		// Read the next byte
		b := rd.raw[rd.off]
		rd.off++

		// Drop the continuation bit and add to the slice
		bytes = append(bytes, b&0x7F)

		// If the most significant bit is not set, we're done reading bytes
		if b&0x80 == 0 {
			break
		}
	}

	// Reverse the order of the bytes to convert to big-endian
	for i, j := 0, len(bytes)-1; i < j; i, j = i+1, j-1 {
		bytes[i], bytes[j] = bytes[j], bytes[i]
	}

	// Combine the bytes into an int64
	var result int64
	for _, b := range bytes {
		result = (result << 7) | int64(b)
	}

	fmt.Printf("Result: %d\n", result)

	return result, nil
}

func (rd *RealDecoder) GetSignedVarInt() (int64, error) {
	result, err := rd.GetUnsignedVarInt()
	if err != nil {
		return -1, err
	}

	// Zig-Zag encoding
	// Positive integers p are encoded as 2 * p (the even numbers)
	// while negative integers n are encoded as 2 * |n| - 1 (the odd numbers).
	if result%2 == 0 {
		result = result / 2
	} else {
		result = -(result + 1) / 2
	}

	fmt.Printf("Result: %d\n", result)

	return result, nil
}
