package decoder

import (
	"encoding/binary"
	"math"

	"github.com/codecrafters-io/kafka-tester/protocol/errors"
)

type realDecoder struct {
	raw []byte
	off int
}

// primitives

func (rd *realDecoder) GetInt8() (int8, error) {
	if rd.Remaining() < 1 {
		rd.off = len(rd.raw)
		return -1, errors.ErrInsufficientData
	}
	tmp := int8(rd.raw[rd.off])
	rd.off++
	return tmp, nil
}

func (rd *realDecoder) GetInt16() (int16, error) {
	if rd.Remaining() < 2 {
		rd.off = len(rd.raw)
		return -1, errors.ErrInsufficientData
	}
	tmp := int16(binary.BigEndian.Uint16(rd.raw[rd.off:]))
	rd.off += 2
	return tmp, nil
}

func (rd *realDecoder) GetInt32() (int32, error) {
	if rd.Remaining() < 4 {
		rd.off = len(rd.raw)
		return -1, errors.ErrInsufficientData
	}
	tmp := int32(binary.BigEndian.Uint32(rd.raw[rd.off:]))
	rd.off += 4
	return tmp, nil
}

func (rd *realDecoder) GetInt64() (int64, error) {
	if rd.Remaining() < 8 {
		rd.off = len(rd.raw)
		return -1, errors.ErrInsufficientData
	}
	tmp := int64(binary.BigEndian.Uint64(rd.raw[rd.off:]))
	rd.off += 8
	return tmp, nil
}

func (rd *realDecoder) GetVarint() (int64, error) {
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

func (rd *realDecoder) GetUVarint() (uint64, error) {
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

func (rd *realDecoder) GetFloat64() (float64, error) {
	if rd.Remaining() < 8 {
		rd.off = len(rd.raw)
		return -1, errors.ErrInsufficientData
	}
	tmp := math.Float64frombits(binary.BigEndian.Uint64(rd.raw[rd.off:]))
	rd.off += 8
	return tmp, nil
}

func (rd *realDecoder) GetArrayLength() (int, error) {
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

func (rd *realDecoder) GetCompactArrayLength() (int, error) {
	n, err := rd.GetUVarint()
	if err != nil {
		return 0, err
	}

	if n == 0 {
		return 0, nil
	}

	return int(n) - 1, nil
}

func (rd *realDecoder) GetBool() (bool, error) {
	b, err := rd.GetInt8()
	if err != nil || b == 0 {
		return false, err
	}
	if b != 1 {
		return false, errors.ErrInvalidBool
	}
	return true, nil
}

func (rd *realDecoder) GetEmptyTaggedFieldArray() (int, error) {
	tagCount, err := rd.GetUVarint()
	if err != nil {
		return 0, err
	}

	// skip over any tagged fields without deserializing them
	// as we don't currently support doing anything with them
	for i := uint64(0); i < tagCount; i++ {
		// fetch and ignore tag identifier
		_, err := rd.GetUVarint()
		if err != nil {
			return 0, err
		}
		length, err := rd.GetUVarint()
		if err != nil {
			return 0, err
		}
		if _, err := rd.GetRawBytes(int(length)); err != nil {
			return 0, err
		}
	}

	return 0, nil
}

// collections

func (rd *realDecoder) GetBytes() ([]byte, error) {
	tmp, err := rd.GetInt32()
	if err != nil {
		return nil, err
	}
	if tmp == -1 {
		return nil, nil
	}

	return rd.GetRawBytes(int(tmp))
}

func (rd *realDecoder) GetVarintBytes() ([]byte, error) {
	tmp, err := rd.GetVarint()
	if err != nil {
		return nil, err
	}
	if tmp == -1 {
		return nil, nil
	}

	return rd.GetRawBytes(int(tmp))
}

func (rd *realDecoder) GetCompactBytes() ([]byte, error) {
	n, err := rd.GetUVarint()
	if err != nil {
		return nil, err
	}

	length := int(n - 1)
	return rd.GetRawBytes(length)
}

func (rd *realDecoder) GetStringLength() (int, error) {
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

func (rd *realDecoder) GetString() (string, error) {
	n, err := rd.GetStringLength()
	if err != nil || n == -1 {
		return "", err
	}

	tmpStr := string(rd.raw[rd.off : rd.off+n])
	rd.off += n
	return tmpStr, nil
}

func (rd *realDecoder) GetNullableString() (*string, error) {
	n, err := rd.GetStringLength()
	if err != nil || n == -1 {
		return nil, err
	}

	tmpStr := string(rd.raw[rd.off : rd.off+n])
	rd.off += n
	return &tmpStr, err
}

func (rd *realDecoder) GetCompactString() (string, error) {
	n, err := rd.GetUVarint()
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

func (rd *realDecoder) GetCompactNullableString() (*string, error) {
	n, err := rd.GetUVarint()
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

func (rd *realDecoder) GetCompactInt32Array() ([]int32, error) {
	n, err := rd.GetUVarint()
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

func (rd *realDecoder) GetInt32Array() ([]int32, error) {
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

func (rd *realDecoder) GetInt64Array() ([]int64, error) {
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

func (rd *realDecoder) GetStringArray() ([]string, error) {
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

func (rd *realDecoder) Remaining() int {
	return len(rd.raw) - rd.off
}

func (rd *realDecoder) GetSubset(length int) (PacketDecoder, error) {
	buf, err := rd.GetRawBytes(length)
	if err != nil {
		return nil, err
	}
	return &realDecoder{raw: buf}, nil
}

func (rd *realDecoder) GetRawBytes(length int) ([]byte, error) {
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

func (rd *realDecoder) Peek(offset, length int) (PacketDecoder, error) {
	if rd.Remaining() < offset+length {
		return nil, errors.ErrInsufficientData
	}
	off := rd.off + offset
	return &realDecoder{raw: rd.raw[off : off+length]}, nil
}

func (rd *realDecoder) PeekInt8(offset int) (int8, error) {
	const byteLen = 1
	if rd.Remaining() < offset+byteLen {
		return -1, errors.ErrInsufficientData
	}
	return int8(rd.raw[rd.off+offset]), nil
}
