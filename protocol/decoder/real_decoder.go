package decoder

import (
	"encoding/binary"
	"fmt"
	"math"
	"strings"

	"github.com/codecrafters-io/kafka-tester/protocol/errors"
)

type RealDecoder struct {
	raw []byte
	off int
}

func (rd *RealDecoder) Init(raw []byte) {
	rd.raw = raw
	rd.off = 0
}

func (rd *RealDecoder) GetInt8() (int8, error) {
	if rd.Remaining() < 1 {
		rem := rd.Remaining()
		return -1, errors.NewPacketDecodingError(fmt.Sprintf("Expected int8 length to be 1 byte, got %d bytes", rem), "INT8")
	}
	tmp := int8(rd.raw[rd.off])
	rd.off++
	return tmp, nil
}

func (rd *RealDecoder) GetInt16() (int16, error) {
	if rd.Remaining() < 2 {
		rem := rd.Remaining()
		return -1, errors.NewPacketDecodingError(fmt.Sprintf("Expected int16 length to be 2 bytes, got %d bytes", rem), "INT16")
	}
	tmp := int16(binary.BigEndian.Uint16(rd.raw[rd.off:]))
	rd.off += 2
	return tmp, nil
}

func (rd *RealDecoder) GetInt32() (int32, error) {
	if rd.Remaining() < 4 {
		rem := rd.Remaining()
		return -1, errors.NewPacketDecodingError(fmt.Sprintf("Expected int32 length to be 4 bytes, got %d bytes", rem), "INT32")
	}
	tmp := int32(binary.BigEndian.Uint32(rd.raw[rd.off:]))
	rd.off += 4
	return tmp, nil
}

func (rd *RealDecoder) GetInt64() (int64, error) {
	if rd.Remaining() < 8 {
		rem := rd.Remaining()
		return -1, errors.NewPacketDecodingError(fmt.Sprintf("Expected int64 length to be 8 bytes, got %d bytes", rem), "INT64")
	}
	tmp := int64(binary.BigEndian.Uint64(rd.raw[rd.off:]))
	rd.off += 8
	return tmp, nil
}

func (rd *RealDecoder) GetFloat64() (float64, error) {
	if rd.Remaining() < 8 {
		rem := rd.Remaining()
		return -1, errors.NewPacketDecodingError(fmt.Sprintf("Expected float64 length to be 8 bytes, got %d bytes", rem), "FLOAT64")
	}
	tmp := math.Float64frombits(binary.BigEndian.Uint64(rd.raw[rd.off:]))
	rd.off += 8
	return tmp, nil
}

func (rd *RealDecoder) GetUnsignedVarint() (uint64, error) {
	tmp, n := binary.Uvarint(rd.raw[rd.off:])
	if n == 0 {
		return 0, errors.NewPacketDecodingError("Unexpected end of data", "UNSIGNED_VARINT")
	}

	if n < 0 {
		return 0, errors.NewPacketDecodingError(fmt.Sprintf("Unexpected unsigned varint overflow after decoding %d bytes", -n), "UNSIGNED_VARINT")
	}

	rd.off += n
	return tmp, nil
}

func (rd *RealDecoder) GetSignedVarint() (int64, error) {
	tmp, n := binary.Varint(rd.raw[rd.off:])
	if n == 0 {
		return -1, errors.NewPacketDecodingError("Unexpected end of data", "SIGNED_VARINT")
	}
	if n < 0 {
		return -1, errors.NewPacketDecodingError(fmt.Sprintf("Unexpected varint overflow after decoding %d bytes", -n), "SIGNED_VARINT")
	}
	rd.off += n
	return tmp, nil
}

func (rd *RealDecoder) GetArrayLength() (int, error) {
	if rd.Remaining() < 4 {
		rem := rd.Remaining()
		return -1, errors.NewPacketDecodingError(fmt.Sprintf("Expected array length prefix to be 4 bytes, got %d bytes", rem), "ARRAY_LENGTH")
	}
	tmp, err := rd.GetInt32()
	if err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return -1, decodingErr.WithAddedContext("COMPACT_INT32_ARRAY")
		}
		return -1, err
	}
	arrayLength := int(tmp)
	if arrayLength > rd.Remaining() {
		return -1, errors.NewPacketDecodingError(fmt.Sprintf("Expect to read at least %d bytes, but only %d bytes are remaining", arrayLength, rd.off), "array length")
	} else if arrayLength > 2*math.MaxUint16 {
		return -1, errors.NewPacketDecodingError(fmt.Sprintf("Invalid array length: %d", arrayLength), "ARRAY_LENGTH")
	}

	return arrayLength, nil
}

func (rd *RealDecoder) GetCompactArrayLength() (int, error) {
	n, err := rd.GetUnsignedVarint()
	if err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return 0, decodingErr.WithAddedContext("COMPACT_ARRAY_LENGTH")
		}
		return 0, err
	}

	if n == 0 {
		return 0, nil
	}

	return int(n) - 1, nil
}

func (rd *RealDecoder) GetBool() (bool, error) {
	b, err := rd.GetInt8()
	if err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return false, decodingErr.WithAddedContext("BOOLEAN")
		}
		return false, err
	}
	if b == 0 {
		return false, nil
	}
	if b != 1 {
		return false, errors.NewPacketDecodingError(fmt.Sprintf("Expected bool to be 1 or 0, got %d", b), "BOOLEAN")
	}
	return true, nil
}

func (rd *RealDecoder) GetEmptyTaggedFieldArray() (int, error) {
	tagCount, err := rd.GetUnsignedVarint()
	if err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return 0, decodingErr.WithAddedContext("TAGGED_FIELD_ARRAY")
		}
		return 0, err
	}

	// skip over any tagged fields without deserializing them
	// as we don't currently support doing anything with them
	for i := uint64(0); i < tagCount; i++ {
		// fetch and ignore tag identifier
		_, err := rd.GetUnsignedVarint()
		if err != nil {
			if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
				return 0, decodingErr.WithAddedContext("TAGGED_FIELD_ARRAY")
			}
			return 0, err
		}
		length, err := rd.GetUnsignedVarint()
		if err != nil {
			if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
				return 0, decodingErr.WithAddedContext("TAGGED_FIELD_ARRAY")
			}
			return 0, err
		}
		_, err = rd.GetRawBytes(int(length))
		if err != nil {
			if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
				return 0, decodingErr.WithAddedContext("TAGGED_FIELD_ARRAY")
			}
			return 0, err
		}
	}

	return 0, nil
}

func (rd *RealDecoder) GetBytes() ([]byte, error) {
	tmp, err := rd.GetInt32()
	if err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return nil, decodingErr.WithAddedContext("RAW_BYTES")
		}
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
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return nil, decodingErr.WithAddedContext("VARINT_BYTES")
		}
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
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return nil, decodingErr.WithAddedContext("COMPACT_BYTES")
		}
		return nil, err
	}

	length := int(n - 1)
	return rd.GetRawBytes(length)
}

func (rd *RealDecoder) GetStringLength() (int, error) {
	length, err := rd.GetInt16()
	if err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return 0, decodingErr.WithAddedContext("STRING_LENGTH")
		}
		return 0, err
	}

	n := int(length)

	switch {
	case n < -1:
		return 0, errors.NewPacketDecodingError(fmt.Sprintf("Expected string length to be >= -1, got %d", n), "STRING_LENGTH")
	case n > rd.Remaining():
		return 0, errors.NewPacketDecodingError(fmt.Sprintf("Expect to read at least %d bytes, but only %d bytes are remaining", n, rd.off), "STRING_LENGTH")
	}

	return n, nil
}

func (rd *RealDecoder) GetString() (string, error) {
	n, err := rd.GetStringLength()
	if err != nil || n == -1 {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return "", decodingErr.WithAddedContext("STRING")
		}
		return "", err
	}

	if rd.Remaining() < n {
		rem := rd.Remaining()
		return "", errors.NewPacketDecodingError(fmt.Sprintf("Expected string length to be %d bytes, got %d bytes", n, rem), "STRING")
	}

	tmpStr := string(rd.raw[rd.off : rd.off+n])
	rd.off += n
	return tmpStr, nil
}

func (rd *RealDecoder) GetNullableString() (*string, error) {
	n, err := rd.GetStringLength()
	if err != nil || n == -1 {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return nil, decodingErr.WithAddedContext("NULLABLE_STRING")
		}
		return nil, err
	}

	if rd.Remaining() < n {
		rem := rd.Remaining()
		return nil, errors.NewPacketDecodingError(fmt.Sprintf("Expected string length to be %d bytes, got %d bytes", n, rem), "NULLABLE_STRING")
	}

	tmpStr := string(rd.raw[rd.off : rd.off+n])
	rd.off += n
	return &tmpStr, nil
}

func (rd *RealDecoder) GetCompactString() (string, error) {
	n, err := rd.GetUnsignedVarint()
	if err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return "", decodingErr.WithAddedContext("COMPACT_STRING")
		}
		return "", err
	}

	length := int(n - 1)
	if length < 0 {
		return "", errors.NewPacketDecodingError(fmt.Sprintf("Expected COMPACT_STRING length to be > 0, got %d", length), "COMPACT_STRING")
	}

	if rd.Remaining() < length {
		rem := rd.Remaining()
		return "", errors.NewPacketDecodingError(fmt.Sprintf("Expected string length to be %d bytes, got %d bytes", length, rem), "COMPACT_STRING")
	}

	tmpStr := string(rd.raw[rd.off : rd.off+length])
	rd.off += length
	return tmpStr, nil
}

func (rd *RealDecoder) GetCompactNullableString() (*string, error) {
	n, err := rd.GetUnsignedVarint()
	if err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return nil, decodingErr.WithAddedContext("COMPACT_NULLABLE_STRING")
		}
		return nil, err
	}

	length := int(n - 1)

	if length < 0 {
		return nil, errors.NewPacketDecodingError(fmt.Sprintf("Expected compact nullable string length to be > 0, got %d", length), "COMPACT_NULLABLE_STRING")
	}

	if rd.Remaining() < length {
		rem := rd.Remaining()
		return nil, errors.NewPacketDecodingError(fmt.Sprintf("Expected string length to be %d bytes, got %d bytes", length, rem), "COMPACT_NULLABLE_STRING")
	}

	tmpStr := string(rd.raw[rd.off : rd.off+length])
	rd.off += length
	return &tmpStr, nil
}

func (rd *RealDecoder) GetCompactInt32Array() ([]int32, error) {
	n, err := rd.GetUnsignedVarint()
	if err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return nil, decodingErr.WithAddedContext("COMPACT_INT32_ARRAY")
		}
		return nil, err
	}

	if n == 0 {
		return nil, nil
	}

	arrayLength := int(n) - 1

	ret := make([]int32, arrayLength)

	for i := range ret {
		entry, err := rd.GetInt32()
		if err != nil {
			if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
				return nil, decodingErr.WithAddedContext("COMPACT_INT32_ARRAY")
			}
			return nil, err
		}
		ret[i] = entry
	}
	return ret, nil
}

func (rd *RealDecoder) GetInt32Array() ([]int32, error) {
	n, err := rd.GetArrayLength()
	if err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return nil, decodingErr.WithAddedContext("INT32_ARRAY")
		}
		return nil, err
	}

	if rd.Remaining() < 4*n {
		rem := rd.Remaining()
		return nil, errors.NewPacketDecodingError(fmt.Sprintf("Expected int32 array length to be %d bytes, got %d bytes", 4*n, rem), "INT32_ARRAY")
	}

	if n == 0 {
		return nil, nil
	}

	if n < 0 {
		return nil, errors.NewPacketDecodingError(fmt.Sprintf("Array length can only be -1 or > 0, got: %d", n), "INT32_ARRAY")
	}

	ret := make([]int32, n)
	for i := range ret {
		entry, err := rd.GetInt32()
		if err != nil {
			if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
				return nil, decodingErr.WithAddedContext("COMPACT_INT32_ARRAY")
			}
			return nil, err
		}
		ret[i] = entry
	}
	return ret, nil
}

func (rd *RealDecoder) GetInt64Array() ([]int64, error) {
	n, err := rd.GetArrayLength()
	if err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return nil, decodingErr.WithAddedContext("INT64_ARRAY")
		}
		return nil, err
	}

	if rd.Remaining() < 8*n {
		rem := rd.Remaining()
		return nil, errors.NewPacketDecodingError(fmt.Sprintf("Expected int64 array length to be %d bytes, got %d bytes", 8*n, rem), "INT64_ARRAY")
	}

	if n == 0 {
		return nil, nil
	}

	if n < 0 {
		return nil, errors.NewPacketDecodingError(fmt.Sprintf("Array length can only be -1 or > 0, got: %d", n), "INT64_ARRAY")
	}

	ret := make([]int64, n)
	for i := range ret {
		entry, err := rd.GetInt64()
		if err != nil {
			if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
				return nil, decodingErr.WithAddedContext("COMPACT_INT32_ARRAY")
			}
			return nil, err
		}
		ret[i] = entry
	}
	return ret, nil
}

func (rd *RealDecoder) GetStringArray() ([]string, error) {
	n, err := rd.GetArrayLength()
	if err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return nil, decodingErr.WithAddedContext("STRING_ARRAY")
		}
		return nil, err
	}

	if n == 0 {
		return nil, nil
	}

	if n < 0 {
		return nil, errors.NewPacketDecodingError(fmt.Sprintf("Array length can only be -1 or > 0, got: %d", n), "STRING_ARRAY")
	}

	ret := make([]string, n)
	for i := range ret {
		str, err := rd.GetString()
		if err != nil {
			if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
				return nil, decodingErr.WithAddedContext("STRING_ARRAY")
			}
			return nil, err
		}

		ret[i] = str
	}
	return ret, nil
}

func (rd *RealDecoder) Remaining() int {
	return len(rd.raw) - rd.off
}

func (rd *RealDecoder) GetRawBytes(length int) ([]byte, error) {
	if length < 0 {
		return nil, errors.NewPacketDecodingError(fmt.Sprintf("Expected length to be >= 0, got %d", length), "RAW_BYTES")
	} else if length > rd.Remaining() {
		return nil, errors.NewPacketDecodingError(fmt.Sprintf("Expected length to be lesser than remaining bytes (%d), got %d", rd.Remaining(), length), "RAW_BYTES")
	}

	start := rd.off
	rd.off += length
	return rd.raw[start:rd.off], nil
}

func (rd *RealDecoder) Offset() int {
	return rd.off
}

func (rd *RealDecoder) GetRawBytesFromOffset(length int) ([]byte, error) {
	if length < 0 {
		return nil, errors.NewPacketDecodingError(fmt.Sprintf("Expected length to be >= 0, got %d", length), "RAW_BYTES_FROM_OFFSET")
	}

	if rd.off >= len(rd.raw) {
		return nil, errors.NewPacketDecodingError(fmt.Sprintf("Expected offset to be less than length of raw bytes (%d), got %d", len(rd.raw), rd.off), "RAW_BYTES_FROM_OFFSET")
	} else if rd.off+length > len(rd.raw) {
		return nil, errors.NewPacketDecodingError(fmt.Sprintf("Expected offset to be less than length of raw bytes (%d), got %d", len(rd.raw), rd.off), "RAW_BYTES_FROM_OFFSET")
	}

	return rd.raw[rd.off : rd.off+length], nil
}

// FormatDetailedError formats the error message with the received bytes and the offset
func (rd *RealDecoder) FormatDetailedError(message string) error {
	lines := []string{}

	offset := rd.Offset()
	receivedBytes := rd.raw
	receivedByteString := NewInspectableHexDump(receivedBytes)

	lines = append(lines, "Received:")
	lines = append(lines, receivedByteString.FormatWithHighlightedOffset(offset))
	lines = append(lines, message)

	//lint:ignore SA1006 we are okay with this
	return fmt.Errorf(strings.Join(lines, "\n"))
}
