package decoder

import (
	"encoding/binary"
	"fmt"
	"math"
	"strings"

	"github.com/codecrafters-io/kafka-tester/protocol/errors"
)

type Decoder struct {
	bytes  []byte
	offset int
}

func (rd *Decoder) Init(raw []byte) {
	rd.bytes = raw
	rd.offset = 0
}

func (rd *Decoder) GetInt8() (int8, error) {
	if rd.Remaining() < 1 {
		rem := rd.Remaining()
		return -1, errors.NewPacketDecodingError(fmt.Sprintf("Expected int8 length to be 1 byte, got %d bytes", rem), "INT8")
	}
	decodedInteger := int8(rd.bytes[rd.offset])
	rd.offset++
	return decodedInteger, nil
}

func (rd *Decoder) GetInt16() (int16, error) {
	if rd.Remaining() < 2 {
		rem := rd.Remaining()
		return -1, errors.NewPacketDecodingError(fmt.Sprintf("Expected int16 length to be 2 bytes, got %d bytes", rem), "INT16")
	}
	decodedInteger := int16(binary.BigEndian.Uint16(rd.bytes[rd.offset:]))
	rd.offset += 2
	return decodedInteger, nil
}

func (rd *Decoder) GetInt32() (int32, error) {
	if rd.Remaining() < 4 {
		rem := rd.Remaining()
		return -1, errors.NewPacketDecodingError(fmt.Sprintf("Expected int32 length to be 4 bytes, got %d bytes", rem), "INT32")
	}
	decodedInteger := int32(binary.BigEndian.Uint32(rd.bytes[rd.offset:]))
	rd.offset += 4
	return decodedInteger, nil
}

func (rd *Decoder) GetInt64() (int64, error) {
	if rd.Remaining() < 8 {
		rem := rd.Remaining()
		return -1, errors.NewPacketDecodingError(fmt.Sprintf("Expected int64 length to be 8 bytes, got %d bytes", rem), "INT64")
	}
	decodedInteger := int64(binary.BigEndian.Uint64(rd.bytes[rd.offset:]))
	rd.offset += 8
	return decodedInteger, nil
}

func (rd *Decoder) GetFloat64() (float64, error) {
	if rd.Remaining() < 8 {
		rem := rd.Remaining()
		return -1, errors.NewPacketDecodingError(fmt.Sprintf("Expected float64 length to be 8 bytes, got %d bytes", rem), "FLOAT64")
	}
	decodedFloat := math.Float64frombits(binary.BigEndian.Uint64(rd.bytes[rd.offset:]))
	rd.offset += 8
	return decodedFloat, nil
}

func (rd *Decoder) GetUnsignedVarint() (uint64, error) {
	decodedInteger, n := binary.Uvarint(rd.bytes[rd.offset:])
	if n == 0 {
		return 0, errors.NewPacketDecodingError("Unexpected end of data", "UNSIGNED_VARINT")
	}

	if n < 0 {
		return 0, errors.NewPacketDecodingError(fmt.Sprintf("Unexpected unsigned varint overflow after decoding %d bytes", -n), "UNSIGNED_VARINT")
	}

	rd.offset += n
	return decodedInteger, nil
}

func (rd *Decoder) GetSignedVarint() (int64, error) {
	decodedInteger, n := binary.Varint(rd.bytes[rd.offset:])
	if n == 0 {
		return -1, errors.NewPacketDecodingError("Unexpected end of data", "SIGNED_VARINT")
	}
	if n < 0 {
		return -1, errors.NewPacketDecodingError(fmt.Sprintf("Unexpected varint overflow after decoding %d bytes", -n), "SIGNED_VARINT")
	}
	rd.offset += n
	return decodedInteger, nil
}

func (rd *Decoder) GetArrayLength() (int, error) {
	if rd.Remaining() < 4 {
		rem := rd.Remaining()
		return -1, errors.NewPacketDecodingError(fmt.Sprintf("Expected array length prefix to be 4 bytes, got %d bytes", rem), "ARRAY_LENGTH")
	}
	decodedInteger, err := rd.GetInt32()
	if err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return -1, decodingErr.WithAddedContext("COMPACT_INT32_ARRAY")
		}
		return -1, err
	}
	arrayLength := int(decodedInteger)
	if arrayLength > rd.Remaining() {
		return -1, errors.NewPacketDecodingError(fmt.Sprintf("Expect to read at least %d bytes, but only %d bytes are remaining", arrayLength, rd.offset), "array length")
	} else if arrayLength > 2*math.MaxUint16 {
		return -1, errors.NewPacketDecodingError(fmt.Sprintf("Invalid array length: %d", arrayLength), "ARRAY_LENGTH")
	}

	return arrayLength, nil
}

func (rd *Decoder) GetCompactArrayLength() (int, error) {
	decodedInteger, err := rd.GetUnsignedVarint()
	if err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return 0, decodingErr.WithAddedContext("COMPACT_ARRAY_LENGTH")
		}
		return 0, err
	}

	if decodedInteger == 0 {
		return 0, nil
	}

	return int(decodedInteger) - 1, nil
}

func (rd *Decoder) GetBool() (bool, error) {
	decodedBool, err := rd.GetInt8()
	if err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return false, decodingErr.WithAddedContext("BOOLEAN")
		}
		return false, err
	}
	if decodedBool == 0 {
		return false, nil
	}
	if decodedBool != 1 {
		return false, errors.NewPacketDecodingError(fmt.Sprintf("Expected bool to be 1 or 0, got %d", decodedBool), "BOOLEAN")
	}
	return true, nil
}

func (rd *Decoder) GetEmptyTaggedFieldArray() (int, error) {
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

func (rd *Decoder) GetBytes() ([]byte, error) {
	bytesCount, err := rd.GetInt32()
	if err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return nil, decodingErr.WithAddedContext("RAW_BYTES")
		}
		return nil, err
	}
	if bytesCount == -1 {
		return nil, nil
	}

	return rd.GetRawBytes(int(bytesCount))
}

func (rd *Decoder) GetVarintBytes() ([]byte, error) {
	bytesCount, err := rd.GetSignedVarint()
	if err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return nil, decodingErr.WithAddedContext("VARINT_BYTES")
		}
		return nil, err
	}
	if bytesCount == -1 {
		return nil, nil
	}

	return rd.GetRawBytes(int(bytesCount))
}

func (rd *Decoder) GetCompactBytes() ([]byte, error) {
	bytesCount, err := rd.GetUnsignedVarint()
	if err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return nil, decodingErr.WithAddedContext("COMPACT_BYTES")
		}
		return nil, err
	}

	length := int(bytesCount - 1)
	return rd.GetRawBytes(length)
}

func (rd *Decoder) GetStringLength() (int, error) {
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
		return 0, errors.NewPacketDecodingError(fmt.Sprintf("Expect to read at least %d bytes, but only %d bytes are remaining", n, rd.offset), "STRING_LENGTH")
	}

	return n, nil
}

func (rd *Decoder) GetString() (string, error) {
	length, err := rd.GetStringLength()
	if err != nil || length == -1 {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return "", decodingErr.WithAddedContext("STRING")
		}
		return "", err
	}

	if rd.Remaining() < length {
		rem := rd.Remaining()
		return "", errors.NewPacketDecodingError(fmt.Sprintf("Expected string length to be %d bytes, got %d bytes", length, rem), "STRING")
	}

	tmpStr := string(rd.bytes[rd.offset : rd.offset+length])
	rd.offset += length
	return tmpStr, nil
}

func (rd *Decoder) GetNullableString() (*string, error) {
	length, err := rd.GetStringLength()
	if err != nil || length == -1 {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return nil, decodingErr.WithAddedContext("NULLABLE_STRING")
		}
		return nil, err
	}

	if rd.Remaining() < length {
		rem := rd.Remaining()
		return nil, errors.NewPacketDecodingError(fmt.Sprintf("Expected string length to be %d bytes, got %d bytes", length, rem), "NULLABLE_STRING")
	}

	tmpStr := string(rd.bytes[rd.offset : rd.offset+length])
	rd.offset += length
	return &tmpStr, nil
}

func (rd *Decoder) GetCompactString() (string, error) {
	decodedInteger, err := rd.GetUnsignedVarint()
	if err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return "", decodingErr.WithAddedContext("COMPACT_STRING")
		}
		return "", err
	}

	length := int(decodedInteger - 1)
	if length < 0 {
		return "", errors.NewPacketDecodingError(fmt.Sprintf("Expected COMPACT_STRING length to be > 0, got %d", length), "COMPACT_STRING")
	}

	if rd.Remaining() < length {
		rem := rd.Remaining()
		return "", errors.NewPacketDecodingError(fmt.Sprintf("Expected string length to be %d bytes, got %d bytes", length, rem), "COMPACT_STRING")
	}

	tmpStr := string(rd.bytes[rd.offset : rd.offset+length])
	rd.offset += length
	return tmpStr, nil
}

func (rd *Decoder) GetCompactNullableString() (*string, error) {
	decodedInteger, err := rd.GetUnsignedVarint()
	if err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return nil, decodingErr.WithAddedContext("COMPACT_NULLABLE_STRING")
		}
		return nil, err
	}

	// Null string
	if decodedInteger == 0 {
		return nil, nil
	}

	length := int(decodedInteger - 1)

	if length < 0 {
		return nil, errors.NewPacketDecodingError(fmt.Sprintf("Expected compact nullable string length to be > 0, got %d", length), "COMPACT_NULLABLE_STRING")
	}

	if rd.Remaining() < length {
		rem := rd.Remaining()
		return nil, errors.NewPacketDecodingError(fmt.Sprintf("Expected string length to be %d bytes, got %d bytes", length, rem), "COMPACT_NULLABLE_STRING")
	}

	tmpStr := string(rd.bytes[rd.offset : rd.offset+length])
	rd.offset += length
	return &tmpStr, nil
}

func (rd *Decoder) GetCompactInt32Array() ([]int32, error) {
	decodedInteger, err := rd.GetUnsignedVarint()
	if err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return nil, decodingErr.WithAddedContext("COMPACT_INT32_ARRAY")
		}
		return nil, err
	}

	if decodedInteger == 0 {
		return nil, nil
	}

	arrayLength := int(decodedInteger) - 1

	array := make([]int32, arrayLength)

	for i := range array {
		element, err := rd.GetInt32()
		if err != nil {
			if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
				return nil, decodingErr.WithAddedContext("COMPACT_INT32_ARRAY")
			}
			return nil, err
		}
		array[i] = element
	}
	return array, nil
}

func (rd *Decoder) GetInt32Array() ([]int32, error) {
	arrayLength, err := rd.GetArrayLength()
	if err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return nil, decodingErr.WithAddedContext("INT32_ARRAY")
		}
		return nil, err
	}

	if rd.Remaining() < 4*arrayLength {
		rem := rd.Remaining()
		return nil, errors.NewPacketDecodingError(fmt.Sprintf("Expected int32 array length to be %d bytes, got %d bytes", 4*arrayLength, rem), "INT32_ARRAY")
	}

	if arrayLength == 0 {
		return nil, nil
	}

	if arrayLength < 0 {
		return nil, errors.NewPacketDecodingError(fmt.Sprintf("Array length can only be -1 or > 0, got: %d", arrayLength), "INT32_ARRAY")
	}

	array := make([]int32, arrayLength)
	for i := range array {
		element, err := rd.GetInt32()
		if err != nil {
			if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
				return nil, decodingErr.WithAddedContext("COMPACT_INT32_ARRAY")
			}
			return nil, err
		}
		array[i] = element
	}
	return array, nil
}

func (rd *Decoder) GetInt64Array() ([]int64, error) {
	arrayLength, err := rd.GetArrayLength()
	if err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return nil, decodingErr.WithAddedContext("INT64_ARRAY")
		}
		return nil, err
	}

	if rd.Remaining() < 8*arrayLength {
		rem := rd.Remaining()
		return nil, errors.NewPacketDecodingError(fmt.Sprintf("Expected int64 array length to be %d bytes, got %d bytes", 8*arrayLength, rem), "INT64_ARRAY")
	}

	if arrayLength == 0 {
		return nil, nil
	}

	if arrayLength < 0 {
		return nil, errors.NewPacketDecodingError(fmt.Sprintf("Array length can only be -1 or > 0, got: %d", arrayLength), "INT64_ARRAY")
	}

	ret := make([]int64, arrayLength)
	for i := range ret {
		element, err := rd.GetInt64()
		if err != nil {
			if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
				return nil, decodingErr.WithAddedContext("COMPACT_INT32_ARRAY")
			}
			return nil, err
		}
		ret[i] = element
	}
	return ret, nil
}

func (rd *Decoder) GetStringArray() ([]string, error) {
	arrayLength, err := rd.GetArrayLength()
	if err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return nil, decodingErr.WithAddedContext("STRING_ARRAY")
		}
		return nil, err
	}

	if arrayLength == 0 {
		return nil, nil
	}

	if arrayLength < 0 {
		return nil, errors.NewPacketDecodingError(fmt.Sprintf("Array length can only be -1 or > 0, got: %d", arrayLength), "STRING_ARRAY")
	}

	array := make([]string, arrayLength)
	for i := range array {
		element, err := rd.GetString()
		if err != nil {
			if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
				return nil, decodingErr.WithAddedContext("STRING_ARRAY")
			}
			return nil, err
		}

		array[i] = element
	}
	return array, nil
}

func (rd *Decoder) Remaining() int {
	return len(rd.bytes) - rd.offset
}

func (rd *Decoder) GetRawBytes(length int) ([]byte, error) {
	if length < 0 {
		return nil, errors.NewPacketDecodingError(fmt.Sprintf("Expected length to be >= 0, got %d", length), "RAW_BYTES")
	} else if length > rd.Remaining() {
		return nil, errors.NewPacketDecodingError(fmt.Sprintf("Expected length to be lesser than remaining bytes (%d), got %d", rd.Remaining(), length), "RAW_BYTES")
	}

	start := rd.offset
	rd.offset += length
	return rd.bytes[start:rd.offset], nil
}

func (rd *Decoder) Offset() int {
	return rd.offset
}

func (rd *Decoder) GetRawBytesFromOffset(length int) ([]byte, error) {
	if length < 0 {
		return nil, errors.NewPacketDecodingError(fmt.Sprintf("Expected length to be >= 0, got %d", length), "RAW_BYTES_FROM_OFFSET")
	}

	if rd.offset >= len(rd.bytes) {
		return nil, errors.NewPacketDecodingError(fmt.Sprintf("Expected offset to be less than length of raw bytes (%d), got %d", len(rd.bytes), rd.offset), "RAW_BYTES_FROM_OFFSET")
	} else if rd.offset+length > len(rd.bytes) {
		return nil, errors.NewPacketDecodingError(fmt.Sprintf("Expected offset to be less than length of raw bytes (%d), got %d", len(rd.bytes), rd.offset), "RAW_BYTES_FROM_OFFSET")
	}

	return rd.bytes[rd.offset : rd.offset+length], nil
}

// FormatDetailedError formats the error message with the received bytes and the offset
func (rd *Decoder) FormatDetailedError(message string) error {
	lines := []string{}

	offset := rd.Offset()
	receivedBytes := rd.bytes
	receivedByteString := NewInspectableHexDump(receivedBytes)

	lines = append(lines, "Received:")
	lines = append(lines, receivedByteString.FormatWithHighlightedOffset(offset))
	lines = append(lines, message)

	//lint:ignore SA1006 we are okay with this
	return fmt.Errorf(strings.Join(lines, "\n"))
}
