package decoder

import (
	"encoding/binary"
	"fmt"
	"math"
	"strings"

	"github.com/codecrafters-io/kafka-tester/protocol/errors"
	"github.com/codecrafters-io/tester-utils/logger"
)

type Decoder struct {
	bytes              []byte
	offset             int
	logger             *logger.Logger
	backupLogger       *logger.Logger
	indentationLevel   int
	currentSectionName string
}

func (d *Decoder) Init(bytes []byte) {
	d.bytes = bytes
	d.offset = 0
}

// InitNew is the Parallel Change for Init()
// Every method will be postfixed using 'New' for parallel change
func (d *Decoder) InitNew(bytes []byte, logger *logger.Logger) {
	d.bytes = bytes
	d.offset = 0
	d.logger = logger.Clone()
	d.logger.UpdateLastSecondaryPrefix("Decoder")
	d.backupLogger = nil
}

func (d *Decoder) BeginSubSection(sectionName string) {
	d.currentSectionName = sectionName
	dot := "."
	// don't print . on first indentation level
	if d.indentationLevel == 0 {
		dot = ""
	}

	d.logger.Debugf("%s%s%s", d.getIndentationString(), dot, sectionName)
	d.indentLog()
}

func (d *Decoder) EndCurrentSubSection() {
	d.unindentLog()
}

func (d *Decoder) indentLog() {
	d.indentationLevel += 1
}

func (d *Decoder) unindentLog() {
	d.indentationLevel = max(d.indentationLevel-1, 0)
}

func (d *Decoder) LogDecodedValue(value string) {
	d.logger.Debugf("%s.%s", d.getIndentationString(), value)
}

func (d *Decoder) MuteLogger() {
	d.backupLogger = d.logger
	mutedLogger := d.backupLogger.Clone()
	mutedLogger.IsDebug = false
	mutedLogger.IsQuiet = true
	d.logger = mutedLogger
}

func (d *Decoder) UnmuteLogger() {
	if d.backupLogger == nil {
		return
	}
	d.logger = d.backupLogger
	d.backupLogger = nil
}

func (d *Decoder) GetInt8() (int8, error) {
	if d.Remaining() < 1 {
		rem := d.Remaining()
		return -1, errors.NewPacketDecodingError(fmt.Sprintf("Expected int8 length to be 1 byte, got %d bytes", rem), "INT8")
	}
	decodedInteger := int8(d.bytes[d.offset])
	d.offset++
	return decodedInteger, nil
}

func (d *Decoder) GetInt16() (int16, error) {
	if d.Remaining() < 2 {
		rem := d.Remaining()
		return -1, errors.NewPacketDecodingError(fmt.Sprintf("Expected int16 length to be 2 bytes, got %d bytes", rem), "INT16")
	}
	decodedInteger := int16(binary.BigEndian.Uint16(d.bytes[d.offset:]))
	d.offset += 2
	return decodedInteger, nil
}

// GetInt16_Updated is the Parallels change version of GetInt16()
func (d *Decoder) GetInt16_Updated(variableName string) (int16, error) {
	if d.Remaining() < 2 {
		rem := d.Remaining()
		errorMessage := fmt.Sprintf("Expected int16 length to be 2 bytes, got %d bytes", rem)
		return -1, errors.NewPacketDecodingError(errorMessage, "INT16", variableName)
	}
	decodedInteger := int16(binary.BigEndian.Uint16(d.bytes[d.offset:]))
	d.offset += 2
	d.LogDecodedValue(fmt.Sprintf("%s (%d)", variableName, decodedInteger))
	return decodedInteger, nil
}

func (d *Decoder) GetInt32() (int32, error) {
	if d.Remaining() < 4 {
		rem := d.Remaining()
		return -1, errors.NewPacketDecodingError(fmt.Sprintf("Expected int32 length to be 4 bytes, got %d bytes", rem), "INT32")
	}
	decodedInteger := int32(binary.BigEndian.Uint32(d.bytes[d.offset:]))
	d.offset += 4
	return decodedInteger, nil
}

// GetInt32_Updated is the Parallel change version of GetInt32
func (d *Decoder) GetInt32_Updated(variableName string) (int32, error) {
	if d.Remaining() < 4 {
		rem := d.Remaining()
		errorMessage := fmt.Sprintf("Expected int32 length to be 4 bytes, got %d bytes", rem)
		err := errors.NewPacketDecodingError(errorMessage, "INT32", variableName)
		return -1, err
	}
	decodedInteger := int32(binary.BigEndian.Uint32(d.bytes[d.offset:]))
	d.offset += 4
	d.LogDecodedValue(fmt.Sprintf("%s (%d)", variableName, decodedInteger))
	return decodedInteger, nil
}

func (d *Decoder) GetInt64() (int64, error) {
	if d.Remaining() < 8 {
		rem := d.Remaining()
		return -1, errors.NewPacketDecodingError(fmt.Sprintf("Expected int64 length to be 8 bytes, got %d bytes", rem), "INT64")
	}
	decodedInteger := int64(binary.BigEndian.Uint64(d.bytes[d.offset:]))
	d.offset += 8
	return decodedInteger, nil
}

func (d *Decoder) GetFloat64() (float64, error) {
	if d.Remaining() < 8 {
		rem := d.Remaining()
		return -1, errors.NewPacketDecodingError(fmt.Sprintf("Expected float64 length to be 8 bytes, got %d bytes", rem), "FLOAT64")
	}
	decodedFloat := math.Float64frombits(binary.BigEndian.Uint64(d.bytes[d.offset:]))
	d.offset += 8
	return decodedFloat, nil
}

func (d *Decoder) GetUnsignedVarint() (uint64, error) {
	decodedInteger, n := binary.Uvarint(d.bytes[d.offset:])
	if n == 0 {
		return 0, errors.NewPacketDecodingError("Unexpected end of data", "UNSIGNED_VARINT")
	}

	if n < 0 {
		return 0, errors.NewPacketDecodingError(fmt.Sprintf("Unexpected unsigned varint overflow after decoding %d bytes", -n), "UNSIGNED_VARINT")
	}

	d.offset += n
	return decodedInteger, nil
}

// GetUnsignedVarint_Updated is parallels version of GetUnsignedVarint
func (d *Decoder) GetUnsignedVarint_Updated(variableName string) (uint64, error) {
	decodedInteger, n := binary.Uvarint(d.bytes[d.offset:])
	if n == 0 {
		return 0, errors.NewPacketDecodingError("Unexpected end of data", "UNSIGNED_VARINT", variableName)
	}

	if n < 0 {
		return 0, errors.NewPacketDecodingError(fmt.Sprintf("Unexpected unsigned varint overflow after decoding %d bytes", -n), "UNSIGNED_VARINT", variableName)
	}

	d.offset += n
	d.LogDecodedValue(fmt.Sprintf("%s (%d)", variableName, decodedInteger))
	return decodedInteger, nil
}

func (d *Decoder) GetSignedVarint() (int64, error) {
	decodedInteger, n := binary.Varint(d.bytes[d.offset:])
	if n == 0 {
		return -1, errors.NewPacketDecodingError("Unexpected end of data", "SIGNED_VARINT")
	}
	if n < 0 {
		return -1, errors.NewPacketDecodingError(fmt.Sprintf("Unexpected varint overflow after decoding %d bytes", -n), "SIGNED_VARINT")
	}
	d.offset += n
	return decodedInteger, nil
}

func (d *Decoder) GetArrayLength() (int, error) {
	if d.Remaining() < 4 {
		rem := d.Remaining()
		return -1, errors.NewPacketDecodingError(fmt.Sprintf("Expected array length prefix to be 4 bytes, got %d bytes", rem), "ARRAY_LENGTH")
	}
	decodedInteger, err := d.GetInt32()
	if err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return -1, decodingErr.WithAddedContext("COMPACT_INT32_ARRAY")
		}
		return -1, err
	}
	arrayLength := int(decodedInteger)
	if arrayLength > d.Remaining() {
		return -1, errors.NewPacketDecodingError(fmt.Sprintf("Expect to read at least %d bytes, but only %d bytes are remaining", arrayLength, d.offset), "array length")
	} else if arrayLength > 2*math.MaxUint16 {
		return -1, errors.NewPacketDecodingError(fmt.Sprintf("Invalid array length: %d", arrayLength), "ARRAY_LENGTH")
	}

	return arrayLength, nil
}

// GetArrayLength_Updated is parallels version of GetArrayLength
func (d *Decoder) GetArrayLength_Updated(variableName string) (int, error) {
	if d.Remaining() < 4 {
		rem := d.Remaining()
		return -1, errors.NewPacketDecodingError(fmt.Sprintf("Expected array length prefix to be 4 bytes, got %d bytes", rem), "ARRAY_LENGTH")
	}

	decodedInteger, err := d.GetInt32()

	if err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return -1, decodingErr.WithAddedContext("COMPACT_INT32_ARRAY")
		}
		return -1, err
	}

	arrayLength := int(decodedInteger)

	if arrayLength > d.Remaining() {
		return -1, errors.NewPacketDecodingError(fmt.Sprintf("Expect to read at least %d bytes, but only %d bytes are remaining", arrayLength, d.offset), "array length")
	} else if arrayLength > 2*math.MaxUint16 {
		return -1, errors.NewPacketDecodingError(fmt.Sprintf("Invalid array length: %d", arrayLength), "ARRAY_LENGTH")
	}

	d.LogDecodedValue(fmt.Sprintf("%s (%d)", variableName, arrayLength))
	return arrayLength, nil
}

func (d *Decoder) GetCompactArrayLength() (int, error) {
	decodedInteger, err := d.GetUnsignedVarint()
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

// GetCompactArrayLength_Updated is parallels version of GetcompactArrayLength
func (d *Decoder) GetCompactArrayLength_Updated(variableName string) (int, error) {
	decodedInteger, err := d.GetUnsignedVarint()

	if err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return 0, decodingErr.WithAddedContext(variableName)
		}
		return 0, err
	}

	var arrayLength int
	if decodedInteger == 0 {
		arrayLength = 0
	} else {
		arrayLength = int(decodedInteger) - 1
	}

	d.LogDecodedValue(fmt.Sprintf("%s (%d)", variableName, arrayLength))

	return arrayLength, nil
}

func (d *Decoder) GetBool() (bool, error) {
	decodedBool, err := d.GetInt8()
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

func (d *Decoder) GetEmptyTaggedFieldArray() (int, error) {
	tagCount, err := d.GetUnsignedVarint()
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
		_, err := d.GetUnsignedVarint()
		if err != nil {
			if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
				return 0, decodingErr.WithAddedContext("TAGGED_FIELD_ARRAY")
			}
			return 0, err
		}
		length, err := d.GetUnsignedVarint()
		if err != nil {
			if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
				return 0, decodingErr.WithAddedContext("TAGGED_FIELD_ARRAY")
			}
			return 0, err
		}
		_, err = d.GetRawBytes(int(length))
		if err != nil {
			if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
				return 0, decodingErr.WithAddedContext("TAGGED_FIELD_ARRAY")
			}
			return 0, err
		}
	}

	return 0, nil
}

// ConsumeTagBuffer is a parallel of GetEmptyTaggedFieldArray
func (d *Decoder) ConsumeTagBuffer() error {
	d.MuteLogger()
	// for error cases
	defer d.UnmuteLogger()
	tagCount, err := d.GetUnsignedVarint_Updated("TAG_BUFFER")

	if err != nil {
		return err
	}

	// skip over any tagged fields without deserializing them
	// as we don't currently support doing anything with them
	for i := uint64(0); i < tagCount; i++ {
		// fetch and ignore tag identifier
		_, err := d.GetUnsignedVarint_Updated("TAG_BUFFER")

		if err != nil {
			return err
		}

		length, err := d.GetUnsignedVarint_Updated("TAG_BUFFER")

		if err != nil {
			return err
		}

		_, err = d.ConsumeRawBytes(int(length), "TAG_BUFFER")

		if err != nil {
			return err
		}
	}

	d.UnmuteLogger()
	d.LogDecodedValue("TAG_BUFFER")
	return nil
}

func (d *Decoder) GetBytes() ([]byte, error) {
	bytesCount, err := d.GetInt32()
	if err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return nil, decodingErr.WithAddedContext("RAW_BYTES")
		}
		return nil, err
	}
	if bytesCount == -1 {
		return nil, nil
	}

	return d.GetRawBytes(int(bytesCount))
}

func (d *Decoder) GetVarintBytes() ([]byte, error) {
	bytesCount, err := d.GetSignedVarint()
	if err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return nil, decodingErr.WithAddedContext("VARINT_BYTES")
		}
		return nil, err
	}
	if bytesCount == -1 {
		return nil, nil
	}

	return d.GetRawBytes(int(bytesCount))
}

func (d *Decoder) GetCompactBytes() ([]byte, error) {
	bytesCount, err := d.GetUnsignedVarint()
	if err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return nil, decodingErr.WithAddedContext("COMPACT_BYTES")
		}
		return nil, err
	}

	length := int(bytesCount - 1)
	return d.GetRawBytes(length)
}

func (d *Decoder) GetStringLength() (int, error) {
	length, err := d.GetInt16()
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
	case n > d.Remaining():
		return 0, errors.NewPacketDecodingError(fmt.Sprintf("Expect to read at least %d bytes, but only %d bytes are remaining", n, d.offset), "STRING_LENGTH")
	}

	return n, nil
}

func (d *Decoder) GetString() (string, error) {
	length, err := d.GetStringLength()
	if err != nil || length == -1 {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return "", decodingErr.WithAddedContext("STRING")
		}
		return "", err
	}

	if d.Remaining() < length {
		rem := d.Remaining()
		return "", errors.NewPacketDecodingError(fmt.Sprintf("Expected string length to be %d bytes, got %d bytes", length, rem), "STRING")
	}

	tmpStr := string(d.bytes[d.offset : d.offset+length])
	d.offset += length
	return tmpStr, nil
}

func (d *Decoder) GetNullableString() (*string, error) {
	length, err := d.GetStringLength()
	if err != nil || length == -1 {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return nil, decodingErr.WithAddedContext("NULLABLE_STRING")
		}
		return nil, err
	}

	if d.Remaining() < length {
		rem := d.Remaining()
		return nil, errors.NewPacketDecodingError(fmt.Sprintf("Expected string length to be %d bytes, got %d bytes", length, rem), "NULLABLE_STRING")
	}

	tmpStr := string(d.bytes[d.offset : d.offset+length])
	d.offset += length
	return &tmpStr, nil
}

func (d *Decoder) GetCompactString() (string, error) {
	decodedInteger, err := d.GetUnsignedVarint()
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

	if d.Remaining() < length {
		rem := d.Remaining()
		return "", errors.NewPacketDecodingError(fmt.Sprintf("Expected string length to be %d bytes, got %d bytes", length, rem), "COMPACT_STRING")
	}

	tmpStr := string(d.bytes[d.offset : d.offset+length])
	d.offset += length
	return tmpStr, nil
}

func (d *Decoder) GetCompactNullableString() (*string, error) {
	decodedInteger, err := d.GetUnsignedVarint()
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

	if d.Remaining() < length {
		rem := d.Remaining()
		return nil, errors.NewPacketDecodingError(fmt.Sprintf("Expected string length to be %d bytes, got %d bytes", length, rem), "COMPACT_NULLABLE_STRING")
	}

	tmpStr := string(d.bytes[d.offset : d.offset+length])
	d.offset += length
	return &tmpStr, nil
}

func (d *Decoder) GetCompactInt32Array() ([]int32, error) {
	decodedInteger, err := d.GetUnsignedVarint()
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
		element, err := d.GetInt32()
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

func (d *Decoder) GetInt32Array() ([]int32, error) {
	arrayLength, err := d.GetArrayLength()
	if err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return nil, decodingErr.WithAddedContext("INT32_ARRAY")
		}
		return nil, err
	}

	if d.Remaining() < 4*arrayLength {
		rem := d.Remaining()
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
		element, err := d.GetInt32()
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

func (d *Decoder) GetInt64Array() ([]int64, error) {
	arrayLength, err := d.GetArrayLength()
	if err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return nil, decodingErr.WithAddedContext("INT64_ARRAY")
		}
		return nil, err
	}

	if d.Remaining() < 8*arrayLength {
		rem := d.Remaining()
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
		element, err := d.GetInt64()
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

func (d *Decoder) GetStringArray() ([]string, error) {
	arrayLength, err := d.GetArrayLength()
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
		element, err := d.GetString()
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

func (d *Decoder) Remaining() int {
	return len(d.bytes) - d.offset
}

func (d *Decoder) GetRawBytes(length int) ([]byte, error) {
	if length < 0 {
		return nil, errors.NewPacketDecodingError(fmt.Sprintf("Expected length to be >= 0, got %d", length), "RAW_BYTES")
	} else if length > d.Remaining() {
		return nil, errors.NewPacketDecodingError(fmt.Sprintf("Expected length to be lesser than remaining bytes (%d), got %d", d.Remaining(), length), "RAW_BYTES")
	}

	start := d.offset
	d.offset += length
	return d.bytes[start:d.offset], nil
}

// ConsumeRawBytes is parallels version of GetRawBytes
func (d *Decoder) ConsumeRawBytes(length int, variableName string) ([]byte, error) {
	if length < 0 {
		return nil, errors.NewPacketDecodingError(fmt.Sprintf("Expected length to be >= 0, got %d", length), "RAW_BYTES", variableName)
	} else if length > d.Remaining() {
		return nil, errors.NewPacketDecodingError(fmt.Sprintf("Expected length to be lesser than remaining bytes (%d), got %d", d.Remaining(), length), "RAW_BYTES", variableName)
	}

	start := d.offset
	d.offset += length
	return d.bytes[start:d.offset], nil
}

func (d *Decoder) Offset() int {
	return d.offset
}

func (d *Decoder) GetRawBytesFromOffset(length int) ([]byte, error) {
	if length < 0 {
		return nil, errors.NewPacketDecodingError(fmt.Sprintf("Expected length to be >= 0, got %d", length), "RAW_BYTES_FROM_OFFSET")
	}

	if d.offset >= len(d.bytes) {
		return nil, errors.NewPacketDecodingError(fmt.Sprintf("Expected offset to be less than length of raw bytes (%d), got %d", len(d.bytes), d.offset), "RAW_BYTES_FROM_OFFSET")
	} else if d.offset+length > len(d.bytes) {
		return nil, errors.NewPacketDecodingError(fmt.Sprintf("Expected offset to be less than length of raw bytes (%d), got %d", len(d.bytes), d.offset), "RAW_BYTES_FROM_OFFSET")
	}

	return d.bytes[d.offset : d.offset+length], nil
}

// FormatDetailedError formats the error message with the received bytes and the offset
func (d *Decoder) FormatDetailedError(message string) error {
	lines := []string{}

	offset := d.Offset()
	receivedBytes := d.bytes
	receivedByteString := NewInspectableHexDump(receivedBytes)

	lines = append(lines, "Received:")
	lines = append(lines, receivedByteString.FormatWithHighlightedOffset(offset))
	lines = append(lines, message)

	//lint:ignore SA1006 we are okay with this
	return fmt.Errorf(strings.Join(lines, "\n"))
}

func (d *Decoder) getIndentationString() string {
	return strings.Repeat("  ", d.indentationLevel)
}
