package decoder

import (
	"encoding/binary"
	"fmt"
	"math"
	"strings"

	go_errors "errors"

	"github.com/codecrafters-io/kafka-tester/protocol/errors"
	"github.com/codecrafters-io/tester-utils/logger"
)

type tagBuffer struct{}

type Decoder struct {
	bytes              []byte
	offset             int
	logger             *logger.Logger
	backupLogger       *logger.Logger
	indentationLevel   int
	currentSectionName string
}

func (d *Decoder) Init(bytes []byte, logger *logger.Logger) {
	d.bytes = bytes
	d.offset = 0
	d.logger = logger.Clone()
	d.logger.UpdateLastSecondaryPrefix("Decoder")
	d.backupLogger = nil
}

func (d *Decoder) BeginSubSection(sectionName string) {
	d.currentSectionName = sectionName
	dot := "."
	// don't print (.) on first indentation level
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

func (d *Decoder) LogDecodedValue(variableName string, value any) {
	indentation := d.getIndentationString()
	if variableName != "" {
		switch castedValue := value.(type) {
		case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
			d.logger.Debugf("%s.%s (%d)", indentation, variableName, castedValue)
		case string:
			d.logger.Debugf("%s.%s (%s)", indentation, variableName, castedValue)
		case bool:
			d.logger.Debugf("%s.%s (%t)", indentation, variableName, castedValue)
		case tagBuffer:
			d.logger.Debugf("%s.%s", indentation, "TAG_BUFFER")
		default:
			d.logger.Debugf("%s%s (%v)", indentation, variableName, castedValue)
		}
	}
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

/* Basic Types */

func (d *Decoder) GetInt8(variableName string) (int8, error) {
	if d.Remaining() < 1 {
		rem := d.Remaining()
		errorMessage := fmt.Sprintf("Expected int8 length to be 1 byte, got %d bytes", rem)
		return -1, errors.NewPacketDecodingError(errorMessage).AddContexts("INT8", variableName)
	}

	decodedInteger := int8(d.bytes[d.offset])
	d.offset++

	d.LogDecodedValue(variableName, decodedInteger)
	return decodedInteger, nil
}

func (d *Decoder) GetInt16(variableName string) (int16, error) {
	if d.Remaining() < 2 {
		rem := d.Remaining()
		errorMessage := fmt.Sprintf("Expected int16 length to be 2 bytes, got %d bytes", rem)
		return -1, errors.NewPacketDecodingError(errorMessage).AddContexts("INT16", variableName)
	}

	decodedInteger := int16(binary.BigEndian.Uint16(d.bytes[d.offset:]))
	d.offset += 2
	d.LogDecodedValue(variableName, decodedInteger)
	return decodedInteger, nil
}

func (d *Decoder) GetInt32(variableName string) (int32, error) {
	if d.Remaining() < 4 {
		rem := d.Remaining()
		errorMessage := fmt.Sprintf("Expected int32 length to be 4 bytes, got %d bytes", rem)
		err := errors.NewPacketDecodingError(errorMessage).AddContexts("INT32", variableName)
		return -1, err
	}

	decodedInteger := int32(binary.BigEndian.Uint32(d.bytes[d.offset:]))
	d.offset += 4
	d.LogDecodedValue(variableName, decodedInteger)
	return decodedInteger, nil
}

func (d *Decoder) GetInt64(variableName string) (int64, error) {
	if d.Remaining() < 8 {
		rem := d.Remaining()
		errorMessage := fmt.Sprintf("Expected int64 length to be 8 bytes, got %d bytes", rem)
		return -1, errors.NewPacketDecodingError(errorMessage).AddContexts("INT64", variableName)
	}

	decodedInteger := int64(binary.BigEndian.Uint64(d.bytes[d.offset:]))
	d.offset += 8
	d.LogDecodedValue(variableName, decodedInteger)
	return decodedInteger, nil
}

func (d *Decoder) GetUnsignedVarint(variableName string) (uint64, error) {
	decodedInteger, n := binary.Uvarint(d.bytes[d.offset:])

	if n == 0 {
		return 0, errors.NewPacketDecodingError("Unexpected end of data").AddContexts("UNSIGNED_VARINT", variableName)
	}

	if n < 0 {
		errorMessage := fmt.Sprintf("Unexpected unsigned varint overflow after decoding %d bytes", -n)
		return 0, errors.NewPacketDecodingError(errorMessage).AddContexts("UNSIGNED_VARINT", variableName)
	}

	d.offset += n
	d.LogDecodedValue(variableName, decodedInteger)
	return decodedInteger, nil
}

func (d *Decoder) GetSignedVarint(variableName string) (int64, error) {
	decodedInteger, n := binary.Varint(d.bytes[d.offset:])
	if n == 0 {
		return -1, errors.NewPacketDecodingError("Unexpected end of data").AddContexts("SIGNED_VARINT", variableName)
	}

	if n < 0 {
		errorMessage := fmt.Sprintf("Unexpected varint overflow after decoding %d bytes", -n)
		return -1, errors.NewPacketDecodingError(errorMessage).AddContexts("SIGNED_VARINT", variableName)
	}

	d.offset += n
	d.LogDecodedValue(variableName, decodedInteger)
	return decodedInteger, nil
}

func (d *Decoder) GetBool(variableName string) (bool, error) {
	if d.Remaining() < 1 {
		rem := d.Remaining()
		errorMessage := fmt.Sprintf("Expected boolean length to be 1 byte, got %d bytes", rem)
		return false, errors.NewPacketDecodingError(errorMessage).AddContexts("BOOLEAN", variableName)
	}

	decodedInteger := int8(d.bytes[d.offset])

	if decodedInteger != 0 && decodedInteger != 1 {
		errorMessage := fmt.Sprintf("Expected bool to be 1 or 0, got %d", decodedInteger)
		return false, errors.NewPacketDecodingError(errorMessage).AddContexts("BOOLEAN", variableName)
	}

	decodedBool := false

	if decodedInteger == 1 {
		decodedBool = true
	}

	d.offset++
	d.LogDecodedValue(variableName, decodedBool)
	return decodedBool, nil
}

/* Composite Types : Uses Basic Types for decoding */

func (d *Decoder) GetArrayLength(variableName string) (int, error) {
	if d.Remaining() < 4 {
		rem := d.Remaining()
		errorMessage := fmt.Sprintf("Expected array length prefix to be 4 bytes, got %d bytes", rem)
		return -1, errors.NewPacketDecodingError(errorMessage).AddContexts("ARRAY_LENGTH", variableName)
	}
	decodedInteger, err := d.GetInt32("")

	if err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return -1, decodingErr.AddContexts("ARRAY_LENGTH", variableName)
		}
		return -1, err
	}

	arrayLength := int(decodedInteger)

	if arrayLength > d.Remaining() {
		errorMessage := fmt.Sprintf("Expect to read at least %d bytes, but only %d bytes are remaining", arrayLength, d.offset)
		return -1, errors.NewPacketDecodingError(errorMessage).AddContexts("ARRAY_LENGTH", variableName)
	} else if arrayLength > 2*math.MaxUint16 {
		return -1, errors.NewPacketDecodingError(fmt.Sprintf("Invalid array length: %d", arrayLength)).AddContexts("ARRAY_LENGTH", variableName)
	}

	d.LogDecodedValue(variableName, arrayLength)
	return arrayLength, nil
}

func (d *Decoder) GetCompactArrayLength(variableName string) (int, error) {
	decodedInteger, err := d.GetUnsignedVarint("")

	if err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return 0, decodingErr.AddContexts("COMPACT_ARRAY_LENGTH", variableName)
		}
		return 0, err
	}

	if decodedInteger == 0 {
		return 0, nil
	}

	d.LogDecodedValue(variableName, decodedInteger)
	return int(decodedInteger) - 1, nil
}

func (d *Decoder) ConsumeTagBuffer() error {
	tagCount, err := d.GetUnsignedVarint("")

	if err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return decodingErr.AddContexts("TAG_BUFFER")
		}

		return err
	}

	// skip over any tagged fields without deserializing them
	// as we don't currently support doing anything with them
	for range tagCount {
		// fetch and ignore tag identifier
		_, err := d.GetUnsignedVarint("")

		if err != nil {
			if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
				return decodingErr.AddContexts("TAG_BUFFER")
			}

			return err
		}

		length, err := d.GetUnsignedVarint("")

		if err != nil {
			if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
				return decodingErr.AddContexts("TAG_BUFFER")
			}

			return err
		}

		_, err = d.ConsumeRawBytes(int(length), "")

		if err != nil {
			if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
				return decodingErr.AddContexts("TAG_BUFFER")
			}

			return err
		}
	}

	d.LogDecodedValue("", tagBuffer{})
	return nil
}

func (d *Decoder) Remaining() int {
	return len(d.bytes) - d.offset
}

// ConsumeRawBytes is parallels version of GetRawBytes
func (d *Decoder) ConsumeRawBytes(length int, variableName string) ([]byte, error) {
	if length < 0 {
		errorMessage := fmt.Sprintf("Expected length to be >= 0, got %d", length)
		return nil, errors.NewPacketDecodingError(errorMessage).AddContexts("RAW_BYTES", variableName)
	} else if length > d.Remaining() {
		errorMessage := fmt.Sprintf("Expected length to be lesser than remaining bytes (%d), got %d", d.Remaining(), length)
		return nil, errors.NewPacketDecodingError(errorMessage).AddContexts("RAW_BYTES", variableName)
	}

	start := d.offset
	d.offset += length
	return d.bytes[start:d.offset], nil
}

func (d *Decoder) Offset() int {
	return d.offset
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

	return go_errors.New(strings.Join(lines, "\n"))
}

func (d *Decoder) getIndentationString() string {
	return strings.Repeat("  ", d.indentationLevel)
}
