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

type Decoder struct {
	bytes            []byte
	offset           int
	logger           *logger.Logger
	indentationLevel int
}

func NewDecoder(bytes []byte, logger *logger.Logger) *Decoder {
	decoderLogger := logger.Clone()
	decoderLogger.UpdateLastSecondaryPrefix("Decoder")
	return &Decoder{
		bytes:            bytes,
		offset:           0,
		logger:           decoderLogger,
		indentationLevel: 0,
	}
}

func (d *Decoder) Offset() int {
	return d.offset
}

func (d *Decoder) UnreadBytesCount() int {
	return len(d.bytes) - d.offset
}

func (d *Decoder) indentLog() {
	d.indentationLevel += 1
}

func (d *Decoder) unindentLog() {
	d.indentationLevel = max(d.indentationLevel-1, 0)
}

func (d *Decoder) BeginSubSection(sectionName string) {
	d.logger.Debugf("%s%s", d.getIndentationString(), sectionName)
	d.indentLog()
}

func (d *Decoder) EndCurrentSubSection() {
	d.unindentLog()
}

func (d *Decoder) logDecodedValue(variableName string, value any) {
	if variableName == "" {
		return
	}

	indentationString := d.getIndentationString()

	switch castedValue := value.(type) {
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		d.logger.Debugf("%s%s (%d)", indentationString, variableName, castedValue)
	case string:
		d.logger.Debugf("%s%s (%s)", indentationString, variableName, castedValue)
	case bool:
		d.logger.Debugf("%s%s (%t)", indentationString, variableName, castedValue)
	default:
		d.logger.Debugf("%s%s (%v)", indentationString, variableName, castedValue)
	}
}

func (d *Decoder) logTagBuffer() {
	d.logger.Debugf("%s%s", d.getIndentationString(), "TAG_BUFFER")
}

// Primitive Types

func (d *Decoder) ReadInt16(variableName string) (int16, error) {
	if d.UnreadBytesCount() < 2 {
		rem := d.UnreadBytesCount()
		errorMessage := fmt.Sprintf("Expected int16 length to be 2 bytes, got %d bytes", rem)
		return -1, errors.NewPacketDecodingError(errorMessage).AddContexts("INT16", variableName)
	}

	decodedInteger := int16(binary.BigEndian.Uint16(d.bytes[d.offset:]))
	d.offset += 2
	d.logDecodedValue(variableName, decodedInteger)
	return decodedInteger, nil
}

func (d *Decoder) ReadInt32(variableName string) (int32, error) {
	if d.UnreadBytesCount() < 4 {
		rem := d.UnreadBytesCount()
		errorMessage := fmt.Sprintf("Expected int32 length to be 4 bytes, got %d bytes", rem)
		err := errors.NewPacketDecodingError(errorMessage).AddContexts("INT32", variableName)
		return -1, err
	}

	decodedInteger := int32(binary.BigEndian.Uint32(d.bytes[d.offset:]))
	d.offset += 4
	d.logDecodedValue(variableName, decodedInteger)
	return decodedInteger, nil
}

func (d *Decoder) ReadUnsignedVarint(variableName string) (uint64, error) {
	decodedInteger, n := binary.Uvarint(d.bytes[d.offset:])

	if n == 0 {
		return 0, errors.NewPacketDecodingError("Unexpected end of data").AddContexts("UNSIGNED_VARINT", variableName)
	}

	if n < 0 {
		errorMessage := fmt.Sprintf("Unexpected unsigned varint overflow after decoding %d bytes", -n)
		return 0, errors.NewPacketDecodingError(errorMessage).AddContexts("UNSIGNED_VARINT", variableName)
	}

	d.offset += n
	d.logDecodedValue(variableName, decodedInteger)
	return decodedInteger, nil
}

// Composite Types : Uses Basic Types for decoding

func (d *Decoder) ReadArrayLength(variableName string) (int, error) {
	if d.UnreadBytesCount() < 4 {
		rem := d.UnreadBytesCount()
		errorMessage := fmt.Sprintf("Expected array length prefix to be 4 bytes, got %d bytes", rem)
		return -1, errors.NewPacketDecodingError(errorMessage).AddContexts("ARRAY_LENGTH", variableName)
	}
	decodedInteger, err := d.ReadInt32("")

	if err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return -1, decodingErr.AddContexts("ARRAY_LENGTH", variableName)
		}
		return -1, err
	}

	arrayLength := int(decodedInteger)

	if arrayLength > d.UnreadBytesCount() {
		errorMessage := fmt.Sprintf("Expect to read at least %d bytes, but only %d bytes are remaining", arrayLength, d.UnreadBytesCount())
		return -1, errors.NewPacketDecodingError(errorMessage).AddContexts("ARRAY_LENGTH", variableName)
	} else if arrayLength > 2*math.MaxUint16 {
		return -1, errors.NewPacketDecodingError(fmt.Sprintf("Invalid array length: %d", arrayLength)).AddContexts("ARRAY_LENGTH", variableName)
	}

	d.logDecodedValue(variableName, arrayLength)
	return arrayLength, nil
}

func (d *Decoder) ReadCompactArrayLength(variableName string) (int, error) {
	decodedInteger, err := d.ReadUnsignedVarint("")

	if err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return 0, decodingErr.AddContexts("COMPACT_ARRAY_LENGTH", variableName)
		}
		return 0, err
	}

	if decodedInteger == 0 {
		return 0, nil
	}

	d.logDecodedValue(variableName, decodedInteger)
	return int(decodedInteger) - 1, nil
}

func (d *Decoder) ConsumeTagBuffer() error {
	tagCount, err := d.ReadUnsignedVarint("")

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
		_, err := d.ReadUnsignedVarint("")

		if err != nil {
			if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
				return decodingErr.AddContexts("TAG_BUFFER")
			}

			return err
		}

		length, err := d.ReadUnsignedVarint("")

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

	d.logTagBuffer()
	return nil
}

func (d *Decoder) ConsumeRawBytes(length int, variableName string) ([]byte, error) {
	if length < 0 {
		errorMessage := fmt.Sprintf("Expected length to be >= 0, got %d", length)
		return nil, errors.NewPacketDecodingError(errorMessage).AddContexts("RAW_BYTES", variableName)
	} else if length > d.UnreadBytesCount() {
		errorMessage := fmt.Sprintf("Expected length to be lesser than remaining bytes (%d), got %d", d.UnreadBytesCount(), length)
		return nil, errors.NewPacketDecodingError(errorMessage).AddContexts("RAW_BYTES", variableName)
	}

	start := d.offset
	d.offset += length
	return d.bytes[start:d.offset], nil
}

// FormatDetailedError formats the error message with the received bytes and the offset
func (d *Decoder) FormatDetailedError(message string) error {
	lines := []string{}

	offset := d.Offset()
	receivedBytes := d.bytes
	receivedBytesHexDump := NewInspectableHexDump(receivedBytes)

	lines = append(lines, "Received:")
	lines = append(lines, receivedBytesHexDump.FormatWithHighlightedOffset(offset))
	lines = append(lines, message)

	return go_errors.New(strings.Join(lines, "\n"))
}

func (d *Decoder) getIndentationString() string {
	indentationSpaces := strings.Repeat("  ", d.indentationLevel)
	bullet := "- "

	// Only need dot for indented level
	if d.indentationLevel != 0 {
		bullet = "- ."
	}

	return indentationSpaces + bullet
}
