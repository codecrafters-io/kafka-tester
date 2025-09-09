package decoder

import (
	"encoding/binary"
	"fmt"
	"strings"

	go_errors "errors"

	"github.com/codecrafters-io/kafka-tester/internal/inspectable_hex_dump"
	"github.com/codecrafters-io/kafka-tester/protocol/errors"
	"github.com/codecrafters-io/kafka-tester/protocol/value"
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

func (d *Decoder) logDecodedValue(variableName string, value value.KafkaProtocolValue) {
	// WILL_REMOVE LATER: Not sure if this is a good approach, if variable name is not empty, we don't log it
	// useful in cases where we don't want to log intermediate variables. (eg. see line 192 in ConsumeTagBuffer)
	// However, it isn't so obvious from the usage
	// If i'd made this a string pointer, a lot more changes were requred and constructs like (&"variable_name") wouldn't have been possible
	// Need some feedback on this
	if variableName == "" {
		return
	}

	d.logger.Debugf("%s%s (%s)", d.getIndentationString(), variableName, value.String())
}

func (d *Decoder) logTagBuffer() {
	d.logger.Debugf("%s%s", d.getIndentationString(), "TAG_BUFFER")
}

// Primitive Types

func (d *Decoder) ReadInt16(variableName string) (value.Int16, error) {
	if d.UnreadBytesCount() < 2 {
		rem := d.UnreadBytesCount()
		errorMessage := fmt.Sprintf("Expected int16 length to be 2 bytes, got %d bytes", rem)
		return value.Int16{}, errors.NewPacketDecodingError(errorMessage).AddContexts("INT16", variableName)
	}

	decodedInteger := int16(binary.BigEndian.Uint16(d.bytes[d.offset:]))
	d.offset += 2

	value := value.Int16{Value: decodedInteger}
	d.logDecodedValue(variableName, value)

	return value, nil
}

func (d *Decoder) ReadInt32(variableName string) (value.Int32, error) {
	if d.UnreadBytesCount() < 4 {
		rem := d.UnreadBytesCount()
		errorMessage := fmt.Sprintf("Expected int32 length to be 4 bytes, got %d bytes", rem)
		err := errors.NewPacketDecodingError(errorMessage).AddContexts("INT32", variableName)
		return value.Int32{}, err
	}

	decodedInteger := int32(binary.BigEndian.Uint32(d.bytes[d.offset:]))
	d.offset += 4

	value := value.Int32{Value: decodedInteger}
	d.logDecodedValue(variableName, value)

	return value, nil
}

func (d *Decoder) ReadUnsignedVarint(variableName string) (value.UnsignedVarint, error) {
	decodedInteger, n := binary.Uvarint(d.bytes[d.offset:])

	if n == 0 {
		return value.UnsignedVarint{}, errors.NewPacketDecodingError("Unexpected end of data").AddContexts("UNSIGNED_VARINT", variableName)
	}

	if n < 0 {
		errorMessage := fmt.Sprintf("Unexpected unsigned varint overflow after decoding %d bytes", -n)
		return value.UnsignedVarint{}, errors.NewPacketDecodingError(errorMessage).AddContexts("UNSIGNED_VARINT", variableName)
	}

	d.offset += n

	value := value.UnsignedVarint{Value: decodedInteger}
	d.logDecodedValue(variableName, value)

	return value, nil
}

// Composite Types : Uses Basic Types for decoding

func (d *Decoder) ReadCompactArrayLength(variableName string) (value.CompactArrayLength, error) {
	decodedInteger, err := d.ReadUnsignedVarint("")

	if err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return value.CompactArrayLength{}, decodingErr.AddContexts("COMPACT_ARRAY_LENGTH", variableName)
		}
		return value.CompactArrayLength{}, err
	}

	value := value.CompactArrayLength(decodedInteger)
	d.logDecodedValue(variableName, value)

	return value, nil
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
	for range tagCount.Value {
		// ignore tag identifier
		_, err := d.ReadUnsignedVarint("")

		if err != nil {
			if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
				return decodingErr.AddContexts("TAG_BUFFER")
			}

			return err
		}

		// value length
		length, err := d.ReadUnsignedVarint("")

		if err != nil {
			if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
				return decodingErr.AddContexts("TAG_BUFFER")
			}

			return err
		}

		// value
		_, err = d.ConsumeRawBytes(int(length.Value), "")

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
	receivedBytesHexDump := inspectable_hex_dump.NewInspectableHexDump(receivedBytes)

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
