package decoder

import (
	"encoding/binary"
	"errors"
	"fmt"
	"strings"

	"github.com/codecrafters-io/kafka-tester/internal/assertions"
	"github.com/codecrafters-io/kafka-tester/offset_buffer"
	"github.com/codecrafters-io/kafka-tester/protocol/utils"
	kafkaValue "github.com/codecrafters-io/kafka-tester/protocol/value"
	"github.com/codecrafters-io/tester-utils/logger"
)

type Decodable interface {
	Decode(decoder *Decoder, variableName string) error
}

type DecoderCallbacks struct {
	OnDecode func(locator string, value kafkaValue.KafkaProtocolValue) error
}

type Decoder struct {
	buffer    *offset_buffer.Buffer
	locator   []string
	logger    *logger.Logger
	callbacks *DecoderCallbacks
}

func NewDecoder(rawBytes []byte, logger *logger.Logger) *Decoder {
	decoderLogger := logger.Clone()
	decoderLogger.UpdateLastSecondaryPrefix("Decoder")
	return &Decoder{
		buffer:    offset_buffer.NewBuffer(rawBytes),
		locator:   nil,
		logger:    decoderLogger,
		callbacks: nil,
	}
}

func NewInstrumentedDecoder(rawBytes []byte, logger *logger.Logger, assertion assertions.Assertion) *Decoder {
	decoder := NewDecoder(rawBytes, logger)
	decoder.AddCallbacks(DecoderCallbacks{
		OnDecode: func(locator string, value kafkaValue.KafkaProtocolValue) error {
			validaton := assertion.GetPrimitiveValidation(locator)

			// no validator
			if utils.CheckIfNilValidation(validaton) {
				decoder.LogDecodedValue(value, "-")
				return nil
			}

			err := validaton.Validate(value)

			if err != nil {
				decoder.LogIncorrectDecodedValue(value)
				return err
			}

			decoder.LogDecodedValue(value, "✔")
			return nil
		},
	})
	return decoder
}

func (d *Decoder) getLocator() string {
	return strings.Join(d.locator, ".")
}

func (d *Decoder) getLocatorWithVariableName(variableName string) string {
	return fmt.Sprintf("%s.%s", d.getLocator(), variableName)
}

func (d *Decoder) GetLogger() *logger.Logger {
	return d.logger
}

func (d *Decoder) AddCallbacks(callbacks DecoderCallbacks) {
	d.callbacks = &callbacks
}

func (d *Decoder) BeginSubSection(sectionName string) {
	d.logger.Debugf("%s%s", d.getIndentationString("-"), sectionName)
	d.locator = append(d.locator, sectionName)
}

func (d *Decoder) EndCurrentSubSection() {
	if len(d.locator) > 0 {
		d.locator = d.locator[:len(d.locator)-1]
	}
}

func (d *Decoder) RemainingBytesCount() uint64 {
	return d.buffer.RemainingBytesCount()
}

func (d *Decoder) LogDecodedValue(value kafkaValue.KafkaProtocolValue, bulletMarker string) {
	indentationString := d.getIndentationString(bulletMarker)

	if value.GetType() == kafkaValue.TAG_BUFFER {
		d.logger.Debugf("%s%s", indentationString, kafkaValue.TAG_BUFFER)
		return
	}

	variableName := value.GetVariableName()
	valueString := value.GetValueString()
	d.logger.Debugf("%s%s (%s)", indentationString, variableName, valueString)
}

func (d *Decoder) LogIncorrectDecodedValue(value kafkaValue.KafkaProtocolValue) {
	indentationString := d.getIndentationString("✘")
	variableName := value.GetVariableName()
	variableType := value.GetType()
	d.logger.Errorf("%s%s (%s)", indentationString, variableName, variableType)
}

func (d *Decoder) LogDecodingError(variableName string) {
	indentationString := d.getIndentationString("✘")
	d.logger.Errorf("%s%s", indentationString, variableName)
}

// Primitive Types

func (d *Decoder) ReadInt16(variableName string) (kafkaValue.Int16, error) {
	if d.RemainingBytesCount() < 2 {
		rem := d.RemainingBytesCount()
		d.LogDecodingError(variableName)
		return kafkaValue.Int16{}, fmt.Errorf("Expected INT16 length to be 2 bytes, got %d bytes", rem)
	}

	decodedInteger := kafkaValue.Int16{
		VariableName: variableName,
		Value:        int16(binary.BigEndian.Uint16(d.buffer.ReadRawBytes(2))),
	}

	return decodedInteger, d.callbacks.OnDecode(d.getLocatorWithVariableName(variableName), &decodedInteger)
}

func (d *Decoder) ReadInt32(variableName string) (kafkaValue.Int32, error) {
	if d.RemainingBytesCount() < 4 {
		rem := d.RemainingBytesCount()
		d.LogDecodingError(variableName)
		return kafkaValue.Int32{}, fmt.Errorf("Expected INT32 length to be 4 bytes, got %d bytes", rem)
	}

	decodedInteger := kafkaValue.Int32{
		VariableName: variableName,
		Value:        int32(binary.BigEndian.Uint32(d.buffer.ReadRawBytes(4))),
	}

	return decodedInteger, d.callbacks.OnDecode(d.getLocatorWithVariableName(variableName), &decodedInteger)
}

func (d *Decoder) readUnsignedVarintWithoutLogging() (kafkaValue.UnsignedVarint, error) {
	decodedUVarInt, numberOfBytesRead := d.buffer.ReadUnsignedVarint()

	// binary.Uvarint returns 0 bytes read if buffer is too small, negative if malformed
	if numberOfBytesRead == 0 {
		return kafkaValue.UnsignedVarint{}, errors.New("Insufficient bytes to decode UNSIGNED_VARINT")
	}

	if numberOfBytesRead < 0 {
		return kafkaValue.UnsignedVarint{}, errors.New("Malformed UNSIGNED_VARINT encoding")
	}

	return kafkaValue.UnsignedVarint{
		Value: decodedUVarInt,
	}, nil
}

func (d *Decoder) ReadCompactArrayLength(variableName string) (kafkaValue.CompactArrayLength, error) {
	unsignedVarInt, err := d.readUnsignedVarintWithoutLogging()

	if err != nil {
		return kafkaValue.CompactArrayLength{}, err
	}

	decodedValue := kafkaValue.CompactArrayLength{Value: unsignedVarInt.Value, VariableName: variableName}

	return decodedValue, d.callbacks.OnDecode(d.getLocatorWithVariableName(variableName), &decodedValue)
}

// Composite Types : Uses Basic Types for decoding

func ReadCompactArray[T Decodable](decoder *Decoder, variableName string) ([]T, error) {
	decoder.BeginSubSection(variableName)
	defer decoder.EndCurrentSubSection()

	compactArrayLength, err := decoder.ReadCompactArrayLength(fmt.Sprintf("%s.Length", variableName))

	if err != nil {
		return nil, err
	}

	actualArrayLength := compactArrayLength.ActualLength()

	// Create array with the specified length
	result := make([]T, actualArrayLength)

	// Decode each element
	for i := uint64(0); i < uint64(actualArrayLength); i++ {
		elementVariableName := fmt.Sprintf("%s[%d]", variableName, i)

		// Create a new instance of T
		var element T

		// Decode the element
		err := element.Decode(decoder, elementVariableName)
		if err != nil {
			return nil, err
		}

		result[i] = element
	}

	return result, nil
}

func (d *Decoder) ConsumeTagBuffer() error {
	d.BeginSubSection("TAG_BUFFER")
	defer d.EndCurrentSubSection()

	tagCount, err := d.readUnsignedVarintWithoutLogging()

	if err != nil {
		return err
	}

	// skip over any tagged fields without deserializing them
	// as we don't currently support doing anything with them
	for range tagCount.Value {
		// ignore tag identifier
		_, err := d.readUnsignedVarintWithoutLogging()

		if err != nil {
			return err
		}

		// value length
		length, err := d.readUnsignedVarintWithoutLogging()

		if err != nil {
			return err
		} else if int(length.Value) < 0 {
			return errors.New("Expected length of value in tag buffer to be positive")
		}

		// value
		_, err = d.ConsumeRawBytes(length.Value)

		if err != nil {
			return err
		}
	}
	return nil
}

func (d *Decoder) ConsumeRawBytes(length uint64) ([]byte, error) {
	if length > uint64(d.RemainingBytesCount()) {
		return nil, fmt.Errorf("Expected length to be lesser than remaining bytes (%d), got %d", d.RemainingBytesCount(), length)
	}

	result := d.buffer.ReadRawBytes(length)
	return result, nil
}

func (d *Decoder) getIndentationString(bulletMarker string) string {
	indentationLevel := len(d.locator)
	indentationSpaces := strings.Repeat("  ", indentationLevel)
	bullet := fmt.Sprintf("%s ", bulletMarker)

	// Only need dot for indented level
	if indentationLevel != 0 {
		bullet = fmt.Sprintf("%s .", bulletMarker)
	}

	return indentationSpaces + bullet
}
