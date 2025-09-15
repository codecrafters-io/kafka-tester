package decoder

import (
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/codecrafters-io/kafka-tester/offset_buffer"
	kafkaValue "github.com/codecrafters-io/kafka-tester/protocol/value"
	"github.com/google/uuid"
)

type Decoder struct {
	buffer *offset_buffer.Buffer
}

func NewDecoder(rawBytes []byte) *Decoder {
	return &Decoder{
		buffer: offset_buffer.NewBuffer(rawBytes),
	}
}

func (d *Decoder) RemainingBytesCount() uint64 {
	return d.buffer.RemainingBytesCount()
}

// Read functions

func (d *Decoder) ReadBoolean() (kafkaValue.Boolean, DecoderError) {
	if d.RemainingBytesCount() < 1 {
		rem := d.RemainingBytesCount()
		return kafkaValue.Boolean{}, d.wrapError(fmt.Errorf("Expected BOOLEAN length to be 1 byte, got %d bytes", rem))
	}

	boolValue := false
	readByte := d.buffer.MustReadNBytes(1)[0]

	// Kafka considers non-zero value as true
	if readByte != 0 {
		boolValue = true
	}

	return kafkaValue.Boolean{
		Value: boolValue,
	}, nil
}

func (d *Decoder) ReadInt8() (kafkaValue.Int8, DecoderError) {
	if d.RemainingBytesCount() < 2 {
		rem := d.RemainingBytesCount()
		return kafkaValue.Int8{}, d.wrapError(fmt.Errorf("Expected INT8 length to be 2 bytes, got %d bytes", rem))
	}

	readByte := d.buffer.MustReadNBytes(1)[0]
	decodedInteger := kafkaValue.Int8{
		Value: int8(readByte),
	}

	return decodedInteger, nil
}

func (d *Decoder) ReadInt16() (kafkaValue.Int16, DecoderError) {
	if d.RemainingBytesCount() < 2 {
		rem := d.RemainingBytesCount()
		return kafkaValue.Int16{}, d.wrapError(fmt.Errorf("Expected INT16 length to be 2 bytes, got %d bytes", rem))
	}

	decodedInteger := kafkaValue.Int16{
		Value: int16(binary.BigEndian.Uint16(d.buffer.MustReadNBytes(2))),
	}

	return decodedInteger, nil
}

func (d *Decoder) ReadInt32() (kafkaValue.Int32, DecoderError) {
	if d.RemainingBytesCount() < 4 {
		rem := d.RemainingBytesCount()
		return kafkaValue.Int32{}, d.wrapError(fmt.Errorf("Expected INT32 length to be 4 bytes, got %d bytes", rem))
	}

	decodedInteger := kafkaValue.Int32{
		Value: int32(binary.BigEndian.Uint32(d.buffer.MustReadNBytes(4))),
	}

	return decodedInteger, nil
}

func (d *Decoder) ReadUnsignedVarint() (kafkaValue.UnsignedVarint, DecoderError) {
	decodedInteger, numberOfBytesRead := binary.Uvarint(d.buffer.RemainingBytes())

	// Moves the offset
	d.buffer.MustReadNBytes(uint64(numberOfBytesRead))

	// binary.Uvarint returns 0 bytes read if buffer is too small
	if numberOfBytesRead == 0 {
		return kafkaValue.UnsignedVarint{}, d.wrapError(errors.New("Insufficient bytes to decode UNSIGNED_VARINT"))
	}

	// binary.Uvarint returns negative if malformed
	if numberOfBytesRead < 0 {
		return kafkaValue.UnsignedVarint{}, d.wrapError(errors.New("Malformed UNSIGNED_VARINT encoding"))
	}

	return kafkaValue.UnsignedVarint{
		Value: decodedInteger,
	}, nil
}

func (d *Decoder) ReadCompactArrayLength() (kafkaValue.CompactArrayLength, DecoderError) {
	unsignedVarInt, err := d.ReadUnsignedVarint()

	if err != nil {
		return kafkaValue.CompactArrayLength{}, err
	}

	return kafkaValue.CompactArrayLength(unsignedVarInt), nil
}

func (d *Decoder) ReadCompactStringLength() (kafkaValue.CompactStringLength, DecoderError) {
	unsignedVarInt, err := d.ReadUnsignedVarint()

	if err != nil {
		return kafkaValue.CompactStringLength{}, err
	}

	return kafkaValue.CompactStringLength(unsignedVarInt), nil
}

func (d *Decoder) ReadCompactString() (kafkaValue.CompactString, DecoderError) {
	lengthValue, err := d.ReadCompactStringLength()
	if err != nil {
		return kafkaValue.CompactString{}, d.wrapError(err)
	}

	if lengthValue.Value == 0 {
		return kafkaValue.CompactString{}, d.wrapError(fmt.Errorf("Compact string length cannot be 0"))
	}

	if d.RemainingBytesCount() < lengthValue.ActualLength() {
		return kafkaValue.CompactString{}, d.wrapError(fmt.Errorf("Expected remaining bytes count for COMPACT_STRING to be %d, got %d", lengthValue.ActualLength(), d.RemainingBytesCount()))
	}

	rawBytes := d.buffer.MustReadNBytes(lengthValue.ActualLength())

	return kafkaValue.CompactString{
		Value: string(rawBytes),
	}, nil
}

func (d *Decoder) ReadCompactNullableString() (kafkaValue.CompactNullableString, DecoderError) {
	lengthValue, err := d.ReadCompactStringLength()

	if err != nil {
		return kafkaValue.CompactNullableString{}, d.wrapError(err)
	}

	if lengthValue.Value == 0 {
		return kafkaValue.CompactNullableString{}, nil
	}

	if d.RemainingBytesCount() < lengthValue.ActualLength() {
		return kafkaValue.CompactNullableString{}, d.wrapError(fmt.Errorf("Expected remaining bytes count for COMPACT_NULLABLE_STRING to be %d, got %d", lengthValue.ActualLength(), d.RemainingBytesCount()))
	}

	rawBytes := d.buffer.MustReadNBytes(lengthValue.ActualLength())
	stringValue := string(rawBytes)

	return kafkaValue.CompactNullableString{
		Value: &stringValue,
	}, nil
}

func (d *Decoder) ReadUUID() (kafkaValue.UUID, DecoderError) {
	uuidBytesCount := 16
	if d.RemainingBytesCount() < uint64(uuidBytesCount) {
		return kafkaValue.UUID{}, d.wrapError(fmt.Errorf("Expected remaining bytes count to be %d, got %d", uuidBytesCount, d.RemainingBytesCount()))
	}

	uuidBytes := d.buffer.MustReadNBytes(uint64(uuidBytesCount))

	uuid, err := uuid.FromBytes(uuidBytes)
	if err != nil {
		return kafkaValue.UUID{}, d.wrapError(fmt.Errorf("Failed to decode UUID: %s", err))
	}

	return kafkaValue.UUID{
		Value: uuid.String(),
	}, nil
}

func (d *Decoder) ConsumeTagBuffer() DecoderError {
	tagCount, err := d.ReadUnsignedVarint()
	if err != nil {
		return err
	}

	// skip over any tagged fields without deserializing them
	// as we don't currently support doing anything with them
	for range tagCount.Value {
		// ignore tag identifier
		_, err := d.ReadUnsignedVarint()

		if err != nil {
			return err
		}

		// value length
		length, err := d.ReadUnsignedVarint()

		if err != nil {
			return err
		} else if int(length.Value) < 0 {
			return d.wrapError(errors.New("Expected length of value in tag buffer to be non-negative"))
		}

		// value
		if err := d.consumeRawBytes(length.Value); err != nil {
			return err
		}
	}
	return nil
}

func (d *Decoder) consumeRawBytes(length uint64) DecoderError {
	if length > uint64(d.RemainingBytesCount()) {
		return d.wrapError(fmt.Errorf("Expected length to be lesser than remaining bytes (%d), got %d", d.RemainingBytesCount(), length))
	}

	// Moves the offset
	d.buffer.MustReadNBytes(length)

	return nil
}

func (d *Decoder) wrapError(err error) DecoderError {
	// If we've already wrapped the error, preserve the nested path
	if decoderError, ok := err.(*decoderErrorImpl); ok {
		return decoderError
	}

	return &decoderErrorImpl{
		message: err.Error(),
		offset:  int(d.buffer.Offset()),
	}
}
