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

func (d *Decoder) ReadBytesCount() uint64 {
	return d.buffer.Offset()
}

func (d *Decoder) RemainingBytesCount() uint64 {
	return d.buffer.RemainingBytesCount()
}

// Read functions

func (d *Decoder) ReadBoolean() (kafkaValue.Boolean, DecoderError) {
	if d.RemainingBytesCount() < 1 {
		rem := d.RemainingBytesCount()
		return kafkaValue.Boolean{}, d.WrapError(fmt.Errorf("Expected BOOLEAN length to be 1 byte, got %d bytes", rem))
	}

	readByte := d.buffer.MustReadNBytes(1)[0]

	return kafkaValue.Boolean{
		Value: readByte != 0,
	}, nil
}

func (d *Decoder) ReadInt8() (kafkaValue.Int8, DecoderError) {
	if d.RemainingBytesCount() < 2 {
		rem := d.RemainingBytesCount()
		return kafkaValue.Int8{}, d.WrapError(fmt.Errorf("Expected INT8 length to be 2 bytes, got %d bytes", rem))
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
		return kafkaValue.Int16{}, d.WrapError(fmt.Errorf("Expected INT16 length to be 2 bytes, got %d bytes", rem))
	}

	decodedInteger := kafkaValue.Int16{
		Value: int16(binary.BigEndian.Uint16(d.buffer.MustReadNBytes(2))),
	}

	return decodedInteger, nil
}

func (d *Decoder) ReadInt32() (kafkaValue.Int32, DecoderError) {
	if d.RemainingBytesCount() < 4 {
		rem := d.RemainingBytesCount()
		return kafkaValue.Int32{}, d.WrapError(fmt.Errorf("Expected INT32 length to be 4 bytes, got %d bytes", rem))
	}

	decodedInteger := kafkaValue.Int32{
		Value: int32(binary.BigEndian.Uint32(d.buffer.MustReadNBytes(4))),
	}

	return decodedInteger, nil
}

func (d *Decoder) ReadInt64() (kafkaValue.Int64, DecoderError) {
	if d.RemainingBytesCount() < 8 {
		rem := d.RemainingBytesCount()
		return kafkaValue.Int64{}, d.WrapError(fmt.Errorf("Expected INT64 length to be 8 bytes, got %d bytes", rem))
	}

	decodedInteger := kafkaValue.Int64{
		Value: int64(binary.BigEndian.Uint64(d.buffer.MustReadNBytes(8))),
	}

	return decodedInteger, nil
}

func (d *Decoder) ReadUnsignedVarint() (kafkaValue.UnsignedVarint, DecoderError) {
	decodedInteger, numberOfBytesRead := binary.Uvarint(d.buffer.RemainingBytes())

	// Moves the offset
	d.buffer.MustReadNBytes(uint64(numberOfBytesRead))

	// binary.Uvarint returns 0 bytes read if buffer is too small
	if numberOfBytesRead == 0 {
		return kafkaValue.UnsignedVarint{}, d.WrapError(errors.New("Insufficient bytes to decode UNSIGNED_VARINT"))
	}

	// binary.Uvarint returns negative if malformed
	if numberOfBytesRead < 0 {
		return kafkaValue.UnsignedVarint{}, d.WrapError(errors.New("Malformed UNSIGNED_VARINT encoding"))
	}

	return kafkaValue.UnsignedVarint{
		Value: decodedInteger,
	}, nil
}

func (d *Decoder) ReadVarint() (kafkaValue.Varint, DecoderError) {
	decodedInteger, numberOfBytesRead := binary.Varint(d.buffer.RemainingBytes())

	// Move the offset
	d.buffer.MustReadNBytes(uint64(numberOfBytesRead))

	// binary.Varint returns 0 bytes read if buffer is too small
	if numberOfBytesRead == 0 {
		return kafkaValue.Varint{}, d.WrapError(errors.New("Insufficient bytes to decode VARINT"))
	}

	// binary.Varint returns negative if malformed
	if numberOfBytesRead < 0 {
		return kafkaValue.Varint{}, d.WrapError(errors.New("Malformed VARINT encoding"))
	}

	return kafkaValue.Varint{
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
		return kafkaValue.CompactString{}, d.WrapError(err)
	}

	if lengthValue.Value == 0 {
		return kafkaValue.CompactString{}, d.WrapError(fmt.Errorf("Compact string length cannot be 0"))
	}

	if d.RemainingBytesCount() < lengthValue.ActualLength() {
		return kafkaValue.CompactString{}, d.WrapError(fmt.Errorf("Expected COMPACT_STRING contents to be %d bytes long, only got %d", lengthValue.ActualLength(), d.RemainingBytesCount()))
	}

	rawBytes := d.buffer.MustReadNBytes(lengthValue.ActualLength())

	return kafkaValue.CompactString{
		Value: string(rawBytes),
	}, nil
}

func (d *Decoder) ReadCompactRecordSize() (kafkaValue.CompactRecordSize, DecoderError) {
	unsignedVarInt, err := d.ReadUnsignedVarint()

	if err != nil {
		return kafkaValue.CompactRecordSize{}, err
	}

	return kafkaValue.CompactRecordSize(unsignedVarInt), nil
}

// ReadRawBytes is re-added because of a condition encountered in fetch api extension
// although there's no data type as raw bytes
// we still need it for data structure like 'Record', (ref. https://kafka.apache.org/documentation/#record)
// Here, the 'key' and 'value' fields are array of bytes,
// Furthermore, they need special format for printing (so decoder logs is easy to read (see String() for RawBytes))
// So, need help implementing a better solution if exists.
func (d *Decoder) ReadRawBytes(count int) (kafkaValue.RawBytes, DecoderError) {
	if d.RemainingBytesCount() < uint64(count) {
		return kafkaValue.RawBytes{}, d.WrapError(fmt.Errorf("Expected remaining bytes count for reading raw bytes to be %d, got %d", count, d.RemainingBytesCount()))
	}

	readBytes := d.buffer.MustReadNBytes(uint64(count))

	return kafkaValue.RawBytes{
		Value: readBytes,
	}, nil
}

func (d *Decoder) ReadCompactNullableString() (kafkaValue.CompactNullableString, DecoderError) {
	lengthValue, err := d.ReadCompactStringLength()

	if err != nil {
		return kafkaValue.CompactNullableString{}, d.WrapError(err)
	}

	if lengthValue.Value == 0 {
		return kafkaValue.CompactNullableString{}, nil
	}

	if d.RemainingBytesCount() < lengthValue.ActualLength() {
		return kafkaValue.CompactNullableString{}, d.WrapError(fmt.Errorf("Expected COMPACT_NULLABLE_STRING contents to be %d bytes long, got only %d", lengthValue.ActualLength(), d.RemainingBytesCount()))
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
		return kafkaValue.UUID{}, d.WrapError(fmt.Errorf("Expected UUID contents to be %d bytes long, got only %d", uuidBytesCount, d.RemainingBytesCount()))
	}

	uuidBytes := d.buffer.MustReadNBytes(uint64(uuidBytesCount))

	uuid, err := uuid.FromBytes(uuidBytes)
	if err != nil {
		return kafkaValue.UUID{}, d.WrapError(fmt.Errorf("Failed to decode UUID: %s", err))
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
			return d.WrapError(errors.New("Expected length of value in tag buffer to be non-negative"))
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
		return d.WrapError(fmt.Errorf("Expected length to be lesser than remaining bytes (%d), got %d", d.RemainingBytesCount(), length))
	}

	// Moves the offset
	d.buffer.MustReadNBytes(length)

	return nil
}

func (d *Decoder) WrapError(err error) DecoderError {
	// If we've already wrapped the error, preserve the nested path
	if decoderError, ok := err.(*decoderErrorImpl); ok {
		return decoderError
	}

	return &decoderErrorImpl{
		message: err.Error(),
		offset:  int(d.buffer.Offset()),
	}
}
