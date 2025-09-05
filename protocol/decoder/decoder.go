package decoder

import (
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/codecrafters-io/kafka-tester/offset_buffer"
	kafkaValue "github.com/codecrafters-io/kafka-tester/protocol/value"
)

type Decodable interface {
	Decode(decoder *Decoder, variableName string) error
}

// TODO[PaulRefactor]: remove all traces of callbacks here
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

// Primitive Types

func (d *Decoder) ReadInt16() (kafkaValue.Int16, error) {
	if d.RemainingBytesCount() < 2 {
		rem := d.RemainingBytesCount()
		return kafkaValue.Int16{}, fmt.Errorf("Expected INT16 length to be 2 bytes, got %d bytes", rem)
	}

	decodedInteger := kafkaValue.Int16{
		Value: int16(binary.BigEndian.Uint16(d.buffer.ReadRawBytes(2))),
	}

	return decodedInteger, nil
}

func (d *Decoder) ReadInt32() (kafkaValue.Int32, error) {
	if d.RemainingBytesCount() < 4 {
		rem := d.RemainingBytesCount()
		return kafkaValue.Int32{}, fmt.Errorf("Expected INT32 length to be 4 bytes, got %d bytes", rem)
	}

	decodedInteger := kafkaValue.Int32{
		Value: int32(binary.BigEndian.Uint32(d.buffer.ReadRawBytes(4))),
	}

	return decodedInteger, nil
}

func (d *Decoder) ReadUnsignedVarint() (kafkaValue.UnsignedVarint, error) {
	decodedInteger, numberOfBytesRead := binary.Uvarint(d.buffer.RemainingBytes())
	// Moves the offset
	d.buffer.ReadRawBytes(uint64(numberOfBytesRead))

	// binary.Uvarint returns 0 bytes read if buffer is too small, negative if malformed
	if numberOfBytesRead == 0 {
		return kafkaValue.UnsignedVarint{}, errors.New("Insufficient bytes to decode UNSIGNED_VARINT")
	}

	if numberOfBytesRead < 0 {
		return kafkaValue.UnsignedVarint{}, errors.New("Malformed UNSIGNED_VARINT encoding")
	}

	return kafkaValue.UnsignedVarint{
		Value: decodedInteger,
	}, nil
}

func (d *Decoder) ReadCompactArrayLength() (kafkaValue.CompactArrayLength, error) {
	unsignedVarInt, err := d.ReadUnsignedVarint()

	if err != nil {
		return kafkaValue.CompactArrayLength{}, err
	}

	return kafkaValue.CompactArrayLength(unsignedVarInt), nil
}

func (d *Decoder) ConsumeTagBuffer() error {
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
