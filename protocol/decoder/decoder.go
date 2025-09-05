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

type DecoderCallbacks struct {
	OnDecode                 func(variableName string, value kafkaValue.KafkaProtocolValue) error
	OnDecodeError            func(variableName string)
	OnCompositeDecodingStart func(compositeVariableName string)
	OnCompositeDecodingEnd   func()
}

type Decoder struct {
	buffer    *offset_buffer.Buffer
	callbacks *DecoderCallbacks
}

func NewDecoder(rawBytes []byte) *Decoder {
	return &Decoder{
		buffer: offset_buffer.NewBuffer(rawBytes),
	}
}

func (d *Decoder) SetCallbacks(callbacks *DecoderCallbacks) *Decoder {
	d.callbacks = callbacks
	return d
}

func (d *Decoder) GetCallBacks() *DecoderCallbacks {
	return d.callbacks
}

func (d *Decoder) RemainingBytesCount() uint64 {
	return d.buffer.RemainingBytesCount()
}

// Primitive Types

func (d *Decoder) ReadInt16(variableName string) (kafkaValue.Int16, error) {
	if d.RemainingBytesCount() < 2 {
		rem := d.RemainingBytesCount()
		d.callbacks.OnDecodeError(variableName)
		return kafkaValue.Int16{}, fmt.Errorf("Expected INT16 length to be 2 bytes, got %d bytes", rem)
	}

	decodedInteger := kafkaValue.Int16{
		Value: int16(binary.BigEndian.Uint16(d.buffer.ReadRawBytes(2))),
	}

	return decodedInteger, d.callbacks.OnDecode(variableName, &decodedInteger)
}

func (d *Decoder) ReadInt32(variableName string) (kafkaValue.Int32, error) {
	if d.RemainingBytesCount() < 4 {
		rem := d.RemainingBytesCount()
		d.callbacks.OnDecodeError(variableName)
		return kafkaValue.Int32{}, fmt.Errorf("Expected INT32 length to be 4 bytes, got %d bytes", rem)
	}

	decodedInteger := kafkaValue.Int32{
		Value: int32(binary.BigEndian.Uint32(d.buffer.ReadRawBytes(4))),
	}

	return decodedInteger, d.callbacks.OnDecode(variableName, &decodedInteger)
}

func (d *Decoder) consumeUnsignedVarint() (uint64, int) {
	decodedInteger, numberOfBytesRead := binary.Uvarint(d.buffer.RemainingBytes())
	// Moves the offset
	d.buffer.ReadRawBytes(uint64(numberOfBytesRead))
	return decodedInteger, numberOfBytesRead
}

func (d *Decoder) readUnsignedVarintWithoutCallback() (kafkaValue.UnsignedVarint, error) {
	decodedUVarInt, numberOfBytesRead := d.consumeUnsignedVarint()

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
	unsignedVarInt, err := d.readUnsignedVarintWithoutCallback()

	if err != nil {
		return kafkaValue.CompactArrayLength{}, err
	}

	decodedValue := kafkaValue.CompactArrayLength(unsignedVarInt)

	return decodedValue, d.callbacks.OnDecode(variableName, &decodedValue)
}

// Composite Types : Uses Basic Types for decoding

// ReadCompactArray reads a compact array of type T
// The compact array length is read as well
func ReadCompactArray[T any, PT interface {
	*T
	Decodable
}](decoder *Decoder, variableName string) ([]T, error) {
	decoder.callbacks.OnCompositeDecodingStart(variableName)
	defer decoder.callbacks.OnCompositeDecodingEnd()

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
		element := new(T)

		// Cast to pointer type that implements Decodable
		ptr := PT(element)
		err := ptr.Decode(decoder, elementVariableName)
		if err != nil {
			return nil, err
		}

		result[i] = *element
	}

	return result, nil
}

func (d *Decoder) ConsumeTagBuffer() error {
	d.callbacks.OnCompositeDecodingStart("TAG_BUFFER")
	defer d.callbacks.OnCompositeDecodingEnd()
	tagCount, err := d.readUnsignedVarintWithoutCallback()

	if err != nil {
		return err
	}

	// skip over any tagged fields without deserializing them
	// as we don't currently support doing anything with them
	for range tagCount.Value {
		// ignore tag identifier
		_, err := d.readUnsignedVarintWithoutCallback()

		if err != nil {
			return err
		}

		// value length
		length, err := d.readUnsignedVarintWithoutCallback()

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
