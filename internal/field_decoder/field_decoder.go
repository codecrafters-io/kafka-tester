package field_decoder

import (
	"fmt"
	"strings"

	"github.com/codecrafters-io/kafka-tester/internal/field_path"
	"github.com/codecrafters-io/kafka-tester/protocol/decoder"
	"github.com/codecrafters-io/kafka-tester/protocol/utils"
	"github.com/codecrafters-io/kafka-tester/protocol/value"
)

type DecodedField struct {
	Path  field_path.FieldPath
	Value value.KafkaProtocolValue
}

type FieldDecoder struct {
	currentPathContexts []string
	decoder             *decoder.Decoder
	decodedFields       []DecodedField
}

func NewFieldDecoder(bytes []byte) *FieldDecoder {
	return &FieldDecoder{
		currentPathContexts: []string{},
		decodedFields:       []DecodedField{},
		decoder:             decoder.NewDecoder(bytes),
	}
}

func (d *FieldDecoder) DecodedFields() []DecodedField {
	return d.decodedFields
}

func (d *FieldDecoder) PushPathContext(pathContext string) {
	d.currentPathContexts = append(d.currentPathContexts, pathContext)
}

func (d *FieldDecoder) PopPathContext() {
	d.currentPathContexts = d.currentPathContexts[:len(d.currentPathContexts)-1]
}

func (d *FieldDecoder) ReadCompactArrayLengthField(path string) (value.CompactArrayLength, FieldDecoderError) {
	d.PushPathContext(path)
	defer d.PopPathContext()

	decodedValue, err := d.decoder.ReadCompactArrayLength()
	if err != nil {
		return value.CompactArrayLength{}, d.wrapError(err)
	}

	d.appendDecodedField(decodedValue)

	return decodedValue, nil
}

func (d *FieldDecoder) ReadCompactNullableStringField(path string) (value.CompactNullableString, FieldDecoderError) {
	d.PushPathContext(path)
	defer d.PopPathContext()

	lengthValue, err := d.decoder.ReadCompactArrayLength()
	if err != nil {
		return value.CompactNullableString{}, d.wrapError(err)
	}

	rawBytes, err := d.decoder.ReadRawBytes(int(lengthValue.ActualLength()))

	if err != nil {
		return value.CompactNullableString{}, d.wrapError(err)
	}

	stringValue := string(rawBytes.Value)

	decodedValue := value.CompactNullableString{
		Value: &stringValue,
	}

	d.appendDecodedField(decodedValue)

	return decodedValue, nil
}

func (d *FieldDecoder) ReadCompactStringField(path string) (value.CompactString, FieldDecoderError) {
	d.PushPathContext(path)
	defer d.PopPathContext()

	lengthValue, err := d.decoder.ReadCompactArrayLength()

	if err != nil {
		return value.CompactString{}, d.wrapError(err)
	}

	if lengthValue.Value == 0 {
		return value.CompactString{}, d.wrapError(fmt.Errorf("Compact string length cannot be 0"))
	}

	rawBytes, err := d.decoder.ReadRawBytes(int(lengthValue.ActualLength()))

	if err != nil {
		return value.CompactString{}, d.wrapError(err)
	}

	stringValue := string(rawBytes.Value)

	decodedValue := value.CompactString{
		Value: stringValue,
	}

	d.appendDecodedField(decodedValue)

	return decodedValue, nil
}

func (d *FieldDecoder) ReadUUIDField(path string) (value.UUID, FieldDecoderError) {
	d.PushPathContext(path)
	defer d.PopPathContext()

	rawBytes, err := d.decoder.ReadRawBytes(16)

	if err != nil {
		return value.UUID{}, d.wrapError(err)
	}

	uuidString, decodeErr := utils.DecodeUUID(rawBytes.Value)
	if decodeErr != nil {
		return value.UUID{}, d.wrapError(err)
	}

	decodedUUID := value.UUID{
		Value: uuidString,
	}

	d.appendDecodedField(decodedUUID)
	return decodedUUID, nil
}

func (d *FieldDecoder) ReadBooleanField(path string) (value.Boolean, FieldDecoderError) {
	d.PushPathContext(path)
	defer d.PopPathContext()

	decodedValue, err := d.decoder.ReadBoolean()
	if err != nil {
		return value.Boolean{}, d.wrapError(err)
	}

	d.appendDecodedField(decodedValue)
	return decodedValue, nil
}

func (d *FieldDecoder) ReadInt16Field(path string) (value.Int16, FieldDecoderError) {
	d.PushPathContext(path)
	defer d.PopPathContext()

	decodedValue, err := d.decoder.ReadInt16()
	if err != nil {
		return value.Int16{}, d.wrapError(err)
	}

	d.appendDecodedField(decodedValue)

	return decodedValue, nil
}

func (d *FieldDecoder) ReadInt8Field(path string) (value.Int8, FieldDecoderError) {
	d.PushPathContext(path)
	defer d.PopPathContext()

	decodedValue, err := d.decoder.ReadInt8()
	if err != nil {
		return value.Int8{}, d.wrapError(err)
	}

	d.appendDecodedField(decodedValue)

	return decodedValue, nil
}

func (d *FieldDecoder) ReadInt32Field(path string) (value.Int32, FieldDecoderError) {
	d.PushPathContext(path)
	defer d.PopPathContext()

	decodedValue, err := d.decoder.ReadInt32()
	if err != nil {
		return value.Int32{}, d.wrapError(err)
	}

	d.appendDecodedField(decodedValue)

	return decodedValue, nil
}

func (d *FieldDecoder) ConsumeTagBufferField() FieldDecoderError {
	d.PushPathContext("TAG_BUFFER")
	defer d.PopPathContext()

	if err := d.decoder.ConsumeTagBuffer(); err != nil {
		return d.wrapError(err)
	}

	return nil
}

func (d *FieldDecoder) RemainingBytesCount() uint64 {
	return d.decoder.RemainingBytesCount()
}

func (d *FieldDecoder) currentPath() field_path.FieldPath {
	return field_path.NewFieldPath(strings.Join(d.currentPathContexts, "."))
}

func (d *FieldDecoder) appendDecodedField(decodedValue value.KafkaProtocolValue) {
	d.decodedFields = append(d.decodedFields, DecodedField{
		Value: decodedValue,
		Path:  d.currentPath(),
	})
}

func (d *FieldDecoder) wrapError(err error) FieldDecoderError {
	// If we've already wrapped the error, preserve the nested path
	if fieldDecoderError, ok := err.(*fieldDecoderErrorImpl); ok {
		return fieldDecoderError
	}

	if decoderError, ok := err.(decoder.DecoderError); ok {
		return &fieldDecoderErrorImpl{
			message: err.Error(),
			offset:  decoderError.Offset(),
			path:    d.currentPath(),
		}
	} else {
		panic("CodeCrafters Internal Error: FieldDecoderError is not a decoder error")
	}
}
