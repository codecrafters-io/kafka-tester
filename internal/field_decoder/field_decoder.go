package field_decoder

import (
	"fmt"
	"strings"

	"github.com/codecrafters-io/kafka-tester/internal/field"
	"github.com/codecrafters-io/kafka-tester/internal/field_path"
	"github.com/codecrafters-io/kafka-tester/protocol/decoder"
	"github.com/codecrafters-io/kafka-tester/protocol/value"
)

type pathContext struct {
	pathName string
	offset   uint64 // the offset at which the pathName was added to the context
}

type FieldDecoder struct {
	currentPathContexts []pathContext
	decoder             *decoder.Decoder
	decodedFields       []field.Field
}

func NewFieldDecoder(bytes []byte) *FieldDecoder {
	return &FieldDecoder{
		currentPathContexts: []pathContext{},
		decodedFields:       []field.Field{},
		decoder:             decoder.NewDecoder(bytes),
	}
}

func (d *FieldDecoder) ReadBytesCount() uint64 {
	return d.decoder.ReadBytesCount()
}

func (d *FieldDecoder) DecodedFields() []field.Field {
	return d.decodedFields
}

func (d *FieldDecoder) PushPathContext(pathName string) {
	d.currentPathContexts = append(d.currentPathContexts, pathContext{
		pathName: pathName,
		offset:   d.ReadBytesCount(), // capture current offset for the context
	})
}

func (d *FieldDecoder) PopPathContext() {
	d.currentPathContexts = d.currentPathContexts[:len(d.currentPathContexts)-1]
}

func (d *FieldDecoder) ReadCompactArrayLengthField(path string) (value.CompactArrayLength, FieldDecoderError) {
	d.PushPathContext(path)
	defer d.PopPathContext()

	decodedValue, err := d.decoder.ReadCompactArrayLength()
	if err != nil {
		return value.CompactArrayLength{}, d.WrapError(err)
	}

	d.appendDecodedField(decodedValue)

	return decodedValue, nil
}

func (d *FieldDecoder) ReadCompactRecordSizeField(path string) (value.CompactRecordSize, FieldDecoderError) {
	d.PushPathContext(path)
	defer d.PopPathContext()

	decodedValue, err := d.decoder.ReadCompactRecordSize()
	if err != nil {
		return value.CompactRecordSize{}, d.WrapError(err)
	}

	d.appendDecodedField(decodedValue)

	return decodedValue, nil
}

func (d *FieldDecoder) ReadUnsignedVarInt(path string) (value.UnsignedVarint, FieldDecoderError) {
	d.PushPathContext(path)
	defer d.PopPathContext()

	decodedValue, err := d.decoder.ReadUnsignedVarint()

	if err != nil {
		return value.UnsignedVarint{}, d.WrapError(err)
	}

	d.appendDecodedField(decodedValue)

	return decodedValue, nil
}

func (d *FieldDecoder) ReadVarint(path string) (value.Varint, FieldDecoderError) {
	d.PushPathContext(path)
	defer d.PopPathContext()

	decodedValue, err := d.decoder.ReadVarint()

	if err != nil {
		return value.Varint{}, d.WrapError(err)
	}

	d.appendDecodedField(decodedValue)

	return decodedValue, nil
}

func (d *FieldDecoder) ReadCompactNullableStringField(path string) (value.CompactNullableString, FieldDecoderError) {
	d.PushPathContext(path)
	defer d.PopPathContext()

	lengthValue, err := d.decoder.ReadCompactArrayLength()
	if err != nil {
		return value.CompactNullableString{}, d.WrapError(err)
	}

	rawBytes, err := d.decoder.ReadRawBytes(int(lengthValue.ActualLength()))

	if err != nil {
		return value.CompactNullableString{}, d.WrapError(err)
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
		return value.CompactString{}, d.WrapError(err)
	}

	if lengthValue.Value == 0 {
		return value.CompactString{}, d.WrapError(fmt.Errorf("Compact string length cannot be 0"))
	}

	rawBytes, err := d.decoder.ReadRawBytes(int(lengthValue.ActualLength()))

	if err != nil {
		return value.CompactString{}, d.WrapError(err)
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

	decodedValue, err := d.decoder.ReadUUID()
	if err != nil {
		return value.UUID{}, d.WrapError(err)
	}

	d.appendDecodedField(decodedValue)
	return decodedValue, nil
}

func (d *FieldDecoder) ReadBooleanField(path string) (value.Boolean, FieldDecoderError) {
	d.PushPathContext(path)
	defer d.PopPathContext()

	decodedValue, err := d.decoder.ReadBoolean()
	if err != nil {
		return value.Boolean{}, d.WrapError(err)
	}

	d.appendDecodedField(decodedValue)
	return decodedValue, nil
}

func (d *FieldDecoder) ReadInt16Field(path string) (value.Int16, FieldDecoderError) {
	d.PushPathContext(path)
	defer d.PopPathContext()

	decodedValue, err := d.decoder.ReadInt16()
	if err != nil {
		return value.Int16{}, d.WrapError(err)
	}

	d.appendDecodedField(decodedValue)

	return decodedValue, nil
}

func (d *FieldDecoder) ReadInt8Field(path string) (value.Int8, FieldDecoderError) {
	d.PushPathContext(path)
	defer d.PopPathContext()

	decodedValue, err := d.decoder.ReadInt8()
	if err != nil {
		return value.Int8{}, d.WrapError(err)
	}

	d.appendDecodedField(decodedValue)

	return decodedValue, nil
}

func (d *FieldDecoder) ReadInt32Field(path string) (value.Int32, FieldDecoderError) {
	d.PushPathContext(path)
	defer d.PopPathContext()

	decodedValue, err := d.decoder.ReadInt32()
	if err != nil {
		return value.Int32{}, d.WrapError(err)
	}

	d.appendDecodedField(decodedValue)

	return decodedValue, nil
}

func (d *FieldDecoder) ReadInt64Field(path string) (value.Int64, FieldDecoderError) {
	d.PushPathContext(path)
	defer d.PopPathContext()

	decodedValue, err := d.decoder.ReadInt64()

	if err != nil {
		return value.Int64{}, d.WrapError(err)
	}

	d.appendDecodedField(decodedValue)

	return decodedValue, nil
}

func (d *FieldDecoder) ReadRawBytes(path string, count int) (value.RawBytes, FieldDecoderError) {
	d.PushPathContext(path)
	defer d.PopPathContext()

	decodedValue, err := d.decoder.ReadRawBytes(count)
	if err != nil {
		return value.RawBytes{}, d.WrapError(err)
	}

	d.appendDecodedField(decodedValue)

	return decodedValue, nil
}

func (d *FieldDecoder) ConsumeTagBufferField() FieldDecoderError {
	d.PushPathContext("TAG_BUFFER")
	defer d.PopPathContext()

	if err := d.decoder.ConsumeTagBuffer(); err != nil {
		return d.WrapError(err)
	}

	return nil
}

func (d *FieldDecoder) RemainingBytesCount() uint64 {
	return d.decoder.RemainingBytesCount()
}

func (d *FieldDecoder) currentPath() field_path.FieldPath {
	pathNamesFromContext := []string{}

	for _, pathContext := range d.currentPathContexts {
		pathNamesFromContext = append(pathNamesFromContext, pathContext.pathName)
	}

	return field_path.NewFieldPath(strings.Join(pathNamesFromContext, "."))
}

func (d *FieldDecoder) appendDecodedField(decodedValue value.KafkaProtocolValue) {
	// retrieve most recently added path context's offset so the offset of the decoded field can be the same
	lastPathContextOffset := d.currentPathContexts[len(d.currentPathContexts)-1].offset

	d.decodedFields = append(d.decodedFields, field.Field{
		Value:       decodedValue,
		Path:        d.currentPath(),
		StartOffset: int(lastPathContextOffset),
		// The pointer will have pointed to the next byte already, so we re-adjust the pointer to the previous byte
		EndOffset: int(d.ReadBytesCount() - 1),
	})
}

func (d *FieldDecoder) WrapError(err error) FieldDecoderError {
	// If we've already wrapped the error, preserve the nested path
	if fieldDecoderError, ok := err.(*fieldDecoderErrorImpl); ok {
		return fieldDecoderError
	}

	if decoderError, ok := err.(decoder.DecoderError); ok {
		return &fieldDecoderErrorImpl{
			message:     err.Error(),
			startOffset: decoderError.StartOffset(),
			endOffset:   decoderError.EndOffset(),
			path:        d.currentPath(),
		}
	}

	panic("Codecrafters Internal Error - error is not of type fieldDecoderErrorImpl or DecoderError")
}

func (d *FieldDecoder) WrapErrorForLastPathSegment(err error, lastPathSegment string) FieldDecoderError {
	decodedFieldPath := fmt.Sprintf("%s.%s", d.currentPath().String(), lastPathSegment)

	for _, decodedField := range d.decodedFields {
		if decodedFieldPath == decodedField.Path.String() {

			// adjust the context for throwing the error properly
			d.PushPathContext(decodedField.Path.LastSegment())

			return &fieldDecoderErrorImpl{
				message:     err.Error(),
				startOffset: int(decodedField.StartOffset),
				endOffset:   int(decodedField.EndOffset),
				path:        d.currentPath(),
			}
		}
	}

	panic("Codecrafters Internal Error - Path '%s' not found in any any of the decoded fields.")
}
