package field_decoder

import (
	"strings"

	"github.com/codecrafters-io/kafka-tester/protocol/decoder"
	"github.com/codecrafters-io/kafka-tester/protocol/value"
)

type Field struct {
	Locator string
	Value   value.KafkaProtocolValue
}

type FieldDecoder struct {
	currentLocatorSegments []string
	decoder                *decoder.Decoder
	decodedFields          []Field
}

func NewFieldDecoder(bytes []byte) *FieldDecoder {
	return &FieldDecoder{
		currentLocatorSegments: []string{},
		decodedFields:          []Field{},
		decoder:                decoder.NewDecoder(bytes),
	}
}

// TODO[PaulRefactor]: See if we can bake this into error?
func (d *FieldDecoder) CurrentLocator() string {
	return strings.Join(d.currentLocatorSegments, ".")
}

func (d *FieldDecoder) DecodedFields() []Field {
	return d.decodedFields
}

func (d *FieldDecoder) PushLocatorSegment(locator string) {
	d.currentLocatorSegments = append(d.currentLocatorSegments, locator)
}

func (d *FieldDecoder) PopLocatorSegment() {
	d.currentLocatorSegments = d.currentLocatorSegments[:len(d.currentLocatorSegments)-1]
}

func (d *FieldDecoder) ReadCompactArrayLength(locator string) (value.CompactArrayLength, error) {
	d.PushLocatorSegment(locator)

	decodedValue, err := d.decoder.ReadCompactArrayLength()
	if err != nil {
		return value.CompactArrayLength{}, err
	}

	d.appendDecodedField(decodedValue)
	d.PopLocatorSegment()

	return decodedValue, nil
}

func (d *FieldDecoder) ReadInt16(locator string) (value.Int16, error) {
	d.PushLocatorSegment(locator)

	decodedValue, err := d.decoder.ReadInt16()
	if err != nil {
		return value.Int16{}, err
	}

	d.appendDecodedField(decodedValue)
	d.PopLocatorSegment()

	return decodedValue, nil
}

func (d *FieldDecoder) ReadInt32(locator string) (value.Int32, error) {
	d.PushLocatorSegment(locator)

	decodedValue, err := d.decoder.ReadInt32()
	if err != nil {
		return value.Int32{}, err
	}

	d.appendDecodedField(decodedValue)
	d.PopLocatorSegment()

	return decodedValue, nil
}

func (d *FieldDecoder) ConsumeTagBuffer() error {
	d.PushLocatorSegment("TAG_BUFFER")

	if err := d.decoder.ConsumeTagBuffer(); err != nil {
		return err
	}

	d.PopLocatorSegment()
	return nil
}

func (d *FieldDecoder) RemainingBytesCount() uint64 {
	return d.decoder.RemainingBytesCount()
}

func (d *FieldDecoder) constructLocator() string {
	return strings.Join(d.currentLocatorSegments, ".")
}

func (d *FieldDecoder) appendDecodedField(decodedValue value.KafkaProtocolValue) {
	d.decodedFields = append(d.decodedFields, Field{
		Value:   decodedValue,
		Locator: d.constructLocator(),
	})
}
