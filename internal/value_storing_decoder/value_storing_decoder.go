package value_storing_decoder

import (
	"iter"
	"strings"

	"github.com/codecrafters-io/kafka-tester/protocol/decoder"
	"github.com/codecrafters-io/kafka-tester/protocol/value"
)

type ValueWithLocator struct {
	Value   value.KafkaProtocolValue
	Locator string
}

type ValueStoringDecoder struct {
	currentLocatorSegments    []string
	decoder                   *decoder.Decoder
	decodedValuesWithLocators []ValueWithLocator
}

func NewValueStoringDecoder(bytes []byte) *ValueStoringDecoder {
	return &ValueStoringDecoder{
		decoder:                   decoder.NewDecoder(bytes),
		currentLocatorSegments:    []string{},
		decodedValuesWithLocators: []ValueWithLocator{},
	}
}

// TODO[PaulRefactor]: See if we can bake this into error?
func (d *ValueStoringDecoder) CurrentLocator() string {
	return strings.Join(d.currentLocatorSegments, ".")
}

func (d *ValueStoringDecoder) DecodedValuesWithLocators() iter.Seq2[value.KafkaProtocolValue, string] {
	return func(yield func(value.KafkaProtocolValue, string) bool) {
		for _, valueWithLocator := range d.decodedValuesWithLocators {
			if !yield(valueWithLocator.Value, valueWithLocator.Locator) {
				return
			}
		}
	}
}

func (d *ValueStoringDecoder) PushLocatorSegment(locator string) {
	d.currentLocatorSegments = append(d.currentLocatorSegments, locator)
}

func (d *ValueStoringDecoder) PopLocatorSegment() {
	d.currentLocatorSegments = d.currentLocatorSegments[:len(d.currentLocatorSegments)-1]
}

func (d *ValueStoringDecoder) ReadCompactArrayLength(locator string) (value.CompactArrayLength, error) {
	d.PushLocatorSegment(locator)

	decodedValue, err := d.decoder.ReadCompactArrayLength()
	if err != nil {
		return value.CompactArrayLength{}, err
	}

	d.appendDecodedValue(decodedValue)
	d.PopLocatorSegment()

	return decodedValue, nil
}

func (d *ValueStoringDecoder) ReadInt16(locator string) (value.Int16, error) {
	d.PushLocatorSegment(locator)

	decodedValue, err := d.decoder.ReadInt16()
	if err != nil {
		return value.Int16{}, err
	}

	d.appendDecodedValue(decodedValue)
	d.PopLocatorSegment()

	return decodedValue, nil
}

func (d *ValueStoringDecoder) ReadInt32(locator string) (value.Int32, error) {
	d.PushLocatorSegment(locator)

	decodedValue, err := d.decoder.ReadInt32()
	if err != nil {
		return value.Int32{}, err
	}

	d.appendDecodedValue(decodedValue)
	d.PopLocatorSegment()

	return decodedValue, nil
}

func (d *ValueStoringDecoder) ConsumeTagBuffer() error {
	d.PushLocatorSegment("TAG_BUFFER")

	if err := d.decoder.ConsumeTagBuffer(); err != nil {
		return err
	}

	d.PopLocatorSegment()
	return nil
}

func (d *ValueStoringDecoder) RemainingBytesCount() uint64 {
	return d.decoder.RemainingBytesCount()
}

func (d *ValueStoringDecoder) constructLocator() string {
	return strings.Join(d.currentLocatorSegments, ".")
}

func (d *ValueStoringDecoder) appendDecodedValue(decodedValue value.KafkaProtocolValue) {
	d.decodedValuesWithLocators = append(d.decodedValuesWithLocators, ValueWithLocator{
		Value:   decodedValue,
		Locator: d.constructLocator(),
	})
}
