package instrumented_decoder

import (
	"fmt"
	"strings"

	"github.com/codecrafters-io/kafka-tester/internal/assertions/value_assertion"
	wireDecoder "github.com/codecrafters-io/kafka-tester/protocol/decoder"
	kafkaValue "github.com/codecrafters-io/kafka-tester/protocol/value"
	"github.com/codecrafters-io/tester-utils/logger"
)

type InstrumentedDecoder struct {
	*wireDecoder.Decoder
	logger  *logger.Logger
	locator []string
}

func (d *InstrumentedDecoder) GetLogger() *logger.Logger {
	return d.logger
}

func (d *InstrumentedDecoder) getLocator() string {
	return strings.Join(d.locator, ".")
}

func (d *InstrumentedDecoder) BeginSubSection(sectionName string) {
	d.logger.Debugf("%s%s", d.getIndentationString("-"), sectionName)
	d.locator = append(d.locator, sectionName)
}

func (d *InstrumentedDecoder) EndCurrentSubSection() {
	if len(d.locator) > 0 {
		d.locator = d.locator[:len(d.locator)-1]
	}
}

func (d *InstrumentedDecoder) getLocatorWithVariableName(variableName string) string {
	return fmt.Sprintf("%s.%s", d.getLocator(), variableName)
}

func (d *InstrumentedDecoder) LogDecodedValue(variableName string, value kafkaValue.KafkaProtocolValue, bulletMarker string) {
	indentationString := d.getIndentationString(bulletMarker)

	if value.GetType() == kafkaValue.TAG_BUFFER {
		d.logger.Debugf("%s%s", indentationString, kafkaValue.TAG_BUFFER)
		return
	}

	valueString := value.String()
	d.logger.Debugf("%s%s (%s)", indentationString, variableName, valueString)
}

func (d *InstrumentedDecoder) LogIncorrectDecodedValue(variableName string, value kafkaValue.KafkaProtocolValue) {
	indentationString := d.getIndentationString("✘")
	variableType := value.GetType()
	d.logger.Errorf("%s%s (%s)", indentationString, variableName, variableType)
}

func (d *InstrumentedDecoder) LogDecodingError(variableName string) {
	indentationString := d.getIndentationString("✘")
	d.logger.Errorf("%s%s", indentationString, variableName)
}

func NewInstrumentedDecoder(rawBytes []byte, logger *logger.Logger, valueAssertions value_assertion.ValueAssertionCollection) *InstrumentedDecoder {
	instrumentedDecoderLogger := logger.Clone()
	instrumentedDecoderLogger.UpdateLastSecondaryPrefix("Decoder")

	instrumentedDecoder := InstrumentedDecoder{
		logger:  instrumentedDecoderLogger,
		locator: nil,
	}

	decoder := wireDecoder.NewDecoder(rawBytes).SetCallbacks(&wireDecoder.DecoderCallbacks{
		OnDecode: func(variableName string, value kafkaValue.KafkaProtocolValue) error {
			locator := instrumentedDecoder.getLocatorWithVariableName(variableName)

			valueAssertion := valueAssertions.GetValueAssertion(locator)

			// No assertion
			if value_assertion.CheckIfValueAssertionIsNil(valueAssertion) {
				instrumentedDecoder.LogDecodedValue(variableName, value, "-")
				return nil
			}

			err := value_assertion.RunValueAssertion(valueAssertion, value)

			// Assertion fail
			if err != nil {
				instrumentedDecoder.LogIncorrectDecodedValue(variableName, value)
				return err
			}

			instrumentedDecoder.LogDecodedValue(variableName, value, "✔")
			return nil
		},
		OnDecodeError: func(variableName string) {
			instrumentedDecoder.LogDecodingError(variableName)
		},
		OnCompositeDecodingStart: func(compositeVariableName string) {
			instrumentedDecoder.BeginSubSection(compositeVariableName)
		},
		OnCompositeDecodingEnd: func() {
			instrumentedDecoder.EndCurrentSubSection()
		},
	})

	instrumentedDecoder.Decoder = decoder
	return &instrumentedDecoder
}

func (d *InstrumentedDecoder) getIndentationString(bulletMarker string) string {
	indentationLevel := len(d.locator)
	indentationSpaces := strings.Repeat("  ", indentationLevel)
	bullet := fmt.Sprintf("%s ", bulletMarker)

	// Only need dot for indented level
	if indentationLevel != 0 {
		bullet = fmt.Sprintf("%s .", bulletMarker)
	}

	return indentationSpaces + bullet
}
