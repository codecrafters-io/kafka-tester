package value_assertion

import (
	"reflect"

	kafkaValue "github.com/codecrafters-io/kafka-tester/protocol/value"
)

type ValueAssertion interface {
	Assert(receivedValue kafkaValue.KafkaProtocolValue) error
}

type ValueAssertionCollection map[string]ValueAssertion

func NewValueAssertionMap() ValueAssertionCollection {
	return make(ValueAssertionCollection)
}

func (a ValueAssertionCollection) Add(locator string, valueAssertion ValueAssertion) {
	a[locator] = valueAssertion
}

func (a ValueAssertionCollection) GetValueAssertion(locator string) ValueAssertion {
	valueAssertion, ok := a[locator]
	if !ok {
		return nil
	}
	return valueAssertion
}

func RunValueAssertion(valueAssertion ValueAssertion, value kafkaValue.KafkaProtocolValue) error {
	switch value.GetType() {
	case kafkaValue.INT16:
		castedInt16Value, _ := value.(*kafkaValue.Int16)
		return valueAssertion.Assert(castedInt16Value)
	case kafkaValue.INT32:
		castedInt32Value, _ := value.(*kafkaValue.Int32)
		return valueAssertion.Assert(castedInt32Value)
	default:
		return valueAssertion.Assert(value)
	}
}

func CheckIfValueAssertionIsNil(valueAssertion ValueAssertion) bool {
	return valueAssertion == nil || reflect.ValueOf(valueAssertion).IsNil()
}
