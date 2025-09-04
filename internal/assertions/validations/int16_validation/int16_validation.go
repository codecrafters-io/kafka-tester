package int16_validation

import (
	"fmt"

	"github.com/codecrafters-io/kafka-tester/protocol/value"
)

type IsEqualValidation struct {
	ExpectedVaue uint64
}

func IsEqual(expectedValue int16) *IsEqualValidation {
	return &IsEqualValidation{
		ExpectedVaue: uint64(expectedValue),
	}
}

func (v *IsEqualValidation) Validate(receivedValue value.KafkaProtocolValue) error {
	if castedInt16, ok := receivedValue.(*value.Int16); ok {
		if castedInt16.Value == int16(v.ExpectedVaue) {
			return nil
		}
		return fmt.Errorf("Error: Expected INT16 value to be %d, got %d", v.ExpectedVaue, castedInt16.Value)
	}
	return fmt.Errorf("Error: Expected INT16, got %s", receivedValue.GetType())
}
