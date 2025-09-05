package int16_value_assertion

import (
	"fmt"

	"github.com/codecrafters-io/kafka-tester/protocol/value"
)

// TODO[PaulRefactor]: Remove the word "Validation" altogether, we can get away with just calling these assertions
type IsEqualValidation struct {
	ExpectedVaue uint64
}

func IsEqual(expectedValue int16) *IsEqualValidation {
	return &IsEqualValidation{
		ExpectedVaue: uint64(expectedValue),
	}
}

func (v *IsEqualValidation) Assert(receivedValue value.KafkaProtocolValue) error {
	castedValue, _ := receivedValue.(*value.Int16)
	if castedValue.Value == int16(v.ExpectedVaue) {
		return nil
	}
	return fmt.Errorf("Error: Expected INT16 value to be %d, got %d", v.ExpectedVaue, castedValue.Value)
}
