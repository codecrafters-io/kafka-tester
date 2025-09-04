package int32_validation

import (
	"fmt"

	"github.com/codecrafters-io/kafka-tester/protocol/value"
)

type IsEqualValidation struct {
	ExpectedVaue uint64
}

func IsEqual(expectedValue int32) *IsEqualValidation {
	return &IsEqualValidation{
		ExpectedVaue: uint64(expectedValue),
	}
}

func (v *IsEqualValidation) Validate(receivedValue value.KafkaProtocolValue) error {
	if castedInt32, ok := receivedValue.(*value.Int32); ok {
		if castedInt32.Value == int32(v.ExpectedVaue) {
			return nil
		}
		return fmt.Errorf("Error: Expected INT32 value to be %d, got %d", v.ExpectedVaue, castedInt32.Value)
	}
	return fmt.Errorf("Error: Expected INT32, got %s", receivedValue.GetType())
}
