package int32_value_assertion

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

func (v *IsEqualValidation) Assert(receivedValue value.KafkaProtocolValue) error {
	castedValue, _ := receivedValue.(*value.Int32)
	if castedValue.Value == int32(v.ExpectedVaue) {
		return nil
	}
	return fmt.Errorf("Error: Expected INT32 value to be %d, got %d", v.ExpectedVaue, castedValue.Value)
}
