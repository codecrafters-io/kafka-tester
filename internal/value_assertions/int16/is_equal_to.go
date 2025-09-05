package value_assertions

import (
	"fmt"

	value "github.com/codecrafters-io/kafka-tester/protocol/value"
)

func IsEqualTo(expectedValue int16, actualValue value.KafkaProtocolValue) error {
	castedActualValue, ok := actualValue.(*value.Int16)
	if !ok {
		panic("CodeCrafters Internal Error: Expected INT16 value, got " + actualValue.GetType())
	}

	if castedActualValue.Value != int16(expectedValue) {
		return fmt.Errorf("Error: Expected INT16 value to be %d, got %d", expectedValue, castedActualValue.Value)
	}

	return nil
}
