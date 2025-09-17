package value_assertions

import (
	"fmt"

	value "github.com/codecrafters-io/kafka-tester/protocol/value"
)

func IsEqualTo(expectedValue int8, actualValue value.KafkaProtocolValue) error {
	castedActualValue, ok := actualValue.(value.Int8)
	if !ok {
		panic("CodeCrafters Internal Error: Expected INT8 value, got " + actualValue.GetType())
	}

	if castedActualValue.Value != expectedValue {
		return fmt.Errorf("Error: Expected INT8 value to be %d, got %d", expectedValue, castedActualValue.Value)
	}

	return nil
}
