package value_assertions

import (
	"fmt"

	value "github.com/codecrafters-io/kafka-tester/protocol/value"
)

func IsEqualTo(expectedValue int32, actualValue value.KafkaProtocolValue) error {
	castedActualValue, ok := actualValue.(value.Int32)
	if !ok {
		panic("CodeCrafters Internal Error: Expected INT32 value, got " + actualValue.GetType())
	}

	if castedActualValue.Value != expectedValue {
		return fmt.Errorf("Error: Expected INT32 value to be %d, got %d", expectedValue, castedActualValue.Value)
	}

	return nil
}
