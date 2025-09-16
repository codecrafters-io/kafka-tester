package value_assertions

import (
	"fmt"

	value "github.com/codecrafters-io/kafka-tester/protocol/value"
)

func IsEqualTo(expectedValue value.CompactArrayLength, actualValue value.KafkaProtocolValue) error {
	castedActualValue, ok := actualValue.(value.CompactArrayLength)
	if !ok {
		panic("CodeCrafters Internal Error: Expected COMPACT_ARRAY_LENGTH value, got " + actualValue.GetType())
	}

	if castedActualValue.Value != expectedValue.Value {
		return fmt.Errorf("Error: Expected COMPACT_ARRAY_LENGTH value to be %s, got %s", expectedValue, castedActualValue)
	}

	return nil
}
