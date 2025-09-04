package validations

import "github.com/codecrafters-io/kafka-tester/protocol/value"

type Validation interface {
	Validate(receivedValue value.KafkaProtocolValue) error
}

type ValidationMap map[string]Validation

func NewValidationMap(validations map[string]Validation) ValidationMap {
	return validations
}
