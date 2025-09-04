package assertions

import (
	"github.com/codecrafters-io/kafka-tester/internal/assertions/validations"
)

type Assertion interface {
	GetPrimitiveValidation(string) validations.Validation
}
