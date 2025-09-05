package assertions

import (
	"github.com/codecrafters-io/kafka-tester/internal/assertions/value_assertion"
)

type ResponseAssertion interface {
	GetValueAssertionCollection() value_assertion.ValueAssertionCollection
}
