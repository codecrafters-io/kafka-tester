package assertions

// TODO[PaulRefactor]: Rename package itself to "response_assertions", separate from value_assertions

import (
	"github.com/codecrafters-io/kafka-tester/internal/assertions/value_assertion"
)

// TODO[PaulRefactor]: Add more to the interface? Is this all that's needed? If so, why not just accept the value assertions map directly?
type ResponseAssertion interface {
	GetValueAssertionCollection() value_assertion.ValueAssertionCollection
}
