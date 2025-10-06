package response_assertions

import (
	"github.com/codecrafters-io/kafka-tester/internal/field"
	"github.com/codecrafters-io/tester-utils/logger"
)

type ResponseAssertion[T any] interface {
	// AssertSingleField is used to assert fields as they're decoded.
	//
	// Errors will be rendered in the middle of the decoded response, right after the specific field.
	AssertSingleField(field field.Field) *SingleFieldAssertionError

	// AssertAcrossFields is used to run assertions that span multiple fields and hence wouldn't fit in AssertSingleField.
	//
	// Errors will be rendered after the whole decoded response is printed.
	AssertAcrossFields(actualResponse T, logger *logger.Logger) error
}
