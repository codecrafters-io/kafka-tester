package response_assertions

import (
	"github.com/codecrafters-io/kafka-tester/protocol/value"
	"github.com/codecrafters-io/tester-utils/logger"
)

type ResponseAssertion[T any] interface {
	// AssertDecodedValue is used to assert a specific value as it is decoded.
	//
	// Errors will be rendered in the middle of the decoded response, right after the specific decoded value.
	AssertDecodedValue(locator string, value value.KafkaProtocolValue) error

	// RunCompositeAssertions is used to run assertions that span multiple values and hence wouldn't fit in AssertDecodedValue.
	//
	// Errors will be rendered after the whole decoded response is printed.
	RunCompositeAssertions(actualResponse T, logger *logger.Logger) error
}
