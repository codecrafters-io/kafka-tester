package assertions

// TODO[PaulRefactor]: Rename package itself to "response_assertions", separate from value_assertions

import (
	"github.com/codecrafters-io/kafka-tester/protocol/value"
	"github.com/codecrafters-io/tester-utils/logger"
)

type ResponseAssertion interface {
	AssertDecodedValue(locator string, value value.KafkaProtocolValue) error
	RunCompositeAssertions(logger *logger.Logger) error
}
