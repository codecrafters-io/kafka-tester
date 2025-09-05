package response_assertions

import (
	"github.com/codecrafters-io/kafka-tester/protocol/value"
	"github.com/codecrafters-io/tester-utils/logger"
)

type ResponseAssertion interface {
	AssertDecodedValue(locator string, value value.KafkaProtocolValue) error
	RunCompositeAssertions(logger *logger.Logger) error
}
