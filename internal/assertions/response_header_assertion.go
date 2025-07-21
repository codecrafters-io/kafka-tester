package assertions

import (
	"fmt"

	kafkaapi "github.com/codecrafters-io/kafka-tester/protocol/api"
	"github.com/codecrafters-io/tester-utils/logger"
)

type ResponseHeaderAssertion struct {
	ActualValue   kafkaapi.ResponseHeader
	ExpectedValue kafkaapi.ResponseHeader
	logger        *logger.Logger
	err           error

	// nil = don't assert this level
	// empty slice = assert all fields (default)
	// non-empty slice = assert with exclusions
	excludedHeaderFields []string
}

func NewResponseHeaderAssertion(actualValue kafkaapi.ResponseHeader, expectedValue kafkaapi.ResponseHeader, logger *logger.Logger) *ResponseHeaderAssertion {
	return &ResponseHeaderAssertion{
		ActualValue:          actualValue,
		ExpectedValue:        expectedValue,
		logger:               logger,
		excludedHeaderFields: []string{},
	}
}

// assertHeader asserts the contents of the response header
// Fields asserted by default: CorrelationId
func (a *ResponseHeaderAssertion) assertHeader() *ResponseHeaderAssertion {
	if a.err != nil {
		return a
	}

	if !Contains(a.excludedHeaderFields, "CorrelationId") {
		if a.ActualValue.CorrelationId != a.ExpectedValue.CorrelationId {
			a.err = fmt.Errorf("Expected %s to be %d, got %d", "CorrelationId", a.ExpectedValue.CorrelationId, a.ActualValue.CorrelationId)
			return a
		}
		a.logger.Successf("✓ Correlation ID: %v", a.ActualValue.CorrelationId)
	}

	return a
}

// Run runs assertHeader internally
func (a *ResponseHeaderAssertion) Run() error {
	a.assertHeader()

	return a.err
}
