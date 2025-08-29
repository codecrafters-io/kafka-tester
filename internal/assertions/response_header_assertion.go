package assertions

import (
	"fmt"

	"github.com/codecrafters-io/kafka-tester/protocol/kafkaapi_legacy/headers_legacy"
	"github.com/codecrafters-io/tester-utils/logger"
)

type ResponseHeaderAssertion struct {
	ActualValue   headers_legacy.ResponseHeader
	ExpectedValue headers_legacy.ResponseHeader
}

func NewResponseHeaderAssertion(actualValue headers_legacy.ResponseHeader, expectedValue headers_legacy.ResponseHeader) *ResponseHeaderAssertion {
	return &ResponseHeaderAssertion{
		ActualValue:   actualValue,
		ExpectedValue: expectedValue,
	}
}

func (a *ResponseHeaderAssertion) assertCorrelationId(logger *logger.Logger) error {
	if a.ActualValue.CorrelationId != a.ExpectedValue.CorrelationId {
		return fmt.Errorf("Expected correlation_id to be %d, got %d", a.ExpectedValue.CorrelationId, a.ActualValue.CorrelationId)
	}
	logger.Successf("✓ correlation_id: %v", a.ActualValue.CorrelationId)

	return nil
}

func (a *ResponseHeaderAssertion) Run(logger *logger.Logger) error {
	return a.assertCorrelationId(logger)
}
