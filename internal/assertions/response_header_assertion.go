package assertions

import (
	"fmt"

	"github.com/codecrafters-io/kafka-tester/protocol/kafkaapi/headers"
	"github.com/codecrafters-io/tester-utils/logger"
)

type ResponseHeaderAssertion struct {
	ActualValue   headers.ResponseHeader
	ExpectedValue headers.ResponseHeader
}

func NewResponseHeaderAssertion(actualValue headers.ResponseHeader, expectedValue headers.ResponseHeader) *ResponseHeaderAssertion {
	return &ResponseHeaderAssertion{
		ActualValue:   actualValue,
		ExpectedValue: expectedValue,
	}
}

func (a *ResponseHeaderAssertion) assertCorrelationId(logger *logger.Logger) error {
	if a.ActualValue.CorrelationId.Value != a.ExpectedValue.CorrelationId.Value {
		return fmt.Errorf("Expected correlation_id to be %d, got %d", a.ExpectedValue.CorrelationId.Value, a.ActualValue.CorrelationId.Value)
	}

	logger.Successf("✓ correlation_id: %s", a.ActualValue.CorrelationId)

	return nil
}

func (a *ResponseHeaderAssertion) Run(logger *logger.Logger) error {
	return a.assertCorrelationId(logger)
}
