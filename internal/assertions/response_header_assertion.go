package assertions

import (
	"fmt"

	"github.com/codecrafters-io/kafka-tester/protocol/api/headers"
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
	if a.ActualValue.CorrelationId != a.ExpectedValue.CorrelationId {
		return fmt.Errorf("Expected correlation_id to be %d, got %d", a.ExpectedValue.CorrelationId, a.ActualValue.CorrelationId)
	}
	logger.Successf("✓ correlation_id: %v", a.ActualValue.CorrelationId)

	return nil
}

func (a *ResponseHeaderAssertion) Run(logger *logger.Logger) error {
	return a.assertCorrelationId(logger)
}

/******************* Updated interface *****************/

type ResponseHeaderAssertion__Updated struct {
	ActualValue   headers.ResponseHeader__Updated
	ExpectedValue headers.ResponseHeader__Updated
}

func NewResponseHeaderAssertion__Updated(actualValue headers.ResponseHeader__Updated, expectedValue headers.ResponseHeader__Updated) *ResponseHeaderAssertion__Updated {
	return &ResponseHeaderAssertion__Updated{
		ActualValue:   actualValue,
		ExpectedValue: expectedValue,
	}
}

func (a *ResponseHeaderAssertion__Updated) assertCorrelationId(logger *logger.Logger) error {
	if a.ActualValue.CorrelationId != a.ExpectedValue.CorrelationId {
		return fmt.Errorf("Expected correlation_id to be %d, got %d", a.ExpectedValue.CorrelationId, a.ActualValue.CorrelationId)
	}
	logger.Successf("✓ correlation_id: %v", a.ActualValue.CorrelationId)

	return nil
}

func (a *ResponseHeaderAssertion__Updated) Run(logger *logger.Logger) error {
	return a.assertCorrelationId(logger)
}
