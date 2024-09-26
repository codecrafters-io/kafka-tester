package assertions

import (
	"fmt"

	kafkaapi "github.com/codecrafters-io/kafka-tester/protocol/api"
	"github.com/codecrafters-io/tester-utils/logger"
)

type ResponseHeaderAssertion struct {
	ActualValue   kafkaapi.ResponseHeader
	ExpectedValue kafkaapi.ResponseHeader
}

func NewResponseHeaderAssertion(actualValue kafkaapi.ResponseHeader, expectedValue kafkaapi.ResponseHeader) ResponseHeaderAssertion {
	return ResponseHeaderAssertion{ActualValue: actualValue, ExpectedValue: expectedValue}
}

func (a ResponseHeaderAssertion) Evaluate(fields []string, logger *logger.Logger) error {
	if Contains(fields, "CorrelationId") {
		if a.ActualValue.CorrelationId != a.ExpectedValue.CorrelationId {
			return fmt.Errorf("Expected %s to be %d, got %d", "CorrelationId", a.ExpectedValue.CorrelationId, a.ActualValue.CorrelationId)
		}
		logger.Successf("✓ Correlation ID: %v", a.ActualValue.CorrelationId)
	}
	return nil
}
