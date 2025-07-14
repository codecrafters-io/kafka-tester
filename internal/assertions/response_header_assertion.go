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
}

func NewResponseHeaderAssertion(actualValue kafkaapi.ResponseHeader, expectedValue kafkaapi.ResponseHeader, logger *logger.Logger) *ResponseHeaderAssertion {
	return &ResponseHeaderAssertion{
		ActualValue:   actualValue,
		ExpectedValue: expectedValue,
		logger:        logger,
	}
}

func (a *ResponseHeaderAssertion) AssertHeader(fields []string) *ResponseHeaderAssertion {
	if a.err != nil {
		return a
	}

	if Contains(fields, "CorrelationId") {
		if a.ActualValue.CorrelationId != a.ExpectedValue.CorrelationId {
			a.err = fmt.Errorf("Expected %s to be %d, got %d", "CorrelationId", a.ExpectedValue.CorrelationId, a.ActualValue.CorrelationId)
			return a
		}
		a.logger.Successf("✓ Correlation ID: %v", a.ActualValue.CorrelationId)
	}

	return a
}

func (a *ResponseHeaderAssertion) Run() error {
	return a.err
}
