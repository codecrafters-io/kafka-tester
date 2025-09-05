package assertions

import (
	"strings"

	"github.com/codecrafters-io/kafka-tester/internal/assertions/value_assertion"
	"github.com/codecrafters-io/kafka-tester/internal/assertions/value_assertion/int16_value_assertion"
	"github.com/codecrafters-io/kafka-tester/internal/assertions/value_assertion/int32_value_assertion"
)

type ResponseHeaderAssertion struct {
	rootLevelLocator string
	valueAssertions  value_assertion.ValueAssertionCollection
}

func NewResponseHeaderAssertion(responseLocator string) *ResponseHeaderAssertion {
	return &ResponseHeaderAssertion{
		rootLevelLocator: responseLocator,
		valueAssertions:  value_assertion.NewValueAssertionMap(),
	}
}

// Value Assertions

func (a *ResponseHeaderAssertion) WithCorrelationId(expectedCorrelationID int32) *ResponseHeaderAssertion {
	locator := a.addToBaseLocator("CorrelationID")
	a.valueAssertions.Add(locator, int32_value_assertion.IsEqual(expectedCorrelationID))
	return a
}

func (a *ResponseHeaderAssertion) WithErrorCode(expectedCorrelationID int16) *ResponseHeaderAssertion {
	locator := a.addToBaseLocator("ErrorCode")
	a.valueAssertions.Add(locator, int16_value_assertion.IsEqual(expectedCorrelationID))
	return a
}

func (a *ResponseHeaderAssertion) GetValueAssertionCollection() value_assertion.ValueAssertionCollection {
	return a.valueAssertions
}

func (a *ResponseHeaderAssertion) getBaseLocator() string {
	return strings.Join([]string{a.rootLevelLocator, "ResponseHeader"}, ".")
}

func (a *ResponseHeaderAssertion) addToBaseLocator(subLocator string) string {
	return strings.Join([]string{a.getBaseLocator(), subLocator}, ".")
}
