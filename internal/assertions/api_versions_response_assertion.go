package assertions

import (
	"fmt"

	"github.com/codecrafters-io/kafka-tester/internal/assertions/value_assertion"
	"github.com/codecrafters-io/kafka-tester/protocol/kafkaapi"
)

type ApiVersionsResponseAssertion struct {
	headerAssertion    *ResponseHeaderAssertion
	expectedAPIKey     int16
	expectedMinVersion int16
	expectedMaxVersion int16
}

func NewApiVersionsResponseAssertion() *ApiVersionsResponseAssertion {
	return &ApiVersionsResponseAssertion{
		headerAssertion: NewResponseHeaderAssertion("ApiVersionsResponse"),
	}
}

func (a *ApiVersionsResponseAssertion) WithCorrelationId(expectedCorrelationID int32) *ApiVersionsResponseAssertion {
	a.headerAssertion = a.headerAssertion.WithCorrelationId(expectedCorrelationID)
	return a
}

func (a *ApiVersionsResponseAssertion) WithErrorCode(expectedErrorCode int16) *ApiVersionsResponseAssertion {
	a.headerAssertion = a.headerAssertion.WithErrorCode(expectedErrorCode)
	return a
}

func (a *ApiVersionsResponseAssertion) GetValueAssertionCollection() value_assertion.ValueAssertionCollection {
	return a.headerAssertion.GetValueAssertionCollection()
}

// Composite Assertions

func (a *ApiVersionsResponseAssertion) WithAPIKey(expectedAPIKey, expectedMinVersion, expectedMaxVersion int16) *ApiVersionsResponseAssertion {
	a.expectedAPIKey = expectedAPIKey
	a.expectedMinVersion = expectedMinVersion
	a.expectedMaxVersion = expectedMaxVersion
	return a
}

func (a *ApiVersionsResponseAssertion) RunCompositeAssertions(response kafkaapi.ApiVersionsResponse) error {
	// Check that at least one entry has API key = 18 (API_VERSIONS)
	foundAPIKey18 := false
	var apiKey18Entry kafkaapi.ApiKeyEntry

	for i := range response.Body.ApiKeys {
		if response.Body.ApiKeys[i].ApiKey.Value == 18 {
			foundAPIKey18 = true
			apiKey18Entry = response.Body.ApiKeys[i]
			break
		}
	}

	if !foundAPIKey18 {
		return fmt.Errorf("Expected APIVersionsResponseKey array to include API key 18 (API_VERSIONS)")
	}

	// Check version rules from legacy code
	if apiKey18Entry.MinVersion.Value > a.expectedMaxVersion {
		return fmt.Errorf("Expected min version %v to be <= max version %v for API_VERSIONS", apiKey18Entry.MinVersion.Value, a.expectedMaxVersion)
	}

	// anything above or equal to expected minVersion is fine
	if apiKey18Entry.MinVersion.Value < a.expectedMinVersion {
		return fmt.Errorf("Expected API version %v to be supported for API_VERSIONS, got %v", a.expectedMinVersion, apiKey18Entry.MinVersion.Value)
	}

	if apiKey18Entry.MaxVersion.Value < a.expectedMaxVersion {
		return fmt.Errorf("Expected API version %v to be supported for API_VERSIONS, got %v", a.expectedMaxVersion, apiKey18Entry.MaxVersion.Value)
	}

	return nil
}
