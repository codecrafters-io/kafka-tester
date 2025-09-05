package assertions

import (
	"fmt"

	"github.com/codecrafters-io/kafka-tester/internal/assertions/value_assertion"
	"github.com/codecrafters-io/kafka-tester/internal/assertions/value_assertion/int16_value_assertion"
	"github.com/codecrafters-io/kafka-tester/internal/assertions/value_assertion/int32_value_assertion"
	"github.com/codecrafters-io/kafka-tester/protocol/kafkaapi"
	"github.com/codecrafters-io/tester-utils/logger"
)

var apiKeyNames = map[int16]string{
	1:  "FETCH",
	18: "API_VERSIONS",
	75: "DESCRIBE_TOPIC_PARTITIONS",
}

type ApiVersionsResponseAssertion struct {
	valueAssertions    value_assertion.ValueAssertionCollection
	expectedAPIKey     int16
	expectedMinVersion int16
	expectedMaxVersion int16
}

func NewApiVersionsResponseAssertion() *ApiVersionsResponseAssertion {
	return &ApiVersionsResponseAssertion{
		valueAssertions: value_assertion.NewValueAssertionMap(),
	}
}

func (a *ApiVersionsResponseAssertion) WithCorrelationId(expectedCorrelationID int32) *ApiVersionsResponseAssertion {
	a.valueAssertions.Add(
		"ApiVersionsResponse.ResponseHeader.CorrelationID", int32_value_assertion.IsEqual(expectedCorrelationID),
	)
	return a
}

func (a *ApiVersionsResponseAssertion) WithErrorCode(expectedErrorCode int16) *ApiVersionsResponseAssertion {
	a.valueAssertions.Add(
		"ApiVersionsResponse.ApiVersionsResponseBody.ErrorCode", int16_value_assertion.IsEqual(expectedErrorCode),
	)
	return a
}

func (a *ApiVersionsResponseAssertion) GetValueAssertionCollection() value_assertion.ValueAssertionCollection {
	return a.valueAssertions
}

// Composite Assertions

func (a *ApiVersionsResponseAssertion) WithAPIKey(expectedAPIKey, expectedMinVersion, expectedMaxVersion int16) *ApiVersionsResponseAssertion {
	a.expectedAPIKey = expectedAPIKey
	a.expectedMinVersion = expectedMinVersion
	a.expectedMaxVersion = expectedMaxVersion
	return a
}

func (a *ApiVersionsResponseAssertion) RunCompositeAssertions(response kafkaapi.ApiVersionsResponse, logger *logger.Logger) error {

	// Check that at least one entry has API key = 18 (API_VERSIONS)
	foundAPIKey18 := false
	var apiKey18Entry kafkaapi.ApiKeyEntry

	for _, apiKeyEntry := range response.Body.ApiKeys {
		if apiKeyEntry.ApiKey.Value == a.expectedAPIKey {
			foundAPIKey18 = true
			apiKey18Entry = apiKeyEntry
			break
		}
	}

	if !foundAPIKey18 {
		apiKeyName := apiKeyNames[a.expectedAPIKey]
		return fmt.Errorf("Expected ApiKeys array to include API key %d (%s)", a.expectedAPIKey, apiKeyName)
	}

	// Check version rules from legacy code
	apiKeyName := apiKeyNames[a.expectedAPIKey]

	if apiKey18Entry.MinVersion.Value > a.expectedMaxVersion {
		return fmt.Errorf("Expected min version %v to be <= max version %v for %s", apiKey18Entry.MinVersion.Value, a.expectedMaxVersion, apiKeyName)
	}

	// anything above or equal to expected minVersion is fine
	if apiKey18Entry.MinVersion.Value < a.expectedMinVersion {
		return fmt.Errorf("Expected API version %v to be supported for %s, got %v", a.expectedMinVersion, apiKeyName, apiKey18Entry.MinVersion.Value)
	}
	logger.Successf("✔ MinVersion for %s is <= %v & >= %v", apiKeyName, a.expectedMaxVersion, a.expectedMinVersion)

	if apiKey18Entry.MaxVersion.Value < a.expectedMaxVersion {
		return fmt.Errorf("Expected API version %v to be supported for %s, got %v", a.expectedMaxVersion, apiKeyName, apiKey18Entry.MaxVersion.Value)
	}
	logger.Successf("✔ MaxVersion for %s is >= %v", apiKeyName, a.expectedMaxVersion)

	return nil
}
