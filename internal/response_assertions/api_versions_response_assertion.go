package response_assertions

import (
	"fmt"

	int16_assertions "github.com/codecrafters-io/kafka-tester/internal/value_assertions/int16"
	int32_assertions "github.com/codecrafters-io/kafka-tester/internal/value_assertions/int32"
	"github.com/codecrafters-io/kafka-tester/protocol/kafkaapi"
	"github.com/codecrafters-io/kafka-tester/protocol/value"
	"github.com/codecrafters-io/tester-utils/logger"
)

// TODO[PaulRefactor]: We already have APIKeyToName somewhere, see if that can be used here?
var apiKeyNames = map[int16]string{
	1:  "FETCH",
	18: "API_VERSIONS",
	75: "DESCRIBE_TOPIC_PARTITIONS",
}

type ApiVersionsResponseAssertion struct {
	expectedCorrelationID int32
	expectedErrorCode     int16
	expectedApiKeys       []kafkaapi.ApiKeyEntry
}

func NewApiVersionsResponseAssertion() *ApiVersionsResponseAssertion {
	return &ApiVersionsResponseAssertion{
		expectedCorrelationID: -1,
		expectedErrorCode:     0,
		expectedApiKeys:       []kafkaapi.ApiKeyEntry{},
	}
}

func (a *ApiVersionsResponseAssertion) WithCorrelationId(expectedCorrelationID int32) *ApiVersionsResponseAssertion {
	a.expectedCorrelationID = expectedCorrelationID
	return a
}

func (a *ApiVersionsResponseAssertion) WithErrorCode(expectedErrorCode int16) *ApiVersionsResponseAssertion {
	a.expectedErrorCode = expectedErrorCode
	return a
}

func (a *ApiVersionsResponseAssertion) WithApiKeyEntry(expectedApiKey int16, expectedMinVersion int16, expectedMaxVersion int16) *ApiVersionsResponseAssertion {
	a.expectedApiKeys = append(a.expectedApiKeys, kafkaapi.ApiKeyEntry{
		ApiKey:     value.Int16{Value: expectedApiKey},
		MinVersion: value.Int16{Value: expectedMinVersion},
		MaxVersion: value.Int16{Value: expectedMaxVersion},
	})

	return a
}

func (a *ApiVersionsResponseAssertion) AssertDecodedValue(locator string, decodedValue value.KafkaProtocolValue) error {
	if locator == "ApiVersionsResponse.Header.CorrelationID" {
		return int32_assertions.IsEqualTo(a.expectedCorrelationID, decodedValue)
	}

	if locator == "ApiVersionsResponse.Body.ErrorCode" {
		return int16_assertions.IsEqualTo(a.expectedErrorCode, decodedValue)
	}

	if locator == "ApiVersionsResponse.Body.ThrottleTimeMs" {
		// We don't validate ThrottleTimeMs
		return nil
	}

	// TODO: See what basic validations we can do?
	if locator == "ApiVersionsResponse.Body.ApiKeys.Length" {
		// Ignore for now
		return nil
	}

	// TODO[PaulRefactor]: Add assertions for ApiKeys[].ApiKey, ApiKeys[].MinVersion, ApiKeys[].MaxVersion

	// This ensures that we're handling ALL possible locators
	panic("CodeCrafters Internal Error: Unhandled locator: " + locator)
}

func (a *ApiVersionsResponseAssertion) RunCompositeAssertions(response kafkaapi.ApiVersionsResponse, logger *logger.Logger) error {
	for _, expectedApiKey := range a.expectedApiKeys {
		foundAPIKey := false
		var actualApiKeyEntry kafkaapi.ApiKeyEntry

		for _, apiKeyEntry := range response.Body.ApiKeys {
			if apiKeyEntry.ApiKey.Value == expectedApiKey.ApiKey.Value {
				foundAPIKey = true
				actualApiKeyEntry = apiKeyEntry
				break
			}
		}

		if !foundAPIKey {
			apiKeyName := apiKeyNames[expectedApiKey.ApiKey.Value]
			return fmt.Errorf("Expected ApiKeys array to include API key %d (%s)", expectedApiKey.ApiKey.Value, apiKeyName)
		}

		apiKeyName := apiKeyNames[expectedApiKey.ApiKey.Value]

		if actualApiKeyEntry.MinVersion.Value > expectedApiKey.MaxVersion.Value {
			return fmt.Errorf("Expected min version %v to be <= max version %v for %s", actualApiKeyEntry.MinVersion.Value, expectedApiKey.MaxVersion.Value, apiKeyName)
		}

		// anything below or equal to expected minVersion is fine
		if actualApiKeyEntry.MinVersion.Value < expectedApiKey.MinVersion.Value {
			return fmt.Errorf("Expected API version %v to be supported for %s, got %v", expectedApiKey.MinVersion.Value, apiKeyName, actualApiKeyEntry.MinVersion.Value)
		}

		logger.Successf("✔ MinVersion for %s is <= %v & >= %v", apiKeyName, expectedApiKey.MaxVersion.Value, expectedApiKey.MinVersion.Value)

		// anything above or equal to expected maxVersion is fine
		if actualApiKeyEntry.MaxVersion.Value < expectedApiKey.MaxVersion.Value {
			return fmt.Errorf("Expected API version %v to be supported for %s, got %v", expectedApiKey.MaxVersion.Value, apiKeyName, actualApiKeyEntry.MaxVersion.Value)
		}

		logger.Successf("✔ MaxVersion for %s is >= %v", apiKeyName, expectedApiKey.MaxVersion.Value)
	}

	return nil
}
