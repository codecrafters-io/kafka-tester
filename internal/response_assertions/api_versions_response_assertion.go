package response_assertions

import (
	"fmt"
	"regexp"

	"github.com/codecrafters-io/kafka-tester/internal/field"
	int16_assertions "github.com/codecrafters-io/kafka-tester/internal/value_assertions/int16"
	int32_assertions "github.com/codecrafters-io/kafka-tester/internal/value_assertions/int32"
	"github.com/codecrafters-io/kafka-tester/protocol/kafkaapi"
	"github.com/codecrafters-io/kafka-tester/protocol/utils"
	"github.com/codecrafters-io/kafka-tester/protocol/value"
	"github.com/codecrafters-io/tester-utils/logger"
)

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

func (a *ApiVersionsResponseAssertion) ExpectCorrelationId(expectedCorrelationID int32) *ApiVersionsResponseAssertion {
	a.expectedCorrelationID = expectedCorrelationID
	return a
}

func (a *ApiVersionsResponseAssertion) ExpectErrorCode(expectedErrorCode int16) *ApiVersionsResponseAssertion {
	a.expectedErrorCode = expectedErrorCode
	return a
}

func (a *ApiVersionsResponseAssertion) ExpectApiKeyEntry(expectedApiKey int16, expectedMinVersion int16, expectedMaxVersion int16) *ApiVersionsResponseAssertion {
	a.expectedApiKeys = append(a.expectedApiKeys, kafkaapi.ApiKeyEntry{
		ApiKey:     value.Int16{Value: expectedApiKey},
		MinVersion: value.Int16{Value: expectedMinVersion},
		MaxVersion: value.Int16{Value: expectedMaxVersion},
	})

	return a
}

func (a *ApiVersionsResponseAssertion) AssertSingleField(field field.Field) error {
	if field.Path.String() == "ApiVersionsResponse.Header.CorrelationID" {
		return int32_assertions.IsEqualTo(a.expectedCorrelationID, field.Value)
	}

	if field.Path.String() == "ApiVersionsResponse.Body.ErrorCode" {
		return int16_assertions.IsEqualTo(a.expectedErrorCode, field.Value)
	}

	if field.Path.String() == "ApiVersionsResponse.Body.ThrottleTimeMs" {
		// We don't validate ThrottleTimeMs
		return nil
	}

	if field.Path.String() == "ApiVersionsResponse.Body.ApiKeys.Length" {
		// TODO: See if we can assert this to be > 1?
		return nil
	}

	if regexp.MustCompile(`ApiVersionsResponse\.Body\.ApiKeys\.ApiKeys\[\d+\]\.APIKey`).MatchString(field.Path.String()) {
		// TODO: Assert this to be a positive number?
		return nil
	}

	if regexp.MustCompile(`ApiVersionsResponse\.Body\.ApiKeys\.ApiKeys\[\d+\]\.MinVersion`).MatchString(field.Path.String()) {
		// TODO: Assert this to be a positive number?
		return nil
	}

	if regexp.MustCompile(`ApiVersionsResponse\.Body\.ApiKeys\.ApiKeys\[\d+\]\.MaxVersion`).MatchString(field.Path.String()) {
		// TODO: Assert this to be a positive number?
		return nil
	}

	// This ensures that we're handling ALL possible fields
	panic("CodeCrafters Internal Error: Unhandled field path: " + field.Path.String())
}

func (a *ApiVersionsResponseAssertion) AssertAcrossFields(response kafkaapi.ApiVersionsResponse, logger *logger.Logger) error {
	// First, log success messages from single-field assertions that are worth highlighting
	logger.Successf("✓ CorrelationID: %d", a.expectedCorrelationID)
	logger.Successf("✓ ErrorCode: %d (%s)", a.expectedErrorCode, utils.ErrorCodeToName(a.expectedErrorCode))

	if len(response.Body.ApiKeys) < len(a.expectedApiKeys) {
		return fmt.Errorf("Expected ApiKeys array to include atleast %d keys, got %d", len(a.expectedApiKeys), len(response.Body.ApiKeys))
	}

	logger.Successf("✓ API keys array length: %d", len(response.Body.ApiKeys))

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
			apiKeyName := utils.APIKeyToName(expectedApiKey.ApiKey.Value)
			return fmt.Errorf("Expected ApiKeys array to include API key %d (%s)", expectedApiKey.ApiKey.Value, apiKeyName)
		}

		apiKeyName := utils.APIKeyToName(expectedApiKey.ApiKey.Value)

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
