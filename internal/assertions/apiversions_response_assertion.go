package assertions

import (
	"fmt"

	kafkaapi "github.com/codecrafters-io/kafka-tester/protocol/api"
	"github.com/codecrafters-io/tester-utils/logger"
)

type ApiVersionsResponseAssertion struct {
	ActualValue   kafkaapi.ApiVersionsResponseBody
	ExpectedValue kafkaapi.ApiVersionsResponseBody
}

func NewApiVersionsResponseAssertion(actualValue kafkaapi.ApiVersionsResponseBody, expectedValue kafkaapi.ApiVersionsResponseBody) ApiVersionsResponseAssertion {
	return ApiVersionsResponseAssertion{ActualValue: actualValue, ExpectedValue: expectedValue}
}

var apiKeyNames = map[int16]string{
	1:  "FETCH",
	18: "API_VERSIONS",
	75: "DESCRIBE_TOPIC_PARTITIONS",
}

var errorCodes = map[int]string{
	0:   "NO_ERROR",
	3:   "UNKNOWN_TOPIC_OR_PARTITION",
	35:  "UNSUPPORTED_VERSION",
	100: "UNKNOWN_TOPIC_ID",
}

type ApiVersionsResponseAssertion struct {
	ActualValue   kafkaapi.ApiVersionsResponseBody
	ExpectedValue kafkaapi.ApiVersionsResponseBody
}

func NewApiVersionsResponseAssertion(actualValue kafkaapi.ApiVersionsResponseBody, expectedValue kafkaapi.ApiVersionsResponseBody) *ApiVersionsResponseAssertion {
	return &ApiVersionsResponseAssertion{
		ActualValue:   actualValue,
		ExpectedValue: expectedValue,
	}
}

func (a *ApiVersionsResponseAssertion) assertErrorCode(logger *logger.Logger) error {
	expectedErrorCodeName, ok := errorCodes[int(a.ExpectedValue.ErrorCode)]
	if !ok {
		panic(fmt.Sprintf("CodeCrafters Internal Error: Expected %d to be in errorCodes map", a.ExpectedValue.ErrorCode))
	}

	if a.ActualValue.ErrorCode != a.ExpectedValue.ErrorCode {
		return fmt.Errorf("Expected ErrorCode to be %d (%s), got %d", a.ExpectedValue.ErrorCode, expectedErrorCodeName, a.ActualValue.ErrorCode)
	}

	logger.Successf("✓ Error code: %d (%s)", a.ActualValue.ErrorCode, expectedErrorCodeName)

	return nil
}

func (a *ApiVersionsResponseAssertion) assertAPIKeysArray(logger *logger.Logger) error {
	if len(a.ActualValue.ApiKeys) < len(a.ExpectedValue.ApiKeys) {
		return fmt.Errorf("Expected API keys array to include atleast %d keys, got %d", len(a.ExpectedValue.ApiKeys), len(a.ActualValue.ApiKeys))
	}
	logger.Successf("✓ API keys array length: %d", len(a.ActualValue.ApiKeys))

	for _, expectedApiVersionKey := range a.ExpectedValue.ApiKeys {
		found := false

		for _, actualApiVersionKey := range a.ActualValue.ApiKeys {
			if actualApiVersionKey.ApiKey == expectedApiVersionKey.ApiKey {
				found = true
				if actualApiVersionKey.MinVersion > expectedApiVersionKey.MaxVersion {
					return fmt.Errorf("Expected min version %v to be < max version %v for %s", actualApiVersionKey.MinVersion, expectedApiVersionKey.MaxVersion, apiKeyNames[expectedApiVersionKey.ApiKey])
				}

				// anything above or equal to expected minVersion is fine
				if actualApiVersionKey.MinVersion < expectedApiVersionKey.MinVersion {
					return fmt.Errorf("Expected API version %v to be supported for %s, got %v", expectedApiVersionKey.MinVersion, apiKeyNames[expectedApiVersionKey.ApiKey], actualApiVersionKey.MinVersion)
				}
				logger.Successf("✓ MinVersion for %s is <= %v & >= %v", apiKeyNames[expectedApiVersionKey.ApiKey], expectedApiVersionKey.MaxVersion, expectedApiVersionKey.MinVersion)

				if actualApiVersionKey.MaxVersion < expectedApiVersionKey.MaxVersion {
					return fmt.Errorf("Expected API version %v to be supported for %s, got %v", expectedApiVersionKey.MaxVersion, apiKeyNames[expectedApiVersionKey.ApiKey], actualApiVersionKey.MaxVersion)
				}
				logger.Successf("✓ MaxVersion for %s is >= %v", apiKeyNames[expectedApiVersionKey.ApiKey], expectedApiVersionKey.MaxVersion)
			}
		}

		if !found {
			return fmt.Errorf("Expected APIVersionsResponseKey array to include API key %d (%s)", expectedApiVersionKey.ApiKey, apiKeyNames[expectedApiVersionKey.ApiKey])
		}
	}

	return nil
}

func (a *ApiVersionsResponseAssertion) Run(logger *logger.Logger) error {
	if err := a.assertErrorCode(logger); err != nil {
		return err
	}

	if err := a.assertAPIKeysArray(logger); err != nil {
		return err
	}

	return nil
}
