package assertions

import (
	"fmt"

	kafkaapi "github.com/codecrafters-io/kafka-tester/protocol/api"
	"github.com/codecrafters-io/tester-utils/logger"
)

type ApiVersionsResponseAssertion struct {
	ActualValue   kafkaapi.ApiVersionsResponse
	ExpectedValue kafkaapi.ApiVersionsResponse
}

func NewApiVersionsResponseAssertion(actualValue kafkaapi.ApiVersionsResponse, expectedValue kafkaapi.ApiVersionsResponse) ApiVersionsResponseAssertion {
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

func (a ApiVersionsResponseAssertion) Evaluate(fields []string, AssertApiVersionsResponseKey bool, logger *logger.Logger) error {
	if Contains(fields, "ErrorCode") {
		if a.ActualValue.ErrorCode != a.ExpectedValue.ErrorCode {
			return fmt.Errorf("Expected %s to be %d, got %d", "ErrorCode", a.ExpectedValue.ErrorCode, a.ActualValue.ErrorCode)
		}

		errorCodeName, ok := errorCodes[int(a.ActualValue.ErrorCode)]
		if !ok {
			errorCodeName = "UNKNOWN"
		}
		logger.Successf("✓ Error code: %d (%s)", a.ActualValue.ErrorCode, errorCodeName)
	}

	if AssertApiVersionsResponseKey {
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
						return fmt.Errorf("Expected API version %v to be supported for %s, got %v", expectedApiVersionKey.MaxVersion, apiKeyNames[expectedApiVersionKey.ApiKey], actualApiVersionKey.MaxVersion)
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
	}

	return nil
}
