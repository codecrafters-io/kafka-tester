package assertions

import (
	"fmt"

	"github.com/codecrafters-io/kafka-tester/protocol/kafkaapi"
	"github.com/codecrafters-io/tester-utils/logger"
)

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
	ActualValue   kafkaapi.ApiVersionsResponse
	ExpectedValue kafkaapi.ApiVersionsResponse
}

func NewApiVersionsResponseAssertion(actualValue kafkaapi.ApiVersionsResponse, expectedValue kafkaapi.ApiVersionsResponse) *ApiVersionsResponseAssertion {
	return &ApiVersionsResponseAssertion{
		ActualValue:   actualValue,
		ExpectedValue: expectedValue,
	}
}

func (a *ApiVersionsResponseAssertion) assertBody(logger *logger.Logger) error {
	expectedErrorCodeName, ok := errorCodes[int(a.ExpectedValue.Body.ErrorCode.Value)]

	if !ok {
		panic(fmt.Sprintf("CodeCrafters Internal Error: Expected %d to be in errorCodes map", a.ExpectedValue.Body.ErrorCode.Value))
	}

	if a.ActualValue.Body.ErrorCode.Value != a.ExpectedValue.Body.ErrorCode.Value {
		return fmt.Errorf("Expected ErrorCode to be %d (%s), got %d", a.ExpectedValue.Body.ErrorCode.Value, expectedErrorCodeName, a.ActualValue.Body.ErrorCode.Value)
	}

	logger.Successf("✓ ErrorCode: %d (%s)", a.ActualValue.Body.ErrorCode.Value, expectedErrorCodeName)

	if err := a.assertAPIKeysArray(logger); err != nil {
		return err
	}

	return nil
}

func (a *ApiVersionsResponseAssertion) assertAPIKeysArray(logger *logger.Logger) error {
	if len(a.ActualValue.Body.ApiKeys) < len(a.ExpectedValue.Body.ApiKeys) {
		return fmt.Errorf("Expected API keys array to include atleast %d keys, got %d", len(a.ExpectedValue.Body.ApiKeys), len(a.ActualValue.Body.ApiKeys))
	}

	logger.Successf("✓ API keys array length: %d", len(a.ActualValue.Body.ApiKeys))

	for _, expectedApiVersionKey := range a.ExpectedValue.Body.ApiKeys {
		found := false

		for _, actualApiVersionKey := range a.ActualValue.Body.ApiKeys {
			if actualApiVersionKey.ApiKey.Value == expectedApiVersionKey.ApiKey.Value {
				found = true

				if actualApiVersionKey.MinVersion.Value > expectedApiVersionKey.MaxVersion.Value {
					return fmt.Errorf("Expected min version %v to be < max version %v for %s", actualApiVersionKey.MinVersion.Value, expectedApiVersionKey.MaxVersion.Value, apiKeyNames[expectedApiVersionKey.ApiKey.Value])
				}

				// anything above or equal to expected minVersion is fine
				if actualApiVersionKey.MinVersion.Value < expectedApiVersionKey.MinVersion.Value {
					return fmt.Errorf("Expected API version %v to be supported for %s, got %v", expectedApiVersionKey.MinVersion.Value, apiKeyNames[expectedApiVersionKey.ApiKey.Value], actualApiVersionKey.MinVersion.Value)
				}

				logger.Successf("✓ MinVersion for %s is <= %v & >= %v", apiKeyNames[expectedApiVersionKey.ApiKey.Value], expectedApiVersionKey.MaxVersion.Value, expectedApiVersionKey.MinVersion.Value)

				if actualApiVersionKey.MaxVersion.Value < expectedApiVersionKey.MaxVersion.Value {
					return fmt.Errorf("Expected API version %v to be supported for %s, got %v", expectedApiVersionKey.MaxVersion.Value, apiKeyNames[expectedApiVersionKey.ApiKey.Value], actualApiVersionKey.MaxVersion.Value)
				}

				logger.Successf("✓ MaxVersion for %s is >= %v", apiKeyNames[expectedApiVersionKey.ApiKey.Value], expectedApiVersionKey.MaxVersion.Value)
			}
		}

		if !found {
			return fmt.Errorf("Expected APIVersionsResponseKey array to include API key %d (%s)", expectedApiVersionKey.ApiKey.Value, apiKeyNames[expectedApiVersionKey.ApiKey.Value])
		}
	}

	return nil
}

func (a *ApiVersionsResponseAssertion) Run(logger *logger.Logger) error {
	if err := NewResponseHeaderAssertion(a.ActualValue.Header, a.ExpectedValue.Header).Run(logger); err != nil {
		return err
	}

	if err := a.assertBody(logger); err != nil {
		return err
	}

	return nil
}
