package assertions

import (
	"fmt"

	kafkaapi "github.com/codecrafters-io/kafka-tester/protocol/api"
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
	logger        *logger.Logger
	err           error

	// nil = don't assert this level
	// empty slice = assert all fields (default)
	// non-empty slice = assert with exclusions
	excludedBodyFields []string
}

func NewApiVersionsResponseAssertion(actualValue kafkaapi.ApiVersionsResponse, expectedValue kafkaapi.ApiVersionsResponse, logger *logger.Logger) *ApiVersionsResponseAssertion {
	return &ApiVersionsResponseAssertion{
		ActualValue:   actualValue,
		ExpectedValue: expectedValue,
		logger:        logger,
	}
}

// AssertBody asserts the contents of the response body
// Fields asserted by default: ErrorCode
func (a *ApiVersionsResponseAssertion) AssertBody() *ApiVersionsResponseAssertion {
	if a.err != nil {
		return a
	}

	if !Contains(a.excludedBodyFields, "ErrorCode") {
		if a.ActualValue.ErrorCode != a.ExpectedValue.ErrorCode {
			a.err = fmt.Errorf("Expected %s to be %d, got %d", "ErrorCode", a.ExpectedValue.ErrorCode, a.ActualValue.ErrorCode)
			return a
		}

		errorCodeName, ok := errorCodes[int(a.ActualValue.ErrorCode)]
		if !ok {
			errorCodeName = "UNKNOWN"
		}
		a.logger.Successf("✓ Error code: %d (%s)", a.ActualValue.ErrorCode, errorCodeName)
	}

	return a
}

// AssertAPIKeysArray asserts the API keys array in the response
// Fields asserted by default: ApiKeysArray.length, ApiKeyArrayElement.MinVersion, ApiKeyArrayElement.MaxVersion
func (a *ApiVersionsResponseAssertion) AssertAPIKeysArray() *ApiVersionsResponseAssertion {
	if a.err != nil {
		return a
	}

	if len(a.ActualValue.ApiKeys) < len(a.ExpectedValue.ApiKeys) {
		a.err = fmt.Errorf("Expected API keys array to include atleast %d keys, got %d", len(a.ExpectedValue.ApiKeys), len(a.ActualValue.ApiKeys))
		return a
	}
	a.logger.Successf("✓ API keys array length: %d", len(a.ActualValue.ApiKeys))

	for _, expectedApiVersionKey := range a.ExpectedValue.ApiKeys {
		found := false
		for _, actualApiVersionKey := range a.ActualValue.ApiKeys {
			if actualApiVersionKey.ApiKey == expectedApiVersionKey.ApiKey {
				found = true
				if actualApiVersionKey.MinVersion > expectedApiVersionKey.MaxVersion {
					a.err = fmt.Errorf("Expected min version %v to be < max version %v for %s", actualApiVersionKey.MinVersion, expectedApiVersionKey.MaxVersion, apiKeyNames[expectedApiVersionKey.ApiKey])
					return a
				}

				// anything above or equal to expected minVersion is fine
				if actualApiVersionKey.MinVersion < expectedApiVersionKey.MinVersion {
					a.err = fmt.Errorf("Expected API version %v to be supported for %s, got %v", expectedApiVersionKey.MaxVersion, apiKeyNames[expectedApiVersionKey.ApiKey], actualApiVersionKey.MaxVersion)
					return a
				}
				a.logger.Successf("✓ MinVersion for %s is <= %v & >= %v", apiKeyNames[expectedApiVersionKey.ApiKey], expectedApiVersionKey.MaxVersion, expectedApiVersionKey.MinVersion)

				if actualApiVersionKey.MaxVersion < expectedApiVersionKey.MaxVersion {
					a.err = fmt.Errorf("Expected API version %v to be supported for %s, got %v", expectedApiVersionKey.MaxVersion, apiKeyNames[expectedApiVersionKey.ApiKey], actualApiVersionKey.MaxVersion)
					return a
				}
				a.logger.Successf("✓ MaxVersion for %s is >= %v", apiKeyNames[expectedApiVersionKey.ApiKey], expectedApiVersionKey.MaxVersion)
			}
		}
		if !found {
			a.err = fmt.Errorf("Expected APIVersionsResponseKey array to include API key %d (%s)", expectedApiVersionKey.ApiKey, apiKeyNames[expectedApiVersionKey.ApiKey])
			return a
		}
	}

	return a
}

func (a *ApiVersionsResponseAssertion) Run() error {
	return a.err
}
