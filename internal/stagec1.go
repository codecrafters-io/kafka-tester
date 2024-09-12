package internal

import (
	"fmt"
	"math"

	"github.com/codecrafters-io/kafka-tester/internal/kafka_executable"
	"github.com/codecrafters-io/kafka-tester/protocol"
	kafkaapi "github.com/codecrafters-io/kafka-tester/protocol/api"
	"github.com/codecrafters-io/tester-utils/random"
	"github.com/codecrafters-io/tester-utils/test_case_harness"
)

func testSequentialRequests(stageHarness *test_case_harness.TestCaseHarness) error {
	b := kafka_executable.NewKafkaExecutable(stageHarness)
	if err := b.Run(); err != nil {
		return err
	}

	logger := stageHarness.Logger

	broker := protocol.NewBroker("localhost:9092")
	if err := broker.ConnectWithRetries(b, logger); err != nil {
		return err
	}
	defer broker.Close()

	requestCount := random.RandomInt(2, 5)
	for i := 0; i < requestCount; i++ {
		correlationId := int32(random.RandomInt(-math.MaxInt32, math.MaxInt32))
		request := kafkaapi.ApiVersionsRequest{
			Header: kafkaapi.RequestHeader{
				ApiKey:        18,
				ApiVersion:    3,
				CorrelationId: correlationId,
				ClientId:      "kafka-cli",
			},
			Body: kafkaapi.ApiVersionsRequestBody{
				Version:               3,
				ClientSoftwareName:    "kafka-cli",
				ClientSoftwareVersion: "0.1",
			},
		}

		message := kafkaapi.EncodeApiVersionsRequest(&request)
		logger.Infof("Sending request %v of %v: \"ApiVersions\" (version: %v) request (Correlation id: %v)", i+1, requestCount, request.Header.ApiVersion, request.Header.CorrelationId)

		response, err := broker.SendAndReceive(message)
		if err != nil {
			return err
		}
		logger.Infof("Hexdump of sent \"ApiVersions\" request: \n%v\n", protocol.GetFormattedHexdump(message))
		logger.Infof("Hexdump of received \"ApiVersions\" response: \n%v\n", protocol.GetFormattedHexdump(response))

		responseHeader, responseBody, err := kafkaapi.DecodeApiVersionsHeaderAndResponse(response, 3, logger)
		if err != nil {
			return err
		}

		if responseHeader.CorrelationId != correlationId {
			return fmt.Errorf("Expected Correlation ID to be %v, got %v", correlationId, responseHeader.CorrelationId)
		}
		logger.Successf("✓ Correlation ID: %v", responseHeader.CorrelationId)

		if responseBody.ErrorCode != 0 {
			return fmt.Errorf("Expected Error code to be 0, got %v", responseBody.ErrorCode)
		}
		logger.Successf("✓ Error code: 0 (NO_ERROR)")

		if len(responseBody.ApiKeys) < 1 {
			return fmt.Errorf("Expected API keys array to include atleast 1 key (API_VERSIONS), got %v", len(responseBody.ApiKeys))
		}
		logger.Successf("✓ API keys array is non-empty")

		foundAPIKey := false
		MAX_VERSION_APIVERSION := int16(3)
		for _, apiVersionKey := range responseBody.ApiKeys {
			if apiVersionKey.ApiKey == 18 {
				foundAPIKey = true
				if apiVersionKey.MaxVersion >= MAX_VERSION_APIVERSION {
					logger.Successf("✓ API version %v is supported for API_VERSIONS", MAX_VERSION_APIVERSION)
				} else {
					return fmt.Errorf("Expected API version %v to be supported for API_VERSIONS, got %v", MAX_VERSION_APIVERSION, apiVersionKey.MaxVersion)
				}
			}
		}

		if !foundAPIKey {
			return fmt.Errorf("Expected APIVersionsResponseKey to be present for API key 18 (API_VERSIONS)")
		}

		logger.Successf("✓ Test %v of %v: Passed", i+1, requestCount)
	}

	return nil
}
