package internal

import (
	"fmt"

	"github.com/codecrafters-io/tester-utils/logger"

	"github.com/codecrafters-io/kafka-tester/internal/kafka_executable"
	kafkaapi "github.com/codecrafters-io/kafka-tester/protocol/api"
	"github.com/codecrafters-io/kafka-tester/protocol/builder"
	"github.com/codecrafters-io/kafka-tester/protocol/kafka_client"
	"github.com/codecrafters-io/kafka-tester/protocol/serializer"
	"github.com/codecrafters-io/kafka-tester/protocol/utils"
	"github.com/codecrafters-io/tester-utils/random"
	"github.com/codecrafters-io/tester-utils/test_case_harness"
)

func testSequentialRequests(stageHarness *test_case_harness.TestCaseHarness) error {
	b := kafka_executable.NewKafkaExecutable(stageHarness)
	err := serializer.GenerateLogDirs(logger.GetQuietLogger(""), true)
	if err != nil {
		return err
	}

	stageLogger := stageHarness.Logger
	if err := b.Run(); err != nil {
		return err
	}

	client := kafka_client.NewClient("localhost:9092")
	if err := client.ConnectWithRetries(b, stageLogger); err != nil {
		return err
	}
	defer func(client *kafka_client.Client) {
		_ = client.Close()
	}(client)

	requestCount := random.RandomInt(2, 5)
	for i := 0; i < requestCount; i++ {
		correlationId := getRandomCorrelationId()
		request := kafkaapi.ApiVersionsRequest{
			Header: builder.NewRequestHeaderBuilder().BuildApiVersionsRequestHeader(correlationId),
			Body: kafkaapi.ApiVersionsRequestBody{
				Version:               4,
				ClientSoftwareName:    "kafka-cli",
				ClientSoftwareVersion: "0.1",
			},
		}

		message := request.Encode()
		stageLogger.Infof("Sending request %v of %v: \"ApiVersions\" (version: %v) request (Correlation id: %v)", i+1, requestCount, request.Header.ApiVersion, request.Header.CorrelationId)
		stageLogger.Debugf("Hexdump of sent \"ApiVersions\" request: \n%v\n", utils.GetFormattedHexdump(message))

		response, err := client.SendAndReceive(request, stageLogger)
		if err != nil {
			return err
		}
		stageLogger.Debugf("Hexdump of received \"ApiVersions\" response: \n%v\n", utils.GetFormattedHexdump(response.RawBytes))

		responseHeader, responseBody, err := kafkaapi.DecodeApiVersionsHeaderAndResponse(response.Payload, 3, stageLogger)
		if err != nil {
			return err
		}

		if responseHeader.CorrelationId != correlationId {
			return fmt.Errorf("Expected Correlation ID to be %v, got %v", correlationId, responseHeader.CorrelationId)
		}
		stageLogger.Successf("✓ Correlation ID: %v", responseHeader.CorrelationId)

		if responseBody.ErrorCode != 0 {
			return fmt.Errorf("Expected Error code to be 0, got %v", responseBody.ErrorCode)
		}
		stageLogger.Successf("✓ Error code: 0 (NO_ERROR)")

		if len(responseBody.ApiKeys) < 1 {
			return fmt.Errorf("Expected API keys array to include atleast 1 key (API_VERSIONS), got %v", len(responseBody.ApiKeys))
		}
		stageLogger.Successf("✓ API keys array is non-empty")

		foundAPIKey := false
		MAX_VERSION_APIVERSION := int16(4)
		for _, apiVersionKey := range responseBody.ApiKeys {
			if apiVersionKey.ApiKey == 18 {
				foundAPIKey = true
				if apiVersionKey.MaxVersion >= MAX_VERSION_APIVERSION {
					stageLogger.Successf("✓ API version %v is supported for API_VERSIONS", MAX_VERSION_APIVERSION)
				} else {
					return fmt.Errorf("Expected API version %v to be supported for API_VERSIONS, got %v", MAX_VERSION_APIVERSION, apiVersionKey.MaxVersion)
				}
			}
		}

		if !foundAPIKey {
			return fmt.Errorf("Expected APIVersionsResponseKey to be present for API key 18 (API_VERSIONS)")
		}

		stageLogger.Successf("✓ Test %v of %v: Passed", i+1, requestCount)
	}

	return nil
}
