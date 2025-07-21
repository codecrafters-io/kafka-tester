package internal

import (
	"fmt"
	"math"

	"github.com/codecrafters-io/kafka-tester/internal/kafka_executable"
	kafkaapi "github.com/codecrafters-io/kafka-tester/protocol/api"
	"github.com/codecrafters-io/kafka-tester/protocol/builder"
	"github.com/codecrafters-io/kafka-tester/protocol/kafka_client"
	"github.com/codecrafters-io/kafka-tester/protocol/serializer"
	"github.com/codecrafters-io/kafka-tester/protocol/utils"
	"github.com/codecrafters-io/tester-utils/logger"
	"github.com/codecrafters-io/tester-utils/random"
	"github.com/codecrafters-io/tester-utils/test_case_harness"
)

func testConcurrentRequests(stageHarness *test_case_harness.TestCaseHarness) error {
	b := kafka_executable.NewKafkaExecutable(stageHarness)
	err := serializer.GenerateLogDirs(logger.GetQuietLogger(""), true)
	if err != nil {
		return err
	}

	stageLogger := stageHarness.Logger
	if err := b.Run(); err != nil {
		return err
	}

	clientCount := random.RandomInt(2, 4)
	clients := make([]*kafka_client.Client, clientCount)
	correlationIds := make([]int32, clientCount)

	for i := 0; i < clientCount; i++ {
		clients[i] = kafka_client.NewClient("localhost:9092")
		if err := clients[i].ConnectWithRetries(b, stageLogger); err != nil {
			return err
		}
	}

	defer func() {
		for _, client := range clients {
			if client != nil {
				_ = client.Close()
			}
		}
	}()

	for i, client := range clients {
		correlationIds[i] = int32(random.RandomInt(-math.MaxInt32, math.MaxInt32))
		request := kafkaapi.ApiVersionsRequest{
			Header: builder.NewRequestHeaderBuilder().BuildApiVersionsRequestHeader(correlationIds[i]),
			Body: kafkaapi.ApiVersionsRequestBody{
				Version:               4,
				ClientSoftwareName:    "kafka-cli",
				ClientSoftwareVersion: "0.1",
			},
		}

		message := kafkaapi.EncodeApiVersionsRequest(&request)
		stageLogger.Infof("Sending request %v of %v: \"ApiVersions\" (version: %v) request (Correlation id: %v)", i+1, clientCount, request.Header.ApiVersion, request.Header.CorrelationId)
		stageLogger.Debugf("Hexdump of sent \"ApiVersions\" request: \n%v\n", utils.GetFormattedHexdump(message))

		err := client.Send(message)
		if err != nil {
			return err
		}
	}

	for idx := range clients {
		j := len(clients) - idx - 1
		client := clients[j]

		response, err := client.Receive()
		if err != nil {
			return err
		}
		stageLogger.Debugf("Hexdump of received \"ApiVersions\" response: \n%v\n", utils.GetFormattedHexdump(response.RawBytes))

		responseHeader, responseBody, err := kafkaapi.DecodeApiVersionsHeaderAndResponse(response.Payload, 3, stageLogger)
		if err != nil {
			return err
		}

		if responseHeader.CorrelationId != correlationIds[j] {
			return fmt.Errorf("Expected Correlation ID to be %v, got %v", correlationIds[j], responseHeader.CorrelationId)
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

		stageLogger.Successf("✓ Test %v of %v: Passed", j+1, clientCount)
	}

	return nil
}
