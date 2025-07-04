package internal

import (
	"fmt"

	"github.com/codecrafters-io/kafka-tester/internal/kafka_executable"
	"github.com/codecrafters-io/kafka-tester/protocol"
	kafkaapi "github.com/codecrafters-io/kafka-tester/protocol/api"
	"github.com/codecrafters-io/kafka-tester/protocol/serializer"
	"github.com/codecrafters-io/tester-utils/logger"
	"github.com/codecrafters-io/tester-utils/test_case_harness"
)

func testAPIVersion(stageHarness *test_case_harness.TestCaseHarness) error {
	b := kafka_executable.NewKafkaExecutable(stageHarness)
	err := serializer.GenerateLogDirs(logger.GetQuietLogger(""), true)
	if err != nil {
		return err
	}

	stageLogger := stageHarness.Logger
	if err := b.Run(); err != nil {
		return err
	}

	correlationId := getRandomCorrelationId()

	broker := protocol.NewBroker("localhost:9092")
	if err := broker.ConnectWithRetries(b, stageLogger); err != nil {
		return err
	}
	defer func(broker *protocol.Broker) {
		_ = broker.Close()
	}(broker)

	request := kafkaapi.ApiVersionsRequest{
		Header: kafkaapi.RequestHeader{
			ApiKey:        18,
			ApiVersion:    4,
			CorrelationId: correlationId,
			ClientId:      "kafka-cli",
		},
		Body: kafkaapi.ApiVersionsRequestBody{
			Version:               4,
			ClientSoftwareName:    "kafka-cli",
			ClientSoftwareVersion: "0.1",
		},
	}

	message := kafkaapi.EncodeApiVersionsRequest(&request)
	stageLogger.Infof("Sending \"ApiVersions\" (version: %v) request (Correlation id: %v)", request.Header.ApiVersion, request.Header.CorrelationId)
	stageLogger.Debugf("Hexdump of sent \"ApiVersions\" request: \n%v\n", GetFormattedHexdump(message))

	response, err := broker.SendAndReceive(message, "ApiVersions", stageLogger)
	if err != nil {
		return err
	}

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
		return fmt.Errorf("Expected API keys array to be non-empty")
	}
	stageLogger.Successf("✓ API keys array is non-empty")

	foundAPIKey := false
	MAX_VERSION := int16(4)
	for _, apiVersionKey := range responseBody.ApiKeys {
		if apiVersionKey.ApiKey == 18 {
			foundAPIKey = true
			if apiVersionKey.MaxVersion >= MAX_VERSION {
				stageLogger.Successf("✓ API version %v is supported for API_VERSIONS", MAX_VERSION)
			} else {
				return fmt.Errorf("Expected API version %v to be supported for API_VERSIONS, got %v", MAX_VERSION, apiVersionKey.MaxVersion)
			}
		}
	}

	if !foundAPIKey {
		return fmt.Errorf("Expected APIVersionsResponseKey to be present for API key 18 (API_VERSIONS)")
	}

	return nil
}
