package internal

import (
	"fmt"

	"github.com/codecrafters-io/kafka-tester/internal/kafka_executable"
	"github.com/codecrafters-io/kafka-tester/protocol"
	kafkaapi "github.com/codecrafters-io/kafka-tester/protocol/api"
	"github.com/codecrafters-io/tester-utils/test_case_harness"
)

func testAPIVersionwDescribeTopicPartitions(stageHarness *test_case_harness.TestCaseHarness) error {
	b := kafka_executable.NewKafkaExecutable(stageHarness)
	if err := b.Run(); err != nil {
		return err
	}

	logger := stageHarness.Logger

	correlationId := getRandomCorrelationId()

	broker := protocol.NewBroker("localhost:9092")
	if err := broker.ConnectWithRetries(b, logger); err != nil {
		return err
	}
	defer broker.Close()

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
	logger.Infof("Sending \"ApiVersions\" (version: %v) request (Correlation id: %v)", request.Header.ApiVersion, request.Header.CorrelationId)

	response, err := broker.SendAndReceive(message)
	if err != nil {
		return err
	}
	logger.Debugf("Hexdump of sent \"ApiVersions\" request: \n%v\n", GetFormattedHexdump(message))
	logger.Debugf("Hexdump of received \"ApiVersions\" response: \n%v\n", GetFormattedHexdump(response))

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

	if len(responseBody.ApiKeys) < 3 {
		return fmt.Errorf("Expected API keys array to include atleast 2 keys (API_VERSIONS and FETCH), got %v", len(responseBody.ApiKeys))
	}
	logger.Successf("✓ API keys array is non-empty")

	foundAPIKey := 0
	MAX_VERSION_APIVERSION := int16(4)
	MAX_VERSION_FETCH := int16(16)
	MAX_VERSION_DESCRIBE_TOPIC_PARTITIONS := int16(0)

	for _, apiVersionKey := range responseBody.ApiKeys {
		if apiVersionKey.ApiKey == 1 {
			foundAPIKey += 1
			if apiVersionKey.MaxVersion >= MAX_VERSION_FETCH {
				logger.Successf("✓ API version %v is supported for FETCH", MAX_VERSION_FETCH)
			} else {
				return fmt.Errorf("Expected API version %v to be supported for FETCH, got %v", MAX_VERSION_FETCH, apiVersionKey.MaxVersion)
			}
		}
		if apiVersionKey.ApiKey == 18 {
			foundAPIKey += 1
			if apiVersionKey.MaxVersion >= MAX_VERSION_APIVERSION {
				logger.Successf("✓ API version %v is supported for API_VERSIONS", MAX_VERSION_APIVERSION)
			} else {
				return fmt.Errorf("Expected API version %v to be supported for API_VERSIONS, got %v", MAX_VERSION_APIVERSION, apiVersionKey.MaxVersion)
			}
		}
		if apiVersionKey.ApiKey == 75 {
			foundAPIKey += 1
			if apiVersionKey.MaxVersion >= MAX_VERSION_DESCRIBE_TOPIC_PARTITIONS {
				logger.Successf("✓ API version %v is supported for DESCRIBE_TOPIC_PARTITIONS", MAX_VERSION_DESCRIBE_TOPIC_PARTITIONS)
			} else {
				return fmt.Errorf("Expected API version %v to be supported for DESCRIBE_TOPIC_PARTITIONS, got %v", MAX_VERSION_DESCRIBE_TOPIC_PARTITIONS, apiVersionKey.MaxVersion)
			}
		}
	}

	if foundAPIKey != 3 {
		return fmt.Errorf("Expected APIVersionsResponseKey to be present for API key 18 (API_VERSIONS) & 1 (FETCH) & 75 (DESCRIBE_TOPIC_PARTITIONS)")
	}

	return nil
}