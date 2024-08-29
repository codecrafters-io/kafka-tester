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

func testAPIVersion(stageHarness *test_case_harness.TestCaseHarness) error {
	b := kafka_executable.NewKafkaExecutable(stageHarness)
	if err := b.Run(); err != nil {
		return err
	}

	logger := stageHarness.Logger

	correlationId := int32(random.RandomInt(-math.MaxInt32, math.MaxInt32))

	broker := protocol.NewBroker("localhost:9092")
	if err := broker.ConnectWithRetries(b, logger); err != nil {
		return err
	}
	defer broker.Close()

	header := kafkaapi.RequestHeader{
		ApiKey:        18,
		ApiVersion:    3,
		CorrelationId: correlationId,
		ClientId:      "kafka-cli",
	}
	request := kafkaapi.ApiVersionsRequest{
		Version:               3,
		ClientSoftwareName:    "kafka-cli",
		ClientSoftwareVersion: "0.1",
	}

	message := kafkaapi.EncodeApiVersionsRequest(&header, &request)

	response, err := broker.SendAndReceive(message)
	if err != nil {
		return err
	}

	responseHeader, responseBody, err := kafkaapi.DecodeApiVersionsHeaderAndResponse(response, 3)
	if err != nil {
		return err
	}

	if responseHeader.CorrelationId != correlationId {
		return fmt.Errorf("expected correlationId to be %v, got %v", correlationId, responseHeader.CorrelationId)
	}
	logger.Successf("✓ correlationId: %v", responseHeader.CorrelationId)

	MAX_VERSION := int16(3)
	for _, apiVersionKey := range responseBody.ApiKeys {
		if apiVersionKey.ApiKey == 18 {
			if apiVersionKey.MaxVersion >= MAX_VERSION {
				logger.Successf("✓ API version %v is supported for API_VERSIONS", MAX_VERSION)
			} else {
				return fmt.Errorf("expected API version %v to be supported for API_VERSIONS, got %v", MAX_VERSION, apiVersionKey.MaxVersion)
			}
		}
	}

	return nil
}
