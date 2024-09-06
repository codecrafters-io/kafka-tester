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

func testAPIVersionwFetchKey(stageHarness *test_case_harness.TestCaseHarness) error {
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
	logger.Successf("✓ Correlation ID: %v", responseHeader.CorrelationId)

	if responseBody.ErrorCode != 0 {
		return fmt.Errorf("expected error code to be 0, got %v", responseBody.ErrorCode)
	}
	logger.Successf("✓ Error code: 0 (NO_ERROR)")

	MAX_VERSION := int16(16)
	for _, apiVersionKey := range responseBody.ApiKeys {
		if apiVersionKey.ApiKey == 1 {
			if apiVersionKey.MaxVersion >= MAX_VERSION {
				logger.Successf("✓ API version %v is supported for FETCH", MAX_VERSION)
			} else {
				return fmt.Errorf("expected API version %v to be supported for FETCH, got %v", MAX_VERSION, apiVersionKey.MaxVersion)
			}
		}
	}

	return nil
}
