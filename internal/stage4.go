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

func testAPIVersionErrorCase(stageHarness *test_case_harness.TestCaseHarness) error {
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
			ApiVersion:    -1,
			CorrelationId: correlationId,
			ClientId:      "kafka-cli",
		},
		Body: kafkaapi.ApiVersionsRequestBody{
			Version:               -1,
			ClientSoftwareName:    "kafka-cli",
			ClientSoftwareVersion: "0.1",
		},
	}

	message := kafkaapi.EncodeApiVersionsRequest(&request)

	response, err := broker.SendAndReceive(message)
	if err != nil {
		return err
	}

	responseHeader, responseBody, _ := kafkaapi.DecodeApiVersionsHeaderAndResponse(response, -1)
	// We can encounter an error here, but we will soldier on

	// Necessary sanity checks, now that we don't check the error
	if responseHeader == nil {
		return fmt.Errorf("responseHeader is nil")
	}

	if responseHeader.CorrelationId != correlationId {
		return fmt.Errorf("expected correlationId to be %v, got %v", correlationId, responseHeader.CorrelationId)
	}
	logger.Successf("✓ correlationId: %v", responseHeader.CorrelationId)

	if responseBody.ErrorCode != 35 {
		return fmt.Errorf("expected error code to be 35, got %v", responseBody.ErrorCode)
	}
	logger.Successf("✓ ErrorCode: %v", responseBody.ErrorCode)

	return nil
}
