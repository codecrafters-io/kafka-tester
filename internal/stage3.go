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

func testCorrelationId(stageHarness *test_case_harness.TestCaseHarness) error {
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

	correlationId := int32(random.RandomInt(-math.MaxInt32, math.MaxInt32))

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

	message, err := kafkaapi.EncodeApiVersionsRequest(&header, &request)
	if err != nil {
		return err
	}

	response, err := broker.SendAndReceive(message)
	if err != nil {
		return err
	}
	responseHeader, err := kafkaapi.DecodeApiVersionsHeader(response, 3)
	if err != nil {
		return err
	}

	if responseHeader.CorrelationId != correlationId {
		return fmt.Errorf("correlationId mismatch: expected %v, got %v", correlationId, responseHeader.CorrelationId)
	}

	logger.Successf("✓ correlationId: %v", responseHeader.CorrelationId)

	return nil
}
