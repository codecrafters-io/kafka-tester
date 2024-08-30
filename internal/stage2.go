package internal

import (
	"math"

	"github.com/codecrafters-io/kafka-tester/internal/kafka_executable"
	"github.com/codecrafters-io/kafka-tester/protocol"
	kafkaapi "github.com/codecrafters-io/kafka-tester/protocol/api"
	"github.com/codecrafters-io/tester-utils/random"
	"github.com/codecrafters-io/tester-utils/test_case_harness"
)

func testHardcodedCorrelationId(stageHarness *test_case_harness.TestCaseHarness) error {
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

	responseHeader, err := kafkaapi.DecodeApiVersionsHeader(response, 3)
	if err != nil {
		return err
	}

	logger.Successf("✓ correlationId: %v", responseHeader.CorrelationId)

	return nil
}
