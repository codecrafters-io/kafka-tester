package internal

import (
	"github.com/codecrafters-io/kafka-tester/internal/assertions_legacy"
	"github.com/codecrafters-io/kafka-tester/internal/kafka_executable"
	"github.com/codecrafters-io/kafka-tester/protocol/builder_legacy"
	"github.com/codecrafters-io/kafka-tester/protocol/kafka_client_legacy"
	"github.com/codecrafters-io/kafka-tester/protocol/serializer_legacy"
	"github.com/codecrafters-io/tester-utils/logger"
	"github.com/codecrafters-io/tester-utils/test_case_harness"
)

func testAPIVersionWithDescribeTopicPartitions(stageHarness *test_case_harness.TestCaseHarness) error {
	b := kafka_executable.NewKafkaExecutable(stageHarness)
	err := serializer_legacy.GenerateLogDirs(logger.GetQuietLogger(""), true)
	if err != nil {
		return err
	}

	stageLogger := stageHarness.Logger
	if err := b.Run(); err != nil {
		return err
	}

	correlationId := getRandomCorrelationId()

	client := kafka_client_legacy.NewClient("localhost:9092")
	if err := client.ConnectWithRetries(b, stageLogger); err != nil {
		return err
	}
	defer client.Close()

	request := builder_legacy.NewApiVersionsRequestBuilder().
		WithCorrelationId(correlationId).
		Build()

	rawResponse, err := client.SendAndReceive(request, stageLogger)
	if err != nil {
		return err
	}

	actualResponse := builder_legacy.NewApiVersionsResponseBuilder().BuildEmpty()
	if err := actualResponse.Decode(rawResponse.Payload, stageLogger); err != nil {
		return err
	}

	expectedApiVersionResponse := builder_legacy.NewApiVersionsResponseBuilder().
		AddApiKeyEntry(18, 0, 4).
		AddApiKeyEntry(75, 0, 0).
		WithCorrelationId(correlationId).
		Build()

	if err = assertions_legacy.NewApiVersionsResponseAssertion(actualResponse, expectedApiVersionResponse).Run(stageLogger); err != nil {
		return err
	}

	return nil
}
