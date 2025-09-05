package internal

import (
	"github.com/codecrafters-io/kafka-tester/internal/assertions"
	"github.com/codecrafters-io/kafka-tester/internal/kafka_executable"
	"github.com/codecrafters-io/kafka-tester/protocol/builder"
	"github.com/codecrafters-io/kafka-tester/protocol/kafka_client"
	"github.com/codecrafters-io/kafka-tester/protocol/legacy_serializer"
	"github.com/codecrafters-io/tester-utils/logger"
	"github.com/codecrafters-io/tester-utils/test_case_harness"
)

func testAPIVersion(stageHarness *test_case_harness.TestCaseHarness) error {
	stageLogger := stageHarness.Logger

	if err := legacy_serializer.GenerateLogDirs(logger.GetQuietLogger(""), true); err != nil {
		return err
	}

	b := kafka_executable.NewKafkaExecutable(stageHarness)

	if err := b.Run(); err != nil {
		return err
	}

	client := kafka_client.NewClient("localhost:9092")

	if err := client.ConnectWithRetries(b, stageLogger); err != nil {
		return err
	}

	defer client.Close()

	correlationId := getRandomCorrelationId()
	request := builder.NewApiVersionsRequestBuilder().WithCorrelationId(correlationId).Build()
	rawResponse, err := client.SendAndReceive(request, stageLogger)

	if err != nil {
		return err
	}

	actualResponse := builder.NewApiVersionsResponseBuilder().BuildEmpty()

	if err := actualResponse.Decode(rawResponse.Payload, stageLogger); err != nil {
		return err
	}

	expectedApiVersionResponse := builder.NewApiVersionsResponseBuilder().
		AddApiKeyEntry(18, 0, 4).
		WithCorrelationId(correlationId).
		Build()

	if err = assertions.NewApiVersionsResponseAssertion(actualResponse, expectedApiVersionResponse).Run(stageLogger); err != nil {
		return err
	}

	return nil
}
