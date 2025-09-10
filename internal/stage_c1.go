package internal

import (
	"github.com/codecrafters-io/tester-utils/logger"

	"github.com/codecrafters-io/kafka-tester/internal/kafka_executable"
	"github.com/codecrafters-io/kafka-tester/internal/legacy_assertions"
	"github.com/codecrafters-io/kafka-tester/protocol/kafka_files_handler"
	"github.com/codecrafters-io/kafka-tester/protocol/legacy_builder"
	"github.com/codecrafters-io/kafka-tester/protocol/legacy_kafka_client"
	"github.com/codecrafters-io/tester-utils/random"
	"github.com/codecrafters-io/tester-utils/test_case_harness"
)

func testSequentialRequests(stageHarness *test_case_harness.TestCaseHarness) error {
	b := kafka_executable.NewKafkaExecutable(stageHarness)

	if err := kafka_files_handler.NewFilesHandler().GenerateServerConfiguration(logger.GetQuietLogger("")); err != nil {
		return err
	}

	stageLogger := stageHarness.Logger
	if err := b.Run(); err != nil {
		return err
	}

	client := legacy_kafka_client.NewClient("localhost:9092")
	if err := client.ConnectWithRetries(b, stageLogger); err != nil {
		return err
	}
	defer func(client *legacy_kafka_client.Client) {
		_ = client.Close()
	}(client)

	requestCount := random.RandomInt(2, 5)
	for i := range requestCount {
		correlationId := getRandomCorrelationId()
		request := legacy_builder.NewApiVersionsRequestBuilder().
			WithCorrelationId(correlationId).
			Build()

		rawResponse, err := client.SendAndReceive(request, stageLogger)
		if err != nil {
			return err
		}

		actualResponse := legacy_builder.NewApiVersionsResponseBuilder().BuildEmpty()
		if err := actualResponse.Decode(rawResponse.Payload, stageLogger); err != nil {
			return err
		}

		expectedApiVersionResponse := legacy_builder.NewApiVersionsResponseBuilder().
			AddApiKeyEntry(18, 0, 4).
			WithCorrelationId(correlationId).
			Build()

		if err = legacy_assertions.NewApiVersionsResponseAssertion(actualResponse, expectedApiVersionResponse).Run(stageLogger); err != nil {
			return err
		}

		stageLogger.Successf("✓ Test %v of %v: Passed", i+1, requestCount)
	}

	return nil
}
