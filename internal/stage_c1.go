package internal

import (
	"github.com/codecrafters-io/tester-utils/logger"

	"github.com/codecrafters-io/kafka-tester/internal/assertions_legacy"
	"github.com/codecrafters-io/kafka-tester/internal/kafka_executable"
	"github.com/codecrafters-io/kafka-tester/protocol/builder"
	"github.com/codecrafters-io/kafka-tester/protocol/kafka_client"
	"github.com/codecrafters-io/kafka-tester/protocol/serializer"
	"github.com/codecrafters-io/tester-utils/random"
	"github.com/codecrafters-io/tester-utils/test_case_harness"
)

func testSequentialRequests(stageHarness *test_case_harness.TestCaseHarness) error {
	b := kafka_executable.NewKafkaExecutable(stageHarness)
	err := serializer.GenerateLogDirs(logger.GetQuietLogger(""), true)
	if err != nil {
		return err
	}

	stageLogger := stageHarness.Logger
	if err := b.Run(); err != nil {
		return err
	}

	client := kafka_client.NewClient("localhost:9092")
	if err := client.ConnectWithRetries(b, stageLogger); err != nil {
		return err
	}
	defer func(client *kafka_client.Client) {
		_ = client.Close()
	}(client)

	requestCount := random.RandomInt(2, 5)
	for i := range requestCount {
		correlationId := getRandomCorrelationId()
		request := builder.NewApiVersionsRequestBuilder().
			WithCorrelationId(correlationId).
			Build()

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

		if err = assertions_legacy.NewApiVersionsResponseAssertion(actualResponse, expectedApiVersionResponse).Run(stageLogger); err != nil {
			return err
		}

		stageLogger.Successf("✓ Test %v of %v: Passed", i+1, requestCount)
	}

	return nil
}
