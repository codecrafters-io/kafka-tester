package internal

import (
	"github.com/codecrafters-io/kafka-tester/internal/kafka_executable"
	"github.com/codecrafters-io/kafka-tester/internal/response_asserter"
	"github.com/codecrafters-io/kafka-tester/internal/response_assertions"
	"github.com/codecrafters-io/kafka-tester/internal/response_decoders"
	"github.com/codecrafters-io/kafka-tester/protocol/builder"
	"github.com/codecrafters-io/kafka-tester/protocol/kafka_client"
	"github.com/codecrafters-io/kafka-tester/protocol/kafka_files_generator"
	"github.com/codecrafters-io/kafka-tester/protocol/kafkaapi"
	"github.com/codecrafters-io/tester-utils/logger"
	"github.com/codecrafters-io/tester-utils/random"
	"github.com/codecrafters-io/tester-utils/test_case_harness"
)

func testSequentialRequests(stageHarness *test_case_harness.TestCaseHarness) error {
	b := kafka_executable.NewKafkaExecutable(stageHarness)
	stageLogger := stageHarness.Logger

	if err := kafka_files_generator.NewFilesHandler(logger.GetQuietLogger("")).GenerateServerConfiguration(); err != nil {
		return err
	}

	if err := b.Run(); err != nil {
		return err
	}

	client := kafka_client.NewClient("localhost:9092")

	if err := client.ConnectWithRetries(b, stageLogger); err != nil {
		return err
	}

	defer client.Close()
	requestCount := random.RandomInt(2, 5)
	correlationId := getRandomCorrelationId()

	for i := range requestCount {
		request := builder.NewApiVersionsRequestBuilder().WithCorrelationId(correlationId).Build()

		rawResponse, err := client.SendAndReceive(request, stageLogger)
		if err != nil {
			return err
		}

		assertion := response_assertions.NewApiVersionsResponseAssertion().
			ExpectCorrelationId(correlationId).
			ExpectErrorCode(0).
			ExpectApiKeyEntry(18, 0, 4)

		_, err = response_asserter.ResponseAsserter[kafkaapi.ApiVersionsResponse]{
			DecodeFunc: response_decoders.DecodeApiVersionsResponse,
			Assertion:  assertion,
			Logger:     stageLogger,
		}.DecodeAndAssert(rawResponse.Payload)

		if err != nil {
			return err
		}

		stageLogger.Successf("✓ Test %v of %v: Passed", i+1, requestCount)
	}

	return nil
}
