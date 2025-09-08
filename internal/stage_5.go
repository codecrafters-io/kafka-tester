package internal

import (
	"github.com/codecrafters-io/kafka-tester/internal/kafka_executable"
	"github.com/codecrafters-io/kafka-tester/internal/response_asserter"
	"github.com/codecrafters-io/kafka-tester/internal/response_assertions"
	"github.com/codecrafters-io/kafka-tester/internal/response_decoders"
	"github.com/codecrafters-io/kafka-tester/protocol/builder"
	"github.com/codecrafters-io/kafka-tester/protocol/kafka_client"
	"github.com/codecrafters-io/kafka-tester/protocol/kafkaapi"
	"github.com/codecrafters-io/kafka-tester/protocol/serializer_legacy"
	"github.com/codecrafters-io/tester-utils/logger"
	"github.com/codecrafters-io/tester-utils/test_case_harness"
)

func testAPIVersion(stageHarness *test_case_harness.TestCaseHarness) error {
	stageLogger := stageHarness.Logger

	if err := serializer_legacy.GenerateLogDirs(logger.GetQuietLogger(""), true); err != nil {
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

	response, err := client.SendAndReceive(request, stageLogger)
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
	}.DecodeAndAssert(response.Payload)

	return err
}
