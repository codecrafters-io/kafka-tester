package internal

import (
	"github.com/codecrafters-io/kafka-tester/internal/kafka_executable"
	"github.com/codecrafters-io/kafka-tester/internal/response_asserter"
	"github.com/codecrafters-io/kafka-tester/internal/response_assertions"
	"github.com/codecrafters-io/kafka-tester/internal/response_decoders"
	"github.com/codecrafters-io/kafka-tester/protocol/builder"
	"github.com/codecrafters-io/kafka-tester/protocol/kafka_client"
	"github.com/codecrafters-io/kafka-tester/protocol/kafkaapi"
	"github.com/codecrafters-io/kafka-tester/protocol/legacy_serializer"
	"github.com/codecrafters-io/tester-utils/logger"
	"github.com/codecrafters-io/tester-utils/test_case_harness"
)

func testAPIVersionErrorCase(stageHarness *test_case_harness.TestCaseHarness) error {
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
	apiVersion := getInvalidAPIVersion()

	request := builder.NewApiVersionsRequestBuilder().
		WithCorrelationId(correlationId).WithVersion(apiVersion).Build()

	if err := client.Send(request, stageLogger); err != nil {
		return err
	}

	response, err := client.SendAndReceive(request, stageLogger)
	if err != nil {
		return err
	}

	assertion := response_assertions.NewApiVersionsResponseAssertion().
		ExpectCorrelationId(correlationId).ExpectErrorCode(35)

	_, err = response_asserter.ResponseAsserter[kafkaapi.ApiVersionsResponse]{
		DecodeFunc: response_decoders.DecodeApiVersionsResponseUpToErrorCode,
		Assertion:  assertion,
		Logger:     stageLogger,
	}.DecodeAndAssert(response.Payload)

	return err
}
