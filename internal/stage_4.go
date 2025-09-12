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
	"github.com/codecrafters-io/kafka-tester/protocol/utils"
	"github.com/codecrafters-io/tester-utils/logger"
	"github.com/codecrafters-io/tester-utils/test_case_harness"
)

func testAPIVersionErrorCase(stageHarness *test_case_harness.TestCaseHarness) error {
	stageLogger := stageHarness.Logger

	if err := kafka_files_generator.NewFilesHandler(logger.GetQuietLogger("")).GenerateServerConfiguration(); err != nil {
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
	expectedErrorCode := 35

	request := builder.NewApiVersionsRequestBuilder().
		WithCorrelationId(correlationId).WithVersion(apiVersion).Build()

	response, err := client.SendAndReceive(request, stageLogger)
	if err != nil {
		return err
	}

	assertion := response_assertions.NewApiVersionsResponseAssertion().
		ExpectCorrelationId(correlationId).ExpectErrorCode(int16(expectedErrorCode))

	_, err = response_asserter.ResponseAsserter[kafkaapi.ApiVersionsResponse]{
		DecodeFunc: response_decoders.DecodeApiVersionsResponseUpToErrorCode,
		Assertion:  assertion,
		Logger:     stageLogger,
	}.DecodeAndAssertSingleFields(response.Payload)

	if err != nil {
		return err
	}

	stageLogger.Successf("✓ CorrelationID: %d", correlationId)
	stageLogger.Successf("✓ ErrorCode: %d (%s)", expectedErrorCode, utils.ErrorCodeToName(int16(expectedErrorCode)))
	return nil
}
