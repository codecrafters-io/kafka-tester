package internal

import (
	"github.com/codecrafters-io/kafka-tester/internal/instrumented_kafka_client"
	"github.com/codecrafters-io/kafka-tester/internal/kafka_executable"
	"github.com/codecrafters-io/kafka-tester/internal/request_encoders"
	"github.com/codecrafters-io/kafka-tester/internal/response_asserter"
	"github.com/codecrafters-io/kafka-tester/internal/response_assertions"
	"github.com/codecrafters-io/kafka-tester/internal/response_decoders"
	"github.com/codecrafters-io/kafka-tester/protocol/builder"
	"github.com/codecrafters-io/kafka-tester/protocol/kafka_files_generator"
	"github.com/codecrafters-io/kafka-tester/protocol/kafkaapi"
	"github.com/codecrafters-io/tester-utils/logger"
	"github.com/codecrafters-io/tester-utils/test_case_harness"
)

func testFetchWithNoTopics(stageHarness *test_case_harness.TestCaseHarness) error {
	stageLogger := stageHarness.Logger

	files_handler := kafka_files_generator.NewFilesHandler(logger.GetQuietLogger(""))

	// Generate __cluster_metadata-0/00000000000000000000.log (with no topics) for consistency with other stages
	files_handler.AddLogDirectoryGenerationConfig(kafka_files_generator.LogDirectoryGenerationConfig{
		TopicGenerationConfigList: []kafka_files_generator.TopicGenerationConfig{},
	})

	if err := files_handler.GenerateServerConfigAndLogDirs(); err != nil {
		return err
	}

	b := kafka_executable.NewKafkaExecutable(stageHarness)

	if err := b.Run(); err != nil {
		return err
	}

	client := instrumented_kafka_client.NewFromAddr("localhost:9092", stageLogger, "client")

	if err := client.ConnectWithRetries(b, stageLogger); err != nil {
		return err
	}

	defer client.Close()

	correlationId := getRandomCorrelationId()

	request := builder.NewFetchRequstWithEmptyTopicsBuilder().
		WithCorrelationId(correlationId).
		Build()

	rawResponse, err := client.SendAndReceive(
		request_encoders.Encode(request, stageLogger),
		request.Header.ApiKey.Value,
		stageLogger,
	)

	if err != nil {
		return err
	}

	assertion := response_assertions.NewFetchResponseAssertion().
		ExpectCorrelationId(correlationId).
		ExpectErrorCodeInBody(0).
		ExpectThrottleTimeMs(0)

	_, err = response_asserter.ResponseAsserter[kafkaapi.FetchResponse]{
		DecodeFunc: response_decoders.DecodeFetchResponse,
		Assertion:  assertion,
		Logger:     stageLogger,
	}.DecodeAndAssert(rawResponse)

	return err
}
