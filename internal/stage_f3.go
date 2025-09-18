package internal

import (
	"github.com/codecrafters-io/kafka-tester/internal/kafka_executable"
	"github.com/codecrafters-io/kafka-tester/internal/request_encoders"
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

func testFetchWithUnknownTopicId(stageHarness *test_case_harness.TestCaseHarness) error {
	stageLogger := stageHarness.Logger

	files_handler := kafka_files_generator.NewFilesHandler(logger.GetQuietLogger(""))

	// use one UUID for topic creation and another (unknown) for fetch
	topicUUIDs := getRandomTopicUUIDs(2)
	files_handler.AddLogDirectoryGenerationConfig(kafka_files_generator.LogDirectoryGenerationConfig{
		TopicGenerationConfigList: []kafka_files_generator.TopicGenerationConfig{
			{
				Name: random.RandomWord(),
				UUID: topicUUIDs[0],
				PartitonGenerationConfigList: []kafka_files_generator.PartitionGenerationConfig{
					{
						PartitionId: 0,
						Logs:        []string{random.RandomString()},
					},
				},
			},
		},
	})

	if err := files_handler.GenerateServerConfigAndLogDirs(); err != nil {
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
	sentTopicUUID := topicUUIDs[1]
	sentPartitionId := 0

	request := builder.NewFetchRequestBuilder().
		WithCorrelationId(correlationId).
		WithTopicUUID(sentTopicUUID).
		WithPartitionID(int32(sentPartitionId)).
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
		ExpectTopicUUID(sentTopicUUID).
		ExpectPartitionID(int32(sentPartitionId)).
		ExpectErrorCodeInPartition(100).
		ExpectThrottleTimeMs(0)

	_, err = response_asserter.ResponseAsserter[kafkaapi.FetchResponse]{
		DecodeFunc: response_decoders.DecodeFetchResponse,
		Assertion:  assertion,
		Logger:     stageLogger,
	}.DecodeAndAssert(rawResponse.Payload)

	return err
}
