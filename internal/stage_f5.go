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

func testFetchWithSingleMessage(stageHarness *test_case_harness.TestCaseHarness) error {
	stageLogger := stageHarness.Logger

	files_handler := kafka_files_generator.NewFilesHandler(logger.GetQuietLogger(""))

	topicUUID := getRandomTopicUUID()
	partitionID := 0
	files_handler.AddLogDirectoryGenerationConfig(kafka_files_generator.LogDirectoryGenerationConfig{
		TopicGenerationConfigList: []kafka_files_generator.TopicGenerationConfig{
			{
				Name: random.RandomWord(),
				UUID: topicUUID,
				PartitonGenerationConfigList: []kafka_files_generator.PartitionGenerationConfig{
					{
						PartitionID: partitionID,
						Logs:        []string{random.RandomString()},
					},
				},
			},
		},
	})

	if err := files_handler.GenerateServerConfigAndLogDirs(); err != nil {
		return err
	}

	generatedTopicsData := files_handler.GetGeneratedLogDirectoryData().GeneratedTopicsData
	expectedRecordBatches := generatedTopicsData[0].GeneratedRecordBatchesByPartition[partitionID]

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
	sessionId := 0
	partitionId := 0

	request := builder.NewFetchRequestBuilder().
		WithCorrelationId(correlationId).
		WithSessionId(int32(sessionId)).
		WithTopicUUID(topicUUID).
		WithPartitionID(int32(partitionId)).
		Build()

	rawResponse, err := client.SendAndReceive(request, stageLogger)
	if err != nil {
		return err
	}

	assertion := response_assertions.NewFetchResponseAssertion().
		ExpectCorrelationId(correlationId).
		ExpectSessionId(int32(sessionId)).
		ExpectErrorCodeInBody(0).
		ExpectTopicUUID(topicUUID).
		ExpectPartitionID(int32(partitionId)).
		ExpectErrorCodeInPartition(0).
		ExpectThrottleTimeMs(0).
		ExpectRecordBatches(expectedRecordBatches)

	_, err = response_asserter.ResponseAsserter[kafkaapi.FetchResponse]{
		DecodeFunc: response_decoders.DecodeFetchResponse,
		Assertion:  assertion,
		Logger:     stageLogger,
	}.DecodeAndAssert(rawResponse.Payload)

	return err
}
