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

func testProduceForMultipleTopics(stageHarness *test_case_harness.TestCaseHarness) error {
	stageLogger := stageHarness.Logger
	files_handler := kafka_files_generator.NewFilesHandler(logger.GetQuietLogger(""))

	// We could have randomized for 2/3 topics but the logs are long enough already even for 2 topics
	// Decided to stick with 2 topics for now
	topicNames := getRandomTopicNames(2)

	// Randomize the number of partitions for each topic
	partitionsCount1 := random.RandomInt(1, 3)
	partitionsCount2 := random.RandomInt(1, 3)

	// Create partition generation configs for both topics
	topic1PartitionConfig := generateEmptyPartitionConfigs(partitionsCount1)
	topic2PartitionConfig := generateEmptyPartitionConfigs(partitionsCount2)

	files_handler.AddLogDirectoryGenerationConfig(kafka_files_generator.LogDirectoryGenerationConfig{
		TopicGenerationConfigList: []kafka_files_generator.TopicGenerationConfig{
			{
				Name:                         topicNames[0],
				UUID:                         getRandomTopicUUID(),
				PartitonGenerationConfigList: topic1PartitionConfig,
			},
			{
				Name:                         topicNames[1],
				UUID:                         getRandomTopicUUID(),
				PartitonGenerationConfigList: topic2PartitionConfig,
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
	defer client.Close()

	if err := client.ConnectWithRetries(b, stageLogger); err != nil {
		return err
	}
	correlationId := getRandomCorrelationId()

	// Create partition creation data for both topics
	topic1PartitionRequestData := generatePartitionRequestWithRandomLogs(partitionsCount1)
	topic2PartitionRequestData := generatePartitionRequestWithRandomLogs(partitionsCount2)

	request := builder.NewProduceRequestBuilder().
		WithCorrelationId(correlationId).
		WithTopicRequestData([]builder.ProduceRequestTopicData{
			{
				TopicName:              topicNames[0],
				PartitionsCreationData: topic1PartitionRequestData,
			},
			{
				TopicName:              topicNames[1],
				PartitionsCreationData: topic2PartitionRequestData,
			},
		}).
		Build()

	rawResponse, err := client.SendAndReceive(
		request_encoders.Encode(request, stageLogger),
		request.Header.ApiKey.Value,
		stageLogger,
	)

	if err != nil {
		return err
	}

	assertion := response_assertions.NewProduceResponseAssertion().
		ExpectCorrelationId(correlationId).
		ExpectThrottleTimeMs(0).
		ExpectTopicProperties(response_assertions.GetTopicExpectationData(request.Body.Topics))

	_, err = response_asserter.ResponseAsserter[kafkaapi.ProduceResponse]{
		DecodeFunc: response_decoders.DecodeProduceResponse,
		Assertion:  assertion,
		Logger:     stageLogger,
	}.DecodeAndAssert(rawResponse.Payload)

	// TODO: Check the contents of the disk

	return err
}
