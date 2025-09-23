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

func testProduceResponse(stageHarness *test_case_harness.TestCaseHarness) error {
	stageLogger := stageHarness.Logger
	files_handler := kafka_files_generator.NewFilesHandler(logger.GetQuietLogger(""))

	topicName := random.RandomWord()

	files_handler.AddLogDirectoryGenerationConfig(kafka_files_generator.LogDirectoryGenerationConfig{
		TopicGenerationConfigList: []kafka_files_generator.TopicGenerationConfig{
			{
				Name: topicName,
				UUID: getRandomTopicUUID(),
				PartitonGenerationConfigList: []kafka_files_generator.PartitionGenerationConfig{
					{
						PartitionId: 0,
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
	defer client.Close()

	if err := client.ConnectWithRetries(b, stageLogger); err != nil {
		return err
	}

	correlationId := getRandomCorrelationId()

	request := builder.NewProduceRequestBuilder().
		WithCorrelationId(correlationId).
		WithTopicRequestData([]builder.ProduceRequestTopicData{
			{
				TopicName: topicName,
				PartitionsCreationData: []builder.ProduceRequestPartitionData{
					{
						PartitionId: 0,
						Logs:        []string{random.RandomWord()},
					},
				},
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

	return err
}
