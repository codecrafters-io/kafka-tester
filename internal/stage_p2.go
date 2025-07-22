package internal

import (
	"github.com/codecrafters-io/kafka-tester/internal/assertions"
	"github.com/codecrafters-io/kafka-tester/internal/kafka_executable"
	kafkaapi "github.com/codecrafters-io/kafka-tester/protocol/api"
	"github.com/codecrafters-io/kafka-tester/protocol/builder"
	"github.com/codecrafters-io/kafka-tester/protocol/common"
	"github.com/codecrafters-io/kafka-tester/protocol/kafka_client"
	"github.com/codecrafters-io/kafka-tester/protocol/serializer"
	"github.com/codecrafters-io/tester-utils/logger"
	"github.com/codecrafters-io/tester-utils/test_case_harness"
)

func testProduce2(stageHarness *test_case_harness.TestCaseHarness) error {
	b := kafka_executable.NewKafkaExecutable(stageHarness)
	stageLogger := stageHarness.Logger
	err := serializer.GenerateLogDirs(logger.GetQuietLogger(""), false)
	if err != nil {
		return err
	}

	if err := b.Run(); err != nil {
		return err
	}

	correlationId := getRandomCorrelationId()

	client := kafka_client.NewClient("localhost:9092")
	if err := client.ConnectWithRetries(b, stageLogger); err != nil {
		return err
	}
	defer func(client *kafka_client.Client) {
		_ = client.Close()
	}(client)

	request := kafkaapi.ProduceRequest{
		Header: builder.NewRequestHeaderBuilder().
			BuildProduceRequestHeader(correlationId),
		Body: builder.NewProduceRequestBuilder().
			AddRecordBatchToTopicPartition(common.TOPIC_UNKOWN_NAME, 0, []string{common.HELLO_MSG1}).
			Build(),
	}

	response, err := client.SendAndReceive(request, stageLogger)
	if err != nil {
		return err
	}

	responseHeader, responseBody, err := kafkaapi.DecodeProduceHeaderAndResponse(response.Payload, 11, stageLogger)
	if err != nil {
		return err
	}

	expectedResponse := builder.NewProduceResponseBuilder().
		AddTopicPartitionResponse(common.TOPIC_UNKOWN_NAME, 0, 3).
		Build(correlationId)

	if err = assertions.NewResponseHeaderAssertion(*responseHeader, expectedResponse.Header).Evaluate([]string{"CorrelationId"}, stageLogger); err != nil {
		return err
	}

	if err = assertions.NewProduceResponseAssertion(*responseBody, expectedResponse.Body, stageLogger).Run(stageLogger); err != nil {
		return err
	}

	return nil
}
