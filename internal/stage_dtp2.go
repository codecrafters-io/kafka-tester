package internal

import (
	"github.com/codecrafters-io/kafka-tester/internal/kafka_executable"
	"github.com/codecrafters-io/kafka-tester/internal/legacy_assertions"
	"github.com/codecrafters-io/kafka-tester/protocol/common"
	"github.com/codecrafters-io/kafka-tester/protocol/legacy_builder"
	"github.com/codecrafters-io/kafka-tester/protocol/legacy_kafka_client"
	"github.com/codecrafters-io/kafka-tester/protocol/legacy_kafkaapi"
	"github.com/codecrafters-io/kafka-tester/protocol/legacy_serializer"
	"github.com/codecrafters-io/tester-utils/logger"
	"github.com/codecrafters-io/tester-utils/test_case_harness"
)

func testDTPartitionWithUnknownTopic(stageHarness *test_case_harness.TestCaseHarness) error {
	b := kafka_executable.NewKafkaExecutable(stageHarness)
	err := legacy_serializer.GenerateLogDirs(logger.GetQuietLogger(""), true)
	if err != nil {
		return err
	}

	stageLogger := stageHarness.Logger
	if err := b.Run(); err != nil {
		return err
	}

	correlationId := getRandomCorrelationId()

	client := legacy_kafka_client.NewClient("localhost:9092")
	if err := client.ConnectWithRetries(b, stageLogger); err != nil {
		return err
	}
	defer func(client *legacy_kafka_client.Client) {
		_ = client.Close()
	}(client)

	request := legacy_kafkaapi.DescribeTopicPartitionsRequest{
		Header: legacy_builder.NewRequestHeaderBuilder().BuildDescribeTopicPartitionsRequestHeader(correlationId),
		Body: legacy_kafkaapi.DescribeTopicPartitionsRequestBody{
			Topics: []legacy_kafkaapi.TopicName{
				{
					Name: common.TOPIC_UNKOWN_NAME,
				},
			},
			ResponsePartitionLimit: 1,
		},
	}

	response, err := client.SendAndReceive(request, stageLogger)
	if err != nil {
		return err
	}

	responseHeader, responseBody, err := legacy_kafkaapi.DecodeDescribeTopicPartitionsHeaderAndResponse(response.Payload, stageLogger)
	if err != nil {
		return err
	}

	expectedResponseHeader := legacy_builder.BuildResponseHeader(correlationId)
	if err = legacy_assertions.NewResponseHeaderAssertion(*responseHeader, expectedResponseHeader).Run(stageLogger); err != nil {
		return err
	}

	expectedDescribeTopicPartitionsResponse := legacy_kafkaapi.DescribeTopicPartitionsResponse{
		ThrottleTimeMs: 0,
		Topics: []legacy_kafkaapi.DescribeTopicPartitionsResponseTopic{
			{
				ErrorCode:  3,
				Name:       common.TOPIC_UNKOWN_NAME,
				TopicID:    common.TOPIC_UNKOWN_UUID,
				Partitions: []legacy_kafkaapi.DescribeTopicPartitionsResponsePartition{},
			},
		},
	}

	return legacy_assertions.NewDescribeTopicPartitionsResponseAssertion(*responseBody, expectedDescribeTopicPartitionsResponse).
		Run(stageLogger)
}
