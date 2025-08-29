package internal

import (
	"github.com/codecrafters-io/kafka-tester/internal/assertions_legacy"
	"github.com/codecrafters-io/kafka-tester/internal/kafka_executable"
	"github.com/codecrafters-io/kafka-tester/protocol/builder_legacy"
	"github.com/codecrafters-io/kafka-tester/protocol/common"
	"github.com/codecrafters-io/kafka-tester/protocol/kafka_client_legacy"
	"github.com/codecrafters-io/kafka-tester/protocol/kafkaapi_legacy"
	"github.com/codecrafters-io/kafka-tester/protocol/serializer_legacy"
	"github.com/codecrafters-io/tester-utils/test_case_harness"
)

func testDTPartitionWithTopicAndMultiplePartitions2(stageHarness *test_case_harness.TestCaseHarness) error {
	b := kafka_executable.NewKafkaExecutable(stageHarness)
	stageLogger := stageHarness.Logger
	err := serializer_legacy.GenerateLogDirs(stageLogger, true)
	if err != nil {
		return err
	}

	if err := b.Run(); err != nil {
		return err
	}

	correlationId := getRandomCorrelationId()

	client := kafka_client_legacy.NewClient("localhost:9092")
	if err := client.ConnectWithRetries(b, stageLogger); err != nil {
		return err
	}
	defer func(client *kafka_client_legacy.Client) {
		_ = client.Close()
	}(client)

	request := kafkaapi_legacy.DescribeTopicPartitionsRequest{
		Header: builder_legacy.NewRequestHeaderBuilder().BuildDescribeTopicPartitionsRequestHeader(correlationId),
		Body: kafkaapi_legacy.DescribeTopicPartitionsRequestBody{
			Topics: []kafkaapi_legacy.TopicName{
				{
					Name: common.TOPIC3_NAME,
				},
			},
			ResponsePartitionLimit: 2,
		},
	}

	response, err := client.SendAndReceive(request, stageLogger)
	if err != nil {
		return err
	}

	responseHeader, responseBody, err := kafkaapi_legacy.DecodeDescribeTopicPartitionsHeaderAndResponse(response.Payload, stageLogger)
	if err != nil {
		return err
	}

	expectedResponseHeader := builder_legacy.BuildResponseHeader(correlationId)
	if err = assertions_legacy.NewResponseHeaderAssertion(*responseHeader, expectedResponseHeader).Run(stageLogger); err != nil {
		return err
	}

	expectedDescribeTopicPartitionsResponse := kafkaapi_legacy.DescribeTopicPartitionsResponse{
		ThrottleTimeMs: 0,
		Topics: []kafkaapi_legacy.DescribeTopicPartitionsResponseTopic{
			{
				ErrorCode: 0,
				Name:      common.TOPIC3_NAME,
				TopicID:   common.TOPIC3_UUID,
				Partitions: []kafkaapi_legacy.DescribeTopicPartitionsResponsePartition{
					{
						ErrorCode:              0,
						PartitionIndex:         0,
						LeaderID:               1,
						LeaderEpoch:            1,
						ReplicaNodes:           []int32{1},
						IsrNodes:               []int32{1},
						EligibleLeaderReplicas: []int32{1},
						LastKnownELR:           []int32{1},
						OfflineReplicas:        []int32{1},
					},
					{
						ErrorCode:              0,
						PartitionIndex:         1,
						LeaderID:               1,
						LeaderEpoch:            1,
						ReplicaNodes:           []int32{1},
						IsrNodes:               []int32{1},
						EligibleLeaderReplicas: []int32{1},
						LastKnownELR:           []int32{1},
						OfflineReplicas:        []int32{1},
					},
				},
			},
		},
	}

	return assertions_legacy.NewDescribeTopicPartitionsResponseAssertion(*responseBody, expectedDescribeTopicPartitionsResponse).Run(stageLogger)
}
