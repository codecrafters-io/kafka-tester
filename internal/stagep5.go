package internal

import (
	"github.com/codecrafters-io/kafka-tester/internal/assertions"
	"github.com/codecrafters-io/kafka-tester/internal/kafka_executable"
	"github.com/codecrafters-io/kafka-tester/protocol"
	kafkaapi "github.com/codecrafters-io/kafka-tester/protocol/api"
	"github.com/codecrafters-io/kafka-tester/protocol/common"
	"github.com/codecrafters-io/kafka-tester/protocol/serializer"
	"github.com/codecrafters-io/tester-utils/test_case_harness"
)

func testDTPartitionWithTopics(stageHarness *test_case_harness.TestCaseHarness) error {
	b := kafka_executable.NewKafkaExecutable(stageHarness)
	if err := b.Run(); err != nil {
		return err
	}

	logger := stageHarness.Logger
	serializer.GenerateLogDirs(logger)

	correlationId := getRandomCorrelationId()

	broker := protocol.NewBroker("localhost:9092")
	if err := broker.ConnectWithRetries(b, logger); err != nil {
		return err
	}
	defer broker.Close()

	request := kafkaapi.DescribeTopicPartitionsRequest{
		Header: kafkaapi.RequestHeader{
			ApiKey:        75,
			ApiVersion:    0,
			CorrelationId: correlationId,
			ClientId:      "kafka-tester",
		},
		Body: kafkaapi.DescribeTopicPartitionsRequestBody{
			Topics: []kafkaapi.TopicName{
				{
					Name: common.TOPIC1_NAME,
				},
				{
					Name: common.TOPIC2_NAME,
				},
				{
					Name: common.TOPIC3_NAME,
				},
			},
			ResponsePartitionLimit: 4,
		},
	}
	// response for topicResponses will be sorted by topic name
	// bar -> baz -> foo

	message := kafkaapi.EncodeDescribeTopicPartitionsRequest(&request)
	logger.Infof("Sending \"DescribeTopicPartitions\" (version: %v) request (Correlation id: %v)", request.Header.ApiVersion, request.Header.CorrelationId)

	response, err := broker.SendAndReceive(message)
	if err != nil {
		return err
	}
	logger.Debugf("Hexdump of sent \"DescribeTopicPartitions\" request: \n%v\n", GetFormattedHexdump(message))
	logger.Debugf("Hexdump of received \"DescribeTopicPartitions\" response: \n%v\n", GetFormattedHexdump(response))

	responseHeader, responseBody, err := kafkaapi.DecodeDescribeTopicPartitionsHeaderAndResponse(response, logger)
	if err != nil {
		return err
	}

	expectedResponseHeader := kafkaapi.ResponseHeader{
		CorrelationId: correlationId,
	}
	if err = assertions.NewResponseHeaderAssertion(*responseHeader, expectedResponseHeader).Evaluate([]string{"CorrelationId"}, logger); err != nil {
		return err
	}

	expectedDescribeTopicPartitionsResponse := kafkaapi.DescribeTopicPartitionsResponse{
		ThrottleTimeMs: 0,
		Topics: []kafkaapi.DescribeTopicPartitionsResponseTopic{
			{
				ErrorCode: 0,
				Name:      common.TOPIC1_NAME,
				TopicID:   common.TOPIC1_UUID,
				Partitions: []kafkaapi.DescribeTopicPartitionsResponsePartition{
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
				},
			},
			{
				ErrorCode: 0,
				Name:      common.TOPIC2_NAME,
				TopicID:   common.TOPIC2_UUID,
				Partitions: []kafkaapi.DescribeTopicPartitionsResponsePartition{
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
				},
			},
			{
				ErrorCode: 0,
				Name:      common.TOPIC3_NAME,
				TopicID:   common.TOPIC3_UUID,
				Partitions: []kafkaapi.DescribeTopicPartitionsResponsePartition{
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

	return assertions.NewDescribeTopicPartitionsResponseAssertion(*responseBody, expectedDescribeTopicPartitionsResponse, logger).
		AssertBody([]string{"ThrottleTimeMs"}).
		AssertTopics([]string{"ErrorCode", "Name", "TopicID"}, []string{"ErrorCode", "PartitionIndex"}).
		Run()
}
