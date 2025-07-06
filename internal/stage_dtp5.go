package internal

import (
	"github.com/codecrafters-io/kafka-tester/internal/assertions"
	"github.com/codecrafters-io/kafka-tester/internal/kafka_executable"
	"github.com/codecrafters-io/kafka-tester/protocol"
	kafkaapi "github.com/codecrafters-io/kafka-tester/protocol/api"
	"github.com/codecrafters-io/kafka-tester/protocol/common"
	"github.com/codecrafters-io/kafka-tester/protocol/kafka_broker"
	"github.com/codecrafters-io/kafka-tester/protocol/serializer"
	"github.com/codecrafters-io/tester-utils/test_case_harness"
)

func testDTPartitionWithTopics(stageHarness *test_case_harness.TestCaseHarness) error {
	b := kafka_executable.NewKafkaExecutable(stageHarness)
	stageLogger := stageHarness.Logger
	err := serializer.GenerateLogDirs(stageLogger, true)
	if err != nil {
		return err
	}

	if err := b.Run(); err != nil {
		return err
	}

	correlationId := getRandomCorrelationId()
	broker := kafka_broker.NewBroker("localhost:9092")
	if err := broker.ConnectWithRetries(b, stageLogger); err != nil {
		return err
	}
	defer func(broker *kafka_broker.Broker) {
		_ = broker.Close()
	}(broker)

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
	stageLogger.Infof("Sending \"DescribeTopicPartitions\" (version: %v) request (Correlation id: %v)", request.Header.ApiVersion, request.Header.CorrelationId)
	stageLogger.Debugf("Hexdump of sent \"DescribeTopicPartitions\" request: \n%v\n", protocol.GetFormattedHexdump(message))

	response, err := broker.SendAndReceive(message)
	if err != nil {
		return err
	}
	stageLogger.Debugf("Hexdump of received \"DescribeTopicPartitions\" response: \n%v\n", protocol.GetFormattedHexdump(response.RawBytes))

	responseHeader, responseBody, err := kafkaapi.DecodeDescribeTopicPartitionsHeaderAndResponse(response.Payload, stageLogger)
	if err != nil {
		return err
	}

	expectedResponseHeader := kafkaapi.ResponseHeader{
		CorrelationId: correlationId,
	}
	if err = assertions.NewResponseHeaderAssertion(*responseHeader, expectedResponseHeader).Evaluate([]string{"CorrelationId"}, stageLogger); err != nil {
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

	return assertions.NewDescribeTopicPartitionsResponseAssertion(*responseBody, expectedDescribeTopicPartitionsResponse, stageLogger).
		AssertBody([]string{"ThrottleTimeMs"}).
		AssertTopics([]string{"ErrorCode", "Name", "TopicID"}, []string{"ErrorCode", "PartitionIndex"}).
		Run()
}
