package internal

import (
	"fmt"

	"github.com/codecrafters-io/kafka-tester/internal/assertions"
	"github.com/codecrafters-io/kafka-tester/internal/kafka_executable"
	"github.com/codecrafters-io/kafka-tester/protocol"
	kafkaapi "github.com/codecrafters-io/kafka-tester/protocol/api"
	"github.com/codecrafters-io/kafka-tester/protocol/common"
	"github.com/codecrafters-io/kafka-tester/protocol/serializer"
	"github.com/codecrafters-io/tester-utils/test_case_harness"
)

func testDTPartitionWithTopicAndMultiplePartitions2(stageHarness *test_case_harness.TestCaseHarness) error {
	b := kafka_executable.NewKafkaExecutable(stageHarness)
	if err := b.Run(); err != nil {
		return err
	}

	logger := stageHarness.Logger
	err := serializer.GenerateLogDirs(logger)
	if err != nil {
		return err
	}

	correlationId := getRandomCorrelationId()

	broker := protocol.NewBroker("localhost:9092")
	if err := broker.ConnectWithRetries(b, logger); err != nil {
		return err
	}
	defer broker.Close()

	request := kafkaapi.DescribeTopicPartitionRequest{
		Header: kafkaapi.RequestHeader{
			ApiKey:        75,
			ApiVersion:    0,
			CorrelationId: correlationId,
			ClientId:      "kafka-tester",
		},
		Body: kafkaapi.DescribeTopicPartitionRequestBody{
			Topics: []kafkaapi.TopicName{
				{
					Name: common.TOPIC3_NAME,
				},
			},
			ResponsePartitionLimit: 2,
		},
	}

	message := kafkaapi.EncodeDescribeTopicPartitionRequest(&request)
	logger.Infof("Sending \"DescribeTopicPartition\" (version: %v) request (Correlation id: %v)", request.Header.ApiVersion, request.Header.CorrelationId)

	response, err := broker.SendAndReceive(message)
	if err != nil {
		return err
	}
	logger.Debugf("Hexdump of sent \"DescribeTopicPartition\" request: \n%v\n", GetFormattedHexdump(message))
	logger.Debugf("Hexdump of received \"DescribeTopicPartition\" response: \n%v\n", GetFormattedHexdump(response))

	responseHeader, responseBody, err := kafkaapi.DecodeDescribeTopicPartitionHeaderAndResponse(response, logger)
	if err != nil {
		return err
	}

	expectedDescribeTopicPartitionsResponse := kafkaapi.DescribeTopicPartitionsResponse{
		ThrottleTimeMs: 0,
		Topics: []kafkaapi.DescribeTopicPartitionsResponseTopic{
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

	err = assertions.NewDescribeTopicPartitionsResponseAssertion(*responseBody, expectedDescribeTopicPartitionsResponse, logger).
		AssertBody([]string{"ThrottleTimeMs"}).
		AssertTopics([]string{"ErrorCode", "Name", "TopicID"}).
		AssertPartitions([]string{"ErrorCode", "PartitionIndex"}).
		Run()

	if err != nil {
		return err
	}

	if responseHeader.CorrelationId != correlationId {
		return fmt.Errorf("Expected Correlation ID to be %v, got %v", correlationId, responseHeader.CorrelationId)
	}
	logger.Successf("✓ Correlation ID: %v", responseHeader.CorrelationId)

	return nil
}
