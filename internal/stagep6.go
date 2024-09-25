package internal

import (
	"fmt"

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
	// ToDo: Better framework for testing this

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

	if responseHeader.CorrelationId != correlationId {
		return fmt.Errorf("Expected Correlation ID to be %v, got %v", correlationId, responseHeader.CorrelationId)
	}
	logger.Successf("✓ Correlation ID: %v", responseHeader.CorrelationId)

	if len(responseBody.Topics) != 3 {
		return fmt.Errorf("Expected topicResponse to have length 3, got %v", len(responseBody.Topics))
	}

	// We expect the topics to be in the following order:
	// TOPIC1, TOPIC2, TOPIC3 (in that order)
	// If order is mismatched, the test will fail

	topicResponse1 := responseBody.Topics[0]

	if topicResponse1.ErrorCode != 0 {
		return fmt.Errorf("Expected Error code to be 0, got %v", topicResponse1.ErrorCode)
	}
	logger.Successf("✓ TopicResponse Error code: 0")

	if topicResponse1.Name != common.TOPIC1_NAME {
		return fmt.Errorf("Expected Topic to be %v, got %v", common.TOPIC1_NAME, topicResponse1.Name)
	}
	logger.Successf("✓ Topic Name: %v", topicResponse1.Name)

	if topicResponse1.TopicID != common.TOPIC1_UUID {
		return fmt.Errorf("Expected Topic ID to be %v, got %v", common.TOPIC1_UUID, topicResponse1.TopicID)
	}
	logger.Successf("✓ Topic UUID: %v", topicResponse1.TopicID)

	if len(topicResponse1.Partitions) != 1 {
		return fmt.Errorf("Expected Partitions to have length 1, got %v", len(topicResponse1.Partitions))
	}

	partitionResponse1 := topicResponse1.Partitions[0]

	if partitionResponse1.ErrorCode != 0 {
		return fmt.Errorf("Expected Error code to be 0, got %v", partitionResponse1.ErrorCode)
	}
	logger.Successf("✓ PartitionResponse[0] Error code: 0")

	if partitionResponse1.PartitionIndex != 0 {
		return fmt.Errorf("Expected Partition Index to be 0, got %v", partitionResponse1.PartitionIndex)
	}
	logger.Successf("✓ PartitionResponse[0] Partition Index: 0")

	topicResponse2 := responseBody.Topics[1]

	if topicResponse2.ErrorCode != 0 {
		return fmt.Errorf("Expected Error code to be 0, got %v", topicResponse2.ErrorCode)
	}
	logger.Successf("✓ TopicResponse Error code: 0")

	if topicResponse2.Name != common.TOPIC2_NAME {
		return fmt.Errorf("Expected Topic to be %v, got %v", common.TOPIC2_NAME, topicResponse2.Name)
	}
	logger.Successf("✓ Topic Name: %v", topicResponse2.Name)

	if topicResponse2.TopicID != common.TOPIC2_UUID {
		return fmt.Errorf("Expected Topic ID to be %v, got %v", common.TOPIC2_UUID, topicResponse2.TopicID)
	}
	logger.Successf("✓ Topic UUID: %v", topicResponse2.TopicID)

	if len(topicResponse2.Partitions) != 1 {
		return fmt.Errorf("Expected Partitions to have length 1, got %v", len(topicResponse2.Partitions))
	}

	partitionResponse2 := topicResponse2.Partitions[0]

	if partitionResponse2.ErrorCode != 0 {
		return fmt.Errorf("Expected Error code to be 0, got %v", partitionResponse2.ErrorCode)
	}
	logger.Successf("✓ PartitionResponse[0] Error code: 0")

	if partitionResponse2.PartitionIndex != 0 {
		return fmt.Errorf("Expected Partition Index to be 0, got %v", partitionResponse2.PartitionIndex)
	}
	logger.Successf("✓ PartitionResponse[0] Partition Index: 0")

	topicResponse3 := responseBody.Topics[2]

	if topicResponse3.ErrorCode != 0 {
		return fmt.Errorf("Expected Error code to be 0, got %v", topicResponse3.ErrorCode)
	}
	logger.Successf("✓ TopicResponse Error code: 0")

	if topicResponse3.Name != common.TOPIC3_NAME {
		return fmt.Errorf("Expected Topic to be %v, got %v", common.TOPIC3_NAME, topicResponse3.Name)
	}
	logger.Successf("✓ Topic Name: %v", topicResponse3.Name)

	if topicResponse3.TopicID != common.TOPIC3_UUID {
		return fmt.Errorf("Expected Topic ID to be %v, got %v", common.TOPIC3_UUID, topicResponse3.TopicID)
	}
	logger.Successf("✓ Topic UUID: %v", topicResponse3.TopicID)

	if len(topicResponse3.Partitions) != 2 {
		return fmt.Errorf("Expected Partitions to have length 1, got %v", len(topicResponse3.Partitions))
	}

	partitionResponse3 := topicResponse3.Partitions[0]

	if partitionResponse3.ErrorCode != 0 {
		return fmt.Errorf("Expected Error code to be 0, got %v", partitionResponse3.ErrorCode)
	}
	logger.Successf("✓ PartitionResponse[0] Error code: 0")

	if partitionResponse3.PartitionIndex != 0 {
		return fmt.Errorf("Expected Partition Index to be 0, got %v", partitionResponse3.PartitionIndex)
	}
	logger.Successf("✓ PartitionResponse[0] Partition Index: 0")

	partitionResponse4 := topicResponse3.Partitions[1]

	if partitionResponse4.ErrorCode != 0 {
		return fmt.Errorf("Expected Error code to be 0, got %v", partitionResponse4.ErrorCode)
	}
	logger.Successf("✓ PartitionResponse[1] Error code: 0")

	if partitionResponse4.PartitionIndex != 1 {
		return fmt.Errorf("Expected Partition Index to be 1, got %v", partitionResponse4.PartitionIndex)
	}
	logger.Successf("✓ PartitionResponse[1] Partition Index: 1")

	return nil
}
