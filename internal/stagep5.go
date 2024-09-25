package internal

import (
	"fmt"

	"github.com/codecrafters-io/kafka-tester/internal/kafka_executable"
	"github.com/codecrafters-io/kafka-tester/protocol"
	kafkaapi "github.com/codecrafters-io/kafka-tester/protocol/api"
	"github.com/codecrafters-io/kafka-tester/protocol/common"
	"github.com/codecrafters-io/tester-utils/test_case_harness"
)

func testDTPartitionWithTopicAndMultiplePartitions2(stageHarness *test_case_harness.TestCaseHarness) error {
	b := kafka_executable.NewKafkaExecutable(stageHarness)
	if err := b.Run(); err != nil {
		return err
	}

	logger := stageHarness.Logger

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
	logger.Infof("Sending \"Fetch\" (version: %v) request (Correlation id: %v)", request.Header.ApiVersion, request.Header.CorrelationId)

	response, err := broker.SendAndReceive(message)
	if err != nil {
		return err
	}
	logger.Debugf("Hexdump of sent \"Fetch\" request: \n%v\n", GetFormattedHexdump(message))
	logger.Debugf("Hexdump of received \"Fetch\" response: \n%v\n", GetFormattedHexdump(response))

	responseHeader, responseBody, err := kafkaapi.DecodeDescribeTopicPartitionHeaderAndResponse(response, logger)
	if err != nil {
		return err
	}

	if responseHeader.CorrelationId != correlationId {
		return fmt.Errorf("Expected Correlation ID to be %v, got %v", correlationId, responseHeader.CorrelationId)
	}
	logger.Successf("✓ Correlation ID: %v", responseHeader.CorrelationId)

	if len(responseBody.Topics) != 1 {
		return fmt.Errorf("Expected topicResponse to have length 2, got %v", len(responseBody.Topics))
	}

	topicResponse := responseBody.Topics[0]

	if topicResponse.ErrorCode != 0 {
		return fmt.Errorf("Expected Error code to be 0, got %v", topicResponse.ErrorCode)
	}
	logger.Successf("✓ TopicResponse Error code: 0")

	if topicResponse.Name != common.TOPIC3_NAME {
		return fmt.Errorf("Expected Topic to be %v, got %v", common.TOPIC3_NAME, topicResponse.Name)
	}
	logger.Successf("✓ Topic Name: %v", topicResponse.Name)

	if topicResponse.TopicID != common.TOPIC3_UUID {
		return fmt.Errorf("Expected Topic ID to be %v, got %v", common.TOPIC3_UUID, topicResponse.TopicID)
	}
	logger.Successf("✓ Topic ID: %v", topicResponse.TopicID)

	if len(topicResponse.Partitions) != 2 {
		return fmt.Errorf("Expected Partitions to have length 2, got %v", len(topicResponse.Partitions))
	}

	partitionResponse1 := topicResponse.Partitions[0]

	if partitionResponse1.ErrorCode != 0 {
		return fmt.Errorf("Expected Error code to be 0, got %v", partitionResponse1.ErrorCode)
	}
	logger.Successf("✓ PartitionResponse[0] Error code: 0")

	if partitionResponse1.PartitionIndex != 0 {
		return fmt.Errorf("Expected Partition Index to be 0, got %v", partitionResponse1.PartitionIndex)
	}
	logger.Successf("✓ PartitionResponse[0] Partition Index: 0")

	partitionResponse2 := topicResponse.Partitions[1]

	if partitionResponse2.ErrorCode != 0 {
		return fmt.Errorf("Expected Error code to be 0, got %v", partitionResponse2.ErrorCode)
	}
	logger.Successf("✓ PartitionResponse[1] Error code: 0")

	if partitionResponse2.PartitionIndex != 1 {
		return fmt.Errorf("Expected Partition Index to be 1, got %v", partitionResponse2.PartitionIndex)
	}
	logger.Successf("✓ PartitionResponse[1] Partition Index: 1")

	return nil
}
