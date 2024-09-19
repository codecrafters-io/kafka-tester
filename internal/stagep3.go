package internal

import (
	"fmt"

	"github.com/codecrafters-io/kafka-tester/internal/kafka_executable"
	"github.com/codecrafters-io/kafka-tester/protocol"
	kafkaapi "github.com/codecrafters-io/kafka-tester/protocol/api"
	"github.com/codecrafters-io/tester-utils/test_case_harness"
)

func testDTPartitionWithTopicAndSinglePartition(stageHarness *test_case_harness.TestCaseHarness) error {
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
					Name: "foo",
				},
			},
			ResponsePartitionLimit: 1,
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
		return fmt.Errorf("Expected topicResponse to have length 1, got %v", len(responseBody.Topics))
	}

	topicResponse := responseBody.Topics[0]

	if topicResponse.ErrorCode != 0 {
		return fmt.Errorf("Expected Error code to be 0, got %v", topicResponse.ErrorCode)
	}
	logger.Successf("✓ TopicResponse Error code: 0")

	if topicResponse.Name != "foo" {
		return fmt.Errorf("Expected Topic to be foo, got %v", topicResponse.Name)
	}
	logger.Successf("✓ Topic Name: %v", topicResponse.Name)

	if topicResponse.TopicID != "bfd99e5e-3235-4552-81f8-d4af1741970c" {
		return fmt.Errorf("Expected Topic ID to be bfd99e5e-3235-4552-81f8-d4af1741970c, got %v", topicResponse.TopicID)
	}
	logger.Successf("✓ Topic ID: %v", topicResponse.TopicID)

	if len(topicResponse.Partitions) != 1 {
		return fmt.Errorf("Expected Partitions to have length 1, got %v", len(topicResponse.Partitions))
	}

	partitionResponse := topicResponse.Partitions[0]

	if partitionResponse.ErrorCode != 0 {
		return fmt.Errorf("Expected Error code to be 0, got %v", partitionResponse.ErrorCode)
	}
	logger.Successf("✓ PartitionResponse Error code: 0")

	if partitionResponse.PartitionIndex != 0 {
		return fmt.Errorf("Expected Partition Index to be 0, got %v", partitionResponse.PartitionIndex)
	}
	logger.Successf("✓ PartitionResponse Partition Index: 0")

	return nil
}