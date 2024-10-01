package internal

import (
	"fmt"

	"github.com/codecrafters-io/kafka-tester/internal/assertions"
	"github.com/codecrafters-io/kafka-tester/internal/kafka_executable"
	"github.com/codecrafters-io/kafka-tester/protocol"
	kafkaapi "github.com/codecrafters-io/kafka-tester/protocol/api"
	"github.com/codecrafters-io/kafka-tester/protocol/serializer"
	"github.com/codecrafters-io/tester-utils/logger"
	"github.com/codecrafters-io/tester-utils/test_case_harness"
)

func testDTPartitionWithUnknownTopic(stageHarness *test_case_harness.TestCaseHarness) error {
	b := kafka_executable.NewKafkaExecutable(stageHarness)
	if err := b.Run(); err != nil {
		return err
	}

	quietLogger := logger.GetQuietLogger("")
	logger := stageHarness.Logger
	err := serializer.GenerateLogDirs(quietLogger)
	if err != nil {
		return err
	}

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
					Name: "unknown-topic",
				},
			},
			ResponsePartitionLimit: 1,
		},
	}

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

	if len(responseBody.Topics) != 1 {
		return fmt.Errorf("Expected topics.length to be 1, got %v", len(responseBody.Topics))
	}

	topicResponse := responseBody.Topics[0]

	if topicResponse.ErrorCode != 3 {
		return fmt.Errorf("Expected Error code to be 3, got %v", topicResponse.ErrorCode)
	}
	logger.Successf("✓ TopicResponse Error code: 3 (UNKNOWN_TOPIC_OR_PARTITION)")

	if topicResponse.Name != "unknown-topic" {
		return fmt.Errorf("Expected Topic to be unknown-topic, got %v", topicResponse.Name)
	}
	logger.Successf("✓ Topic Name: %v", topicResponse.Name)

	if topicResponse.TopicID != "00000000-0000-0000-0000-000000000000" {
		return fmt.Errorf("Expected Topic ID to be %v, got %v", "00000000-0000-0000-0000-000000000000", topicResponse.TopicID)
	}
	logger.Successf("✓ Topic UUID: %v", topicResponse.TopicID)

	if len(topicResponse.Partitions) != 0 {
		return fmt.Errorf("Expected Partitions to have length 0, got %v", len(topicResponse.Partitions))
	}

	return nil
}
