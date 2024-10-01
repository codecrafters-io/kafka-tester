package assertions

import (
	"fmt"

	"github.com/codecrafters-io/kafka-tester/protocol"
	kafkaapi "github.com/codecrafters-io/kafka-tester/protocol/api"
	"github.com/codecrafters-io/tester-utils/logger"
)

type DescribeTopicPartitionsResponseAssertion struct {
	ActualValue   kafkaapi.DescribeTopicPartitionsResponse
	ExpectedValue kafkaapi.DescribeTopicPartitionsResponse
	logger        *logger.Logger
	err           error
}

func NewDescribeTopicPartitionsResponseAssertion(actualValue kafkaapi.DescribeTopicPartitionsResponse, expectedValue kafkaapi.DescribeTopicPartitionsResponse, logger *logger.Logger) *DescribeTopicPartitionsResponseAssertion {
	return &DescribeTopicPartitionsResponseAssertion{
		ActualValue:   actualValue,
		ExpectedValue: expectedValue,
		logger:        logger,
	}
}

func (a *DescribeTopicPartitionsResponseAssertion) AssertBody(fields []string) *DescribeTopicPartitionsResponseAssertion {
	if a.err != nil {
		return a
	}
	if Contains(fields, "ThrottleTimeMs") {
		if a.ActualValue.ThrottleTimeMs != a.ExpectedValue.ThrottleTimeMs {
			a.err = fmt.Errorf("Expected %s to be %d, got %d", "ThrottleTimeMs", a.ExpectedValue.ThrottleTimeMs, a.ActualValue.ThrottleTimeMs)
			return a
		}
		a.logger.Successf("✓ Throttle Time: %d", a.ActualValue.ThrottleTimeMs)
	}

	return a
}

func (a *DescribeTopicPartitionsResponseAssertion) AssertTopics(topicFields []string, partitionFields []string) *DescribeTopicPartitionsResponseAssertion {
	if a.err != nil {
		return a
	}

	if len(a.ActualValue.Topics) != len(a.ExpectedValue.Topics) {
		a.err = fmt.Errorf("Expected %s to be %d, got %d", "topics.length", len(a.ExpectedValue.Topics), len(a.ActualValue.Topics))
		return a
	}

	for i, actualTopic := range a.ActualValue.Topics {
		expectedTopic := a.ExpectedValue.Topics[i]
		if Contains(topicFields, "ErrorCode") {
			if actualTopic.ErrorCode != expectedTopic.ErrorCode {
				a.err = fmt.Errorf("Expected %s to be %d, got %d", fmt.Sprintf("TopicResponse[%d] Error Code", i), expectedTopic.ErrorCode, actualTopic.ErrorCode)
				return a
			}
			protocol.SuccessLogWithIndentation(a.logger, 1, "✓ TopicResponse[%d] Error code: %d", i, actualTopic.ErrorCode)
		}

		if Contains(topicFields, "Name") {
			if actualTopic.Name != expectedTopic.Name {
				a.err = fmt.Errorf("Expected %s to be %s, got %s", fmt.Sprintf("TopicResponse[%d] Topic Name", i), expectedTopic.Name, actualTopic.Name)
				return a
			}
			protocol.SuccessLogWithIndentation(a.logger, 1, "✓ TopicResponse[%d] Topic Name: %s", i, actualTopic.Name)
		}

		if Contains(topicFields, "TopicID") {
			if actualTopic.TopicID != expectedTopic.TopicID {
				a.err = fmt.Errorf("Expected %s to be %s, got %s", fmt.Sprintf("TopicResponse[%d] Topic UUID", i), expectedTopic.TopicID, actualTopic.TopicID)
				return a
			}
			protocol.SuccessLogWithIndentation(a.logger, 1, "✓ TopicResponse[%d] Topic UUID: %s", i, actualTopic.TopicID)
		}

		if Contains(topicFields, "TopicAuthorizedOperations") {
			if actualTopic.TopicAuthorizedOperations != expectedTopic.TopicAuthorizedOperations {
				a.err = fmt.Errorf("Expected %s to be %d, got %d", fmt.Sprintf("TopicResponse[%d] Topic Authorized Operations", i), expectedTopic.TopicAuthorizedOperations, actualTopic.TopicAuthorizedOperations)
				return a
			}
			protocol.SuccessLogWithIndentation(a.logger, 1, "✓ TopicResponse[%d] Topic Authorized Operations: %d", i, actualTopic.TopicAuthorizedOperations)
		}

		expectedPartitions := expectedTopic.Partitions
		actualPartitions := actualTopic.Partitions

		if (partitionFields) != nil {
			a.assertPartitions(expectedPartitions, actualPartitions, partitionFields)
		}
	}

	return a
}

func (a *DescribeTopicPartitionsResponseAssertion) assertPartitions(expectedPartitions []kafkaapi.DescribeTopicPartitionsResponsePartition, actualPartitions []kafkaapi.DescribeTopicPartitionsResponsePartition, fields []string) *DescribeTopicPartitionsResponseAssertion {
	if len(actualPartitions) != len(expectedPartitions) {
		a.err = fmt.Errorf("Expected %s to be %d, got %d", "partitions.length", len(expectedPartitions), len(actualPartitions))
		return a
	}

	for j, actualPartition := range actualPartitions {
		expectedPartition := expectedPartitions[j]

		if Contains(fields, "ErrorCode") {
			if actualPartition.ErrorCode != expectedPartition.ErrorCode {
				a.err = fmt.Errorf("Expected %s to be %d, got %d", fmt.Sprintf("PartitionResponse[%d] Error Code", j), expectedPartition.ErrorCode, actualPartition.ErrorCode)
				return a
			}
			protocol.SuccessLogWithIndentation(a.logger, 2, "✓ PartitionResponse[%d] Error code: %d", j, actualPartition.ErrorCode)
		}

		if Contains(fields, "PartitionIndex") {
			if actualPartition.PartitionIndex != expectedPartition.PartitionIndex {
				a.err = fmt.Errorf("Expected %s to be %d, got %d", fmt.Sprintf("Partition Response[%d] Partition Index", j), expectedPartition.PartitionIndex, actualPartition.PartitionIndex)
				return a
			}
			protocol.SuccessLogWithIndentation(a.logger, 2, "✓ PartitionResponse[%d] Partition Index: %d", j, actualPartition.PartitionIndex)
		}

	}

	return nil
}

func (a DescribeTopicPartitionsResponseAssertion) Run() error {
	// firstLevelFields: ["ThrottleTimeMs"]
	// secondLevelFields (Topics): ["Name", "TopicID", "Partitions"]
	// thirdLevelFields (Partitions): ["ID", "Leader", "Replicas", "Isr"]
	return a.err
}
