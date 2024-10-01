package assertions

import (
	"fmt"

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

func (a *DescribeTopicPartitionsResponseAssertion) AssertTopics(fields []string) *DescribeTopicPartitionsResponseAssertion {
	if a.err != nil {
		return a
	}

	if len(a.ActualValue.Topics) != len(a.ExpectedValue.Topics) {
		a.err = fmt.Errorf("Expected %s to be %d, got %d", "topics.length", len(a.ExpectedValue.Topics), len(a.ActualValue.Topics))
		return a
	}

	for i, actualTopic := range a.ActualValue.Topics {
		expectedTopic := a.ExpectedValue.Topics[i]
		if Contains(fields, "ErrorCode") {
			if actualTopic.ErrorCode != expectedTopic.ErrorCode {
				a.err = fmt.Errorf("Expected %s to be %d, got %d", "Error Code", expectedTopic.ErrorCode, actualTopic.ErrorCode)
				return a
			}
			a.logger.Successf("✓ TopicResponse[%d] Error code: %d", i, actualTopic.ErrorCode)
		}

		if Contains(fields, "Name") {
			if actualTopic.Name != expectedTopic.Name {
				a.err = fmt.Errorf("Expected %s to be %s, got %s", "Topic Name", expectedTopic.Name, actualTopic.Name)
				return a
			}
			a.logger.Successf("✓ TopicResponse[%d] Topic Name: %s", i, actualTopic.Name)
		}

		if Contains(fields, "TopicID") {
			if actualTopic.TopicID != expectedTopic.TopicID {
				a.err = fmt.Errorf("Expected %s to be %s, got %s", "Topic UUID", expectedTopic.TopicID, actualTopic.TopicID)
				return a
			}
			a.logger.Successf("✓ TopicResponse[%d] Topic UUID: %s", i, actualTopic.TopicID)
		}

		if Contains(fields, "TopicAuthorizedOperations") {
			if actualTopic.TopicAuthorizedOperations != expectedTopic.TopicAuthorizedOperations {
				a.err = fmt.Errorf("Expected %s to be %d, got %d", "Topic Authorized Operations", expectedTopic.TopicAuthorizedOperations, actualTopic.TopicAuthorizedOperations)
				return a
			}
			a.logger.Successf("✓ TopicResponse[%d] Topic Authorized Operations: %d", i, actualTopic.TopicAuthorizedOperations)
		}
	}

	return a
}

func (a *DescribeTopicPartitionsResponseAssertion) AssertPartitions(fields []string) *DescribeTopicPartitionsResponseAssertion {
	if a.err != nil {
		return a
	}

	for i, actualTopic := range a.ActualValue.Topics {
		expectedTopic := a.ExpectedValue.Topics[i]
		actualPartitions, expectedPartitions := actualTopic.Partitions, expectedTopic.Partitions

		if len(actualPartitions) != len(expectedPartitions) {
			a.err = fmt.Errorf("Expected %s to be %d, got %d", "partitions.length", len(expectedPartitions), len(actualPartitions))
			return a
		}

		for j, actualPartition := range actualPartitions {
			expectedPartition := expectedPartitions[j]

			if Contains(fields, "ErrorCode") {
				if actualPartition.ErrorCode != expectedPartition.ErrorCode {
					// ToDo: Improve error message
					a.err = fmt.Errorf("Expected %s to be %d, got %d", "Error Code", expectedPartition.ErrorCode, actualPartition.ErrorCode)
					return a
				}
				a.logger.Successf("✓ PartitionResponse[%d] Error code: %d", j, actualPartition.ErrorCode)
			}

			if Contains(fields, "PartitionIndex") {
				if actualPartition.PartitionIndex != expectedPartition.PartitionIndex {
					a.err = fmt.Errorf("Expected %s to be %d, got %d", "Partition Index", expectedPartition.PartitionIndex, actualPartition.PartitionIndex)
					return a
				}
				a.logger.Successf("✓ PartitionResponse[%d] Partition Index: %d", j, actualPartition.PartitionIndex)
			}

		}
	}

	return a
}

func (a DescribeTopicPartitionsResponseAssertion) Run() error {
	// firstLevelFields: ["ThrottleTimeMs"]
	// secondLevelFields (Topics): ["Name", "TopicID", "Partitions"]
	// thirdLevelFields (Partitions): ["ID", "Leader", "Replicas", "Isr"]
	return a.err
}
