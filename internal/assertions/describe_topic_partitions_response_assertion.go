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

	// empty slice = assert all fields (default)
	// non-empty slice = assert with exclusions
	// if excludedBodyFields contains "topics", then Topics are not asserted
	// if excludedTopicFields contains "partitions", then Partitions are not asserted
	excludedBodyFields      []string
	excludedTopicFields     []string
	excludedPartitionFields []string
}

func NewDescribeTopicPartitionsResponseAssertion(actualValue kafkaapi.DescribeTopicPartitionsResponse, expectedValue kafkaapi.DescribeTopicPartitionsResponse) *DescribeTopicPartitionsResponseAssertion {
	return &DescribeTopicPartitionsResponseAssertion{
		ActualValue:             actualValue,
		ExpectedValue:           expectedValue,
		excludedBodyFields:      []string{},
		excludedTopicFields:     []string{},
		excludedPartitionFields: []string{},
	}
}

func (a *DescribeTopicPartitionsResponseAssertion) ExcludeBodyFields(fields ...string) *DescribeTopicPartitionsResponseAssertion {
	a.excludedBodyFields = fields
	return a
}

func (a *DescribeTopicPartitionsResponseAssertion) ExcludeTopicFields(fields ...string) *DescribeTopicPartitionsResponseAssertion {
	a.excludedTopicFields = fields
	return a
}

func (a *DescribeTopicPartitionsResponseAssertion) ExcludePartitionFields(fields ...string) *DescribeTopicPartitionsResponseAssertion {
	a.excludedPartitionFields = fields
	return a
}

func (a *DescribeTopicPartitionsResponseAssertion) SkipTopicFields() *DescribeTopicPartitionsResponseAssertion {
	a.excludedBodyFields = append(a.excludedBodyFields, "topics")
	return a
}

func (a *DescribeTopicPartitionsResponseAssertion) SkipPartitionFields() *DescribeTopicPartitionsResponseAssertion {
	a.excludedTopicFields = append(a.excludedTopicFields, "partitions")
	return a
}

// assertBody asserts the contents of the response body
// Fields asserted by default: ThrottleTimeMs
func (a *DescribeTopicPartitionsResponseAssertion) assertBody(logger *logger.Logger) error {
	if !Contains(a.excludedBodyFields, "ThrottleTimeMs") {
		if a.ActualValue.ThrottleTimeMs != a.ExpectedValue.ThrottleTimeMs {
			return fmt.Errorf("Expected %s to be %d, got %d", "ThrottleTimeMs", a.ExpectedValue.ThrottleTimeMs, a.ActualValue.ThrottleTimeMs)
		}
		logger.Successf("✓ Throttle Time: %d", a.ActualValue.ThrottleTimeMs)
	}

	if !Contains(a.excludedBodyFields, "topics") {
		if err := a.assertTopics(logger); err != nil {
			return err
		}
	}

	return nil
}

// assertTopics asserts the contents of the topics array in the response body
// and the partitions array in the topic response (if skipPartitionFields is not called)
// Fields asserted by default: ErrorCode, Name, TopicID,
// Partitions.Length, Partitions.ErrorCode, Partitions.PartitionIndex
func (a *DescribeTopicPartitionsResponseAssertion) assertTopics(logger *logger.Logger) error {
	if len(a.ActualValue.Topics) != len(a.ExpectedValue.Topics) {
		return fmt.Errorf("Expected %s to be %d, got %d", "topics.length", len(a.ExpectedValue.Topics), len(a.ActualValue.Topics))
	}

	for i, actualTopic := range a.ActualValue.Topics {
		expectedTopic := a.ExpectedValue.Topics[i]
		if !Contains(a.excludedTopicFields, "ErrorCode") {
			if actualTopic.ErrorCode != expectedTopic.ErrorCode {
				return fmt.Errorf("Expected %s to be %d, got %d", fmt.Sprintf("TopicResponse[%d] Error Code", i), expectedTopic.ErrorCode, actualTopic.ErrorCode)
			}
			protocol.SuccessLogWithIndentation(logger, 1, "✓ TopicResponse[%d] Error code: %d", i, actualTopic.ErrorCode)
		}

		if !Contains(a.excludedTopicFields, "Name") {
			if actualTopic.Name != expectedTopic.Name {
				return fmt.Errorf("Expected %s to be %s, got %s", fmt.Sprintf("TopicResponse[%d] Topic Name", i), expectedTopic.Name, actualTopic.Name)
			}
			protocol.SuccessLogWithIndentation(logger, 1, "✓ TopicResponse[%d] Topic Name: %s", i, actualTopic.Name)
		}

		if !Contains(a.excludedTopicFields, "TopicID") {
			if actualTopic.TopicID != expectedTopic.TopicID {
				return fmt.Errorf("Expected %s to be %s, got %s", fmt.Sprintf("TopicResponse[%d] Topic UUID", i), expectedTopic.TopicID, actualTopic.TopicID)
			}
			protocol.SuccessLogWithIndentation(logger, 1, "✓ TopicResponse[%d] Topic UUID: %s", i, actualTopic.TopicID)
		}

		expectedPartitions := expectedTopic.Partitions
		actualPartitions := actualTopic.Partitions

		if !Contains(a.excludedTopicFields, "partitions") {
			if err := a.assertPartitions(expectedPartitions, actualPartitions, logger); err != nil {
				return err
			}
		}
	}

	return nil
}

func (a *DescribeTopicPartitionsResponseAssertion) assertPartitions(expectedPartitions []kafkaapi.DescribeTopicPartitionsResponsePartition, actualPartitions []kafkaapi.DescribeTopicPartitionsResponsePartition, logger *logger.Logger) error {
	if len(actualPartitions) != len(expectedPartitions) {
		return fmt.Errorf("Expected %s to be %d, got %d", "partitions.length", len(expectedPartitions), len(actualPartitions))
	}

	for j, actualPartition := range actualPartitions {
		expectedPartition := expectedPartitions[j]

		if !Contains(a.excludedPartitionFields, "ErrorCode") {
			if actualPartition.ErrorCode != expectedPartition.ErrorCode {
				return fmt.Errorf("Expected %s to be %d, got %d", fmt.Sprintf("PartitionResponse[%d] Error Code", j), expectedPartition.ErrorCode, actualPartition.ErrorCode)
			}
			protocol.SuccessLogWithIndentation(logger, 2, "✓ PartitionResponse[%d] Error code: %d", j, actualPartition.ErrorCode)
		}

		if !Contains(a.excludedPartitionFields, "PartitionIndex") {
			if actualPartition.PartitionIndex != expectedPartition.PartitionIndex {
				return fmt.Errorf("Expected %s to be %d, got %d", fmt.Sprintf("Partition Response[%d] Partition Index", j), expectedPartition.PartitionIndex, actualPartition.PartitionIndex)
			}
			protocol.SuccessLogWithIndentation(logger, 2, "✓ PartitionResponse[%d] Partition Index: %d", j, actualPartition.PartitionIndex)
		}
	}

	return nil
}

// assertEmptyPartitions asserts that all topics have empty partitions arrays
func (a *DescribeTopicPartitionsResponseAssertion) assertEmptyPartitions(logger *logger.Logger) error {
	for i, actualTopic := range a.ActualValue.Topics {
		if len(actualTopic.Partitions) != 0 {
			return fmt.Errorf("Expected topic[%d] partitions to be empty, got %d partitions", i, len(actualTopic.Partitions))
		}
		protocol.SuccessLogWithIndentation(logger, 1, "✓ TopicResponse[%d] has empty partitions", i)
	}

	return nil
}

func (a *DescribeTopicPartitionsResponseAssertion) Run(logger *logger.Logger) error {
	if err := a.assertBody(logger); err != nil {
		return err
	}
	if Contains(a.excludedTopicFields, "partitions") {
		if err := a.assertEmptyPartitions(logger); err != nil {
			return err
		}
	}

	return nil
}
