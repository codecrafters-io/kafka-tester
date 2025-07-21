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

	// empty slice = assert all fields (default)
	// non-empty slice = assert with exclusions
	// if excludedBodyFields contains "topics", then Topics are not asserted
	// if excludedTopicFields contains "partitions", then Partitions are not asserted
	excludedBodyFields      []string
	excludedTopicFields     []string
	excludedPartitionFields []string
}

func NewDescribeTopicPartitionsResponseAssertion(actualValue kafkaapi.DescribeTopicPartitionsResponse, expectedValue kafkaapi.DescribeTopicPartitionsResponse, logger *logger.Logger) *DescribeTopicPartitionsResponseAssertion {
	return &DescribeTopicPartitionsResponseAssertion{
		ActualValue:             actualValue,
		ExpectedValue:           expectedValue,
		logger:                  logger,
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

func (a *DescribeTopicPartitionsResponseAssertion) SkipPartitionFields() *DescribeTopicPartitionsResponseAssertion {
	a.excludedTopicFields = append(a.excludedTopicFields, "partitions")
	return a
}

// AssertBody asserts the contents of the response body
// Fields asserted by default: ThrottleTimeMs
func (a *DescribeTopicPartitionsResponseAssertion) AssertBody() *DescribeTopicPartitionsResponseAssertion {
	if a.err != nil {
		return a
	}
	if !Contains(a.excludedBodyFields, "ThrottleTimeMs") {
		if a.ActualValue.ThrottleTimeMs != a.ExpectedValue.ThrottleTimeMs {
			a.err = fmt.Errorf("Expected %s to be %d, got %d", "ThrottleTimeMs", a.ExpectedValue.ThrottleTimeMs, a.ActualValue.ThrottleTimeMs)
			return a
		}
		a.logger.Successf("✓ Throttle Time: %d", a.ActualValue.ThrottleTimeMs)
	}

	if !Contains(a.excludedBodyFields, "topics") {
		a.assertTopics()
	}

	return a
}

// assertTopics asserts the contents of the topics array in the response body
// and the partitions array in the topic response (if skipPartitionFields is not called)
// Fields asserted by default: ErrorCode, Name, TopicID,
// Partitions.Length, Partitions.ErrorCode, Partitions.PartitionIndex
func (a *DescribeTopicPartitionsResponseAssertion) assertTopics() *DescribeTopicPartitionsResponseAssertion {
	if a.err != nil {
		return a
	}

	if len(a.ActualValue.Topics) != len(a.ExpectedValue.Topics) {
		a.err = fmt.Errorf("Expected %s to be %d, got %d", "topics.length", len(a.ExpectedValue.Topics), len(a.ActualValue.Topics))
		return a
	}

	for i, actualTopic := range a.ActualValue.Topics {
		expectedTopic := a.ExpectedValue.Topics[i]
		if !Contains(a.excludedTopicFields, "ErrorCode") {
			if actualTopic.ErrorCode != expectedTopic.ErrorCode {
				a.err = fmt.Errorf("Expected %s to be %d, got %d", fmt.Sprintf("TopicResponse[%d] Error Code", i), expectedTopic.ErrorCode, actualTopic.ErrorCode)
				return a
			}
			protocol.SuccessLogWithIndentation(a.logger, 1, "✓ TopicResponse[%d] Error code: %d", i, actualTopic.ErrorCode)
		}

		if !Contains(a.excludedTopicFields, "Name") {
			if actualTopic.Name != expectedTopic.Name {
				a.err = fmt.Errorf("Expected %s to be %s, got %s", fmt.Sprintf("TopicResponse[%d] Topic Name", i), expectedTopic.Name, actualTopic.Name)
				return a
			}
			protocol.SuccessLogWithIndentation(a.logger, 1, "✓ TopicResponse[%d] Topic Name: %s", i, actualTopic.Name)
		}

		if !Contains(a.excludedTopicFields, "TopicID") {
			if actualTopic.TopicID != expectedTopic.TopicID {
				a.err = fmt.Errorf("Expected %s to be %s, got %s", fmt.Sprintf("TopicResponse[%d] Topic UUID", i), expectedTopic.TopicID, actualTopic.TopicID)
				return a
			}
			protocol.SuccessLogWithIndentation(a.logger, 1, "✓ TopicResponse[%d] Topic UUID: %s", i, actualTopic.TopicID)
		}

		expectedPartitions := expectedTopic.Partitions
		actualPartitions := actualTopic.Partitions

		if !Contains(a.excludedTopicFields, "partitions") {
			a.assertPartitions(expectedPartitions, actualPartitions)
		}
	}

	return a
}

func (a *DescribeTopicPartitionsResponseAssertion) assertPartitions(expectedPartitions []kafkaapi.DescribeTopicPartitionsResponsePartition, actualPartitions []kafkaapi.DescribeTopicPartitionsResponsePartition) *DescribeTopicPartitionsResponseAssertion {
	if !Contains(a.excludedPartitionFields, "Length") {
		if len(actualPartitions) != len(expectedPartitions) {
			a.err = fmt.Errorf("Expected %s to be %d, got %d", "partitions.length", len(expectedPartitions), len(actualPartitions))
			return a
		}
	}

	for j, actualPartition := range actualPartitions {
		expectedPartition := expectedPartitions[j]

		if !Contains(a.excludedPartitionFields, "ErrorCode") {
			if actualPartition.ErrorCode != expectedPartition.ErrorCode {
				a.err = fmt.Errorf("Expected %s to be %d, got %d", fmt.Sprintf("PartitionResponse[%d] Error Code", j), expectedPartition.ErrorCode, actualPartition.ErrorCode)
				return a
			}
			protocol.SuccessLogWithIndentation(a.logger, 2, "✓ PartitionResponse[%d] Error code: %d", j, actualPartition.ErrorCode)
		}

		if !Contains(a.excludedPartitionFields, "PartitionIndex") {
			if actualPartition.PartitionIndex != expectedPartition.PartitionIndex {
				a.err = fmt.Errorf("Expected %s to be %d, got %d", fmt.Sprintf("Partition Response[%d] Partition Index", j), expectedPartition.PartitionIndex, actualPartition.PartitionIndex)
				return a
			}
			protocol.SuccessLogWithIndentation(a.logger, 2, "✓ PartitionResponse[%d] Partition Index: %d", j, actualPartition.PartitionIndex)
		}
	}

	return a
}

// AssertEmptyPartitions asserts that all topics have empty partitions arrays
func (a *DescribeTopicPartitionsResponseAssertion) AssertEmptyPartitions() *DescribeTopicPartitionsResponseAssertion {
	if a.err != nil {
		return a
	}

	for i, actualTopic := range a.ActualValue.Topics {
		if len(actualTopic.Partitions) != 0 {
			a.err = fmt.Errorf("Expected topic[%d] partitions to be empty, got %d partitions", i, len(actualTopic.Partitions))
			return a
		}
		protocol.SuccessLogWithIndentation(a.logger, 1, "✓ TopicResponse[%d] has empty partitions", i)
	}

	return a
}

func (a *DescribeTopicPartitionsResponseAssertion) Run() error {
	return a.err
}
