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

// AssertBody asserts the contents of the response body
// Fields asserted by default: ThrottleTimeMs
func (a *DescribeTopicPartitionsResponseAssertion) AssertBody(excludedFields ...string) *DescribeTopicPartitionsResponseAssertion {
	if a.err != nil {
		return a
	}
	if !Contains(excludedFields, "ThrottleTimeMs") {
		if a.ActualValue.ThrottleTimeMs != a.ExpectedValue.ThrottleTimeMs {
			a.err = fmt.Errorf("Expected %s to be %d, got %d", "ThrottleTimeMs", a.ExpectedValue.ThrottleTimeMs, a.ActualValue.ThrottleTimeMs)
			return a
		}
		a.logger.Successf("✓ Throttle Time: %d", a.ActualValue.ThrottleTimeMs)
	}

	return a
}

type AssertTopicsExcludedFields struct {
	ExcludedTopicFields     []string
	ExcludedPartitionFields []string
}

// AssertTopics asserts the contents of the topics array in the response body
// Fields asserted by default: ErrorCode, Name, TopicID, TopicAuthorizedOperations
// Fields can be excluded by passing the field names in a AssertTopicsExcludedFields struct
// If ExcludedPartitionFields in the struct is nil, the partitions array will be asserted
// assertPartitions asserts the contents of the partitions array in the topic response
// Fields asserted by default: ErrorCode, PartitionIndex
func (a *DescribeTopicPartitionsResponseAssertion) AssertTopics(excludedFields AssertTopicsExcludedFields) *DescribeTopicPartitionsResponseAssertion {
	if a.err != nil {
		return a
	}

	if len(a.ActualValue.Topics) != len(a.ExpectedValue.Topics) {
		a.err = fmt.Errorf("Expected %s to be %d, got %d", "topics.length", len(a.ExpectedValue.Topics), len(a.ActualValue.Topics))
		return a
	}

	excludedTopicFields := excludedFields.ExcludedTopicFields
	excludedPartitionFields := excludedFields.ExcludedPartitionFields

	for i, actualTopic := range a.ActualValue.Topics {
		expectedTopic := a.ExpectedValue.Topics[i]
		if !Contains(excludedTopicFields, "ErrorCode") {
			if actualTopic.ErrorCode != expectedTopic.ErrorCode {
				a.err = fmt.Errorf("Expected %s to be %d, got %d", fmt.Sprintf("TopicResponse[%d] Error Code", i), expectedTopic.ErrorCode, actualTopic.ErrorCode)
				return a
			}
			protocol.SuccessLogWithIndentation(a.logger, 1, "✓ TopicResponse[%d] Error code: %d", i, actualTopic.ErrorCode)
		}

		if !Contains(excludedTopicFields, "Name") {
			if actualTopic.Name != expectedTopic.Name {
				a.err = fmt.Errorf("Expected %s to be %s, got %s", fmt.Sprintf("TopicResponse[%d] Topic Name", i), expectedTopic.Name, actualTopic.Name)
				return a
			}
			protocol.SuccessLogWithIndentation(a.logger, 1, "✓ TopicResponse[%d] Topic Name: %s", i, actualTopic.Name)
		}

		if !Contains(excludedTopicFields, "TopicID") {
			if actualTopic.TopicID != expectedTopic.TopicID {
				a.err = fmt.Errorf("Expected %s to be %s, got %s", fmt.Sprintf("TopicResponse[%d] Topic UUID", i), expectedTopic.TopicID, actualTopic.TopicID)
				return a
			}
			protocol.SuccessLogWithIndentation(a.logger, 1, "✓ TopicResponse[%d] Topic UUID: %s", i, actualTopic.TopicID)
		}

		if !Contains(excludedTopicFields, "TopicAuthorizedOperations") {
			if actualTopic.TopicAuthorizedOperations != expectedTopic.TopicAuthorizedOperations {
				a.err = fmt.Errorf("Expected %s to be %d, got %d", fmt.Sprintf("TopicResponse[%d] Topic Authorized Operations", i), expectedTopic.TopicAuthorizedOperations, actualTopic.TopicAuthorizedOperations)
				return a
			}
			protocol.SuccessLogWithIndentation(a.logger, 1, "✓ TopicResponse[%d] Topic Authorized Operations: %d", i, actualTopic.TopicAuthorizedOperations)
		}

		expectedPartitions := expectedTopic.Partitions
		actualPartitions := actualTopic.Partitions

		if excludedPartitionFields == nil {
			a.assertPartitions(expectedPartitions, actualPartitions, excludedPartitionFields)
		} else {
			if len(actualPartitions) != 0 {
				a.err = fmt.Errorf("Expected %s to be %d, got %d", "partitions.length", 0, len(actualPartitions))
				return a
			}
		}
	}

	return a
}

func (a *DescribeTopicPartitionsResponseAssertion) assertPartitions(expectedPartitions []kafkaapi.DescribeTopicPartitionsResponsePartition, actualPartitions []kafkaapi.DescribeTopicPartitionsResponsePartition, excludedFields []string) *DescribeTopicPartitionsResponseAssertion {
	if len(actualPartitions) != len(expectedPartitions) {
		a.err = fmt.Errorf("Expected %s to be %d, got %d", "partitions.length", len(expectedPartitions), len(actualPartitions))
		return a
	}

	for j, actualPartition := range actualPartitions {
		expectedPartition := expectedPartitions[j]

		if !Contains(excludedFields, "ErrorCode") {
			if actualPartition.ErrorCode != expectedPartition.ErrorCode {
				a.err = fmt.Errorf("Expected %s to be %d, got %d", fmt.Sprintf("PartitionResponse[%d] Error Code", j), expectedPartition.ErrorCode, actualPartition.ErrorCode)
				return a
			}
			protocol.SuccessLogWithIndentation(a.logger, 2, "✓ PartitionResponse[%d] Error code: %d", j, actualPartition.ErrorCode)
		}

		if !Contains(excludedFields, "PartitionIndex") {
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
	return a.err
}
