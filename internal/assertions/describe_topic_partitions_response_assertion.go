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

	// nil = don't assert this level
	// empty slice = assert all fields (default)
	// non-empty slice = assert with exclusions
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

	return a
}

// AssertOnlyTopics asserts only the contents of the topics array in the response body
// Fields asserted by default: ErrorCode, Name, TopicID,
func (a *DescribeTopicPartitionsResponseAssertion) AssertOnlyTopics() *DescribeTopicPartitionsResponseAssertion {
	return a.assertTopicsAndPartitions(true)
}

// AssertTopicsAndPartitions asserts the contents of the topics array in the response body
// and the partitions array in the topic response
// Fields asserted by default: ErrorCode, Name, TopicID,
// Partitions.Length, Partitions.ErrorCode, Partitions.PartitionIndex
func (a *DescribeTopicPartitionsResponseAssertion) AssertTopicsAndPartitions() *DescribeTopicPartitionsResponseAssertion {
	return a.assertTopicsAndPartitions(false)
}

// assertTopicsAndPartitions is the internal function that is called by
// AssertOnlyTopics and AssertTopicsAndPartitions
// It asserts the contents of the topics array in the response body
// and the partitions array in the topic response (if skipPartitionsAssertion is false)
// Fields asserted by default: ErrorCode, Name, TopicID,
// Partitions.Length, Partitions.ErrorCode, Partitions.PartitionIndex
// As we want partition assertions logs adjacent to the topics they are in,
// we can't separate AssertPartitions out
func (a *DescribeTopicPartitionsResponseAssertion) assertTopicsAndPartitions(skipPartitionsAssertion bool) *DescribeTopicPartitionsResponseAssertion {
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

		if !skipPartitionsAssertion {
			expectedPartitions := expectedTopic.Partitions
			actualPartitions := actualTopic.Partitions
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
		}
	}

	return a
}

func (a *DescribeTopicPartitionsResponseAssertion) Run() error {
	return a.err
}
