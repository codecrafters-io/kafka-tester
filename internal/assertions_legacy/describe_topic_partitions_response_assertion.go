package assertions_legacy

import (
	"fmt"

	"github.com/codecrafters-io/kafka-tester/protocol"
	"github.com/codecrafters-io/kafka-tester/protocol/kafkaapi_legacy"
	"github.com/codecrafters-io/tester-utils/logger"
)

type DescribeTopicPartitionsResponseAssertion struct {
	ActualValue             kafkaapi_legacy.DescribeTopicPartitionsResponse
	ExpectedValue           kafkaapi_legacy.DescribeTopicPartitionsResponse
	excludedBodyFields      []string
	excludedTopicFields     []string
	excludedPartitionFields []string
}

var DTP_EXCLUDABLE_BODY_FIELDS = []string{"ThrottleTimeMs", "Topics"}
var DTP_EXCLUDABLE_TOPIC_FIELDS = []string{"ErrorCode", "Name", "TopicID", "Partitions"}
var DTP_EXCLUDABLE_PARTITION_FIELDS = []string{"ErrorCode", "PartitionIndex"}

func NewDescribeTopicPartitionsResponseAssertion(actualValue kafkaapi_legacy.DescribeTopicPartitionsResponse, expectedValue kafkaapi_legacy.DescribeTopicPartitionsResponse) *DescribeTopicPartitionsResponseAssertion {
	return &DescribeTopicPartitionsResponseAssertion{
		ActualValue:             actualValue,
		ExpectedValue:           expectedValue,
		excludedBodyFields:      []string{},
		excludedTopicFields:     []string{},
		excludedPartitionFields: []string{},
	}
}

func (a *DescribeTopicPartitionsResponseAssertion) ExcludeBodyFields(fields ...string) *DescribeTopicPartitionsResponseAssertion {
	mustValidateExclusions(fields, DTP_EXCLUDABLE_BODY_FIELDS)

	a.excludedBodyFields = fields
	return a
}

func (a *DescribeTopicPartitionsResponseAssertion) ExcludeTopicFields(fields ...string) *DescribeTopicPartitionsResponseAssertion {
	mustValidateExclusions(fields, DTP_EXCLUDABLE_TOPIC_FIELDS)

	a.excludedTopicFields = fields
	return a
}

func (a *DescribeTopicPartitionsResponseAssertion) ExcludePartitionFields(fields ...string) *DescribeTopicPartitionsResponseAssertion {
	mustValidateExclusions(fields, DTP_EXCLUDABLE_PARTITION_FIELDS)

	a.excludedPartitionFields = fields
	return a
}

func (a *DescribeTopicPartitionsResponseAssertion) SkipTopicsAndPartitions() *DescribeTopicPartitionsResponseAssertion {
	a.excludedBodyFields = append(a.excludedBodyFields, "Topics")
	return a
}

func (a *DescribeTopicPartitionsResponseAssertion) SkipPartitions() *DescribeTopicPartitionsResponseAssertion {
	a.excludedTopicFields = append(a.excludedTopicFields, "Partitions")
	return a
}

func (a *DescribeTopicPartitionsResponseAssertion) assertThrottleTimeMs(logger *logger.Logger) error {
	if !Contains(a.excludedBodyFields, "ThrottleTimeMs") {
		if a.ActualValue.ThrottleTimeMs != a.ExpectedValue.ThrottleTimeMs {
			return fmt.Errorf("Expected ThrottleTimeMs to be %d, got %d", a.ExpectedValue.ThrottleTimeMs, a.ActualValue.ThrottleTimeMs)
		}
		logger.Successf("✓ Throttle Time: %d", a.ActualValue.ThrottleTimeMs)
	}

	return nil
}

// assertTopics asserts the contents of the topics array in the response body
// and the partitions array in the topic response (if skipPartitionFields is not called)
// Fields asserted by default: ErrorCode, Name, TopicID,
// Partitions.Length, Partitions.ErrorCode, Partitions.PartitionIndex
func (a *DescribeTopicPartitionsResponseAssertion) assertTopics(logger *logger.Logger) error {
	if len(a.ActualValue.Topics) != len(a.ExpectedValue.Topics) {
		return fmt.Errorf("Expected topics.length to be %d, got %d", len(a.ExpectedValue.Topics), len(a.ActualValue.Topics))
	}

	if len(a.ActualValue.Topics) == 0 {
		protocol.SuccessLogWithIndentation(logger, 0, "✓ Response body has no topic response")
	} else {
		protocol.SuccessLogWithIndentation(logger, 0, "✓ Response body has %d topic responses", len(a.ActualValue.Topics))
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

		if !Contains(a.excludedTopicFields, "Partitions") {
			if err := a.assertPartitions(expectedPartitions, actualPartitions, i, logger); err != nil {
				return err
			}
		}
	}

	return nil
}

func (a *DescribeTopicPartitionsResponseAssertion) assertPartitions(expectedPartitions []kafkaapi_legacy.DescribeTopicPartitionsResponsePartition, actualPartitions []kafkaapi_legacy.DescribeTopicPartitionsResponsePartition, topicPartitionIndex int, logger *logger.Logger) error {
	if len(actualPartitions) != len(expectedPartitions) {
		return fmt.Errorf("Expected partitions.length to be %d, got %d", len(expectedPartitions), len(actualPartitions))
	}

	if len(actualPartitions) == 0 {
		protocol.SuccessLogWithIndentation(logger, 1, "✓ TopicResponse[%d] has empty partitions", topicPartitionIndex)
	} else {
		protocol.SuccessLogWithIndentation(logger, 1, "✓ TopicResponse[%d] has %d partition(s)", topicPartitionIndex, len(actualPartitions))
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

func (a *DescribeTopicPartitionsResponseAssertion) Run(logger *logger.Logger) error {
	if err := a.assertThrottleTimeMs(logger); err != nil {
		return err
	}

	if !Contains(a.excludedBodyFields, "Topics") {
		if err := a.assertTopics(logger); err != nil {
			return err
		}
	}

	return nil
}
