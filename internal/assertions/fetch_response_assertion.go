package assertions

import (
	"fmt"

	"github.com/codecrafters-io/kafka-tester/protocol"
	kafkaapi "github.com/codecrafters-io/kafka-tester/protocol/api"
	"github.com/codecrafters-io/tester-utils/logger"
)

type FetchResponseAssertion struct {
	ActualValue   kafkaapi.FetchResponse
	ExpectedValue kafkaapi.FetchResponse
	logger        *logger.Logger
	err           error
}

func NewFetchResponseAssertion(actualValue kafkaapi.FetchResponse, expectedValue kafkaapi.FetchResponse, logger *logger.Logger) *FetchResponseAssertion {
	return &FetchResponseAssertion{
		ActualValue:   actualValue,
		ExpectedValue: expectedValue,
		logger:        logger,
	}
}

func (a *FetchResponseAssertion) AssertBody(fields []string) *FetchResponseAssertion {
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

	if Contains(fields, "ErrorCode") {
		if a.ActualValue.ErrorCode != a.ExpectedValue.ErrorCode {
			a.err = fmt.Errorf("Expected %s to be %d, got %d", "ErrorCode", a.ExpectedValue.ErrorCode, a.ActualValue.ErrorCode)
			return a
		}
		a.logger.Successf("✓ Error Code: %d", a.ActualValue.ErrorCode)
	}

	if Contains(fields, "SessionID") {
		if a.ActualValue.SessionID != a.ExpectedValue.SessionID {
			a.err = fmt.Errorf("Expected %s to be %d, got %d", "SessionID", a.ExpectedValue.SessionID, a.ActualValue.SessionID)
			return a
		}
		a.logger.Successf("✓ Session ID: %d", a.ActualValue.SessionID)
	}

	return a
}

func (a *FetchResponseAssertion) AssertTopics(topicFields []string, partitionFields []string, recordBatchFields []string, recordFields []string) *FetchResponseAssertion {
	if a.err != nil {
		return a
	}

	if len(a.ActualValue.TopicResponses) != len(a.ExpectedValue.TopicResponses) {
		a.err = fmt.Errorf("Expected %s to be %d, got %d", "topics.length", len(a.ExpectedValue.TopicResponses), len(a.ActualValue.TopicResponses))
		return a
	}

	for i, actualTopic := range a.ActualValue.TopicResponses {
		expectedTopic := a.ExpectedValue.TopicResponses[i]
		if Contains(topicFields, "Topic") {
			if actualTopic.Topic != expectedTopic.Topic {
				a.err = fmt.Errorf("Expected %s to be %s, got %s", fmt.Sprintf("TopicResponse[%d] Topic UUID", i), expectedTopic.Topic, actualTopic.Topic)
				return a
			}
			protocol.SuccessLogWithIndentation(a.logger, 1, "✓ TopicResponse[%d] Topic UUID: %s", i, actualTopic.Topic)
		}

		expectedPartitions := expectedTopic.PartitionResponses
		actualPartitions := actualTopic.PartitionResponses

		if (partitionFields) != nil {
			a.assertPartitions(expectedPartitions, actualPartitions, partitionFields)
		} else {
			if len(actualPartitions) != 0 {
				a.err = fmt.Errorf("Expected %s to be %d, got %d", "partitions.length", 0, len(actualPartitions))
				return a
			}
		}
	}

	return a
}

func (a *FetchResponseAssertion) assertPartitions(expectedPartitions []kafkaapi.PartitionResponse, actualPartitions []kafkaapi.PartitionResponse, fields []string) *FetchResponseAssertion {
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

	return a
}

func (a FetchResponseAssertion) Run() error {
	// firstLevelFields: ["ThrottleTimeMs", "ErrorCode", "SessionID"]
	// secondLevelFields (Topics): ["ErrorCode", "Name", "TopicID", "TopicAuthorizedOperations"]
	// thirdLevelFields (Partitions): ["ErrorCode, "PartitionIndex"]
	return a.err
}
