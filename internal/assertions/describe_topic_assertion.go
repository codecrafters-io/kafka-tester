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
}

func (a DescribeTopicPartitionsResponseAssertion) Evaluate(firstLevelFields, secondLevelFields, thirdLevelFields []string, logger *logger.Logger) error {
	// firstLevelFields: ["ThrottleTimeMs"]
	// secondLevelFields (Topics): ["Name", "TopicID", "Partitions"]
	// thirdLevelFields (Partitions): ["ID", "Leader", "Replicas", "Isr"]

	if Contains(firstLevelFields, "ThrottleTimeMs") {
		if a.ActualValue.ThrottleTimeMs != a.ExpectedValue.ThrottleTimeMs {
			return fmt.Errorf("Expected %s to be %d, got %d", "ThrottleTimeMs", a.ExpectedValue.ThrottleTimeMs, a.ActualValue.ThrottleTimeMs)
		}
		logger.Successf("✓ Throttle Time: %d", a.ActualValue.ThrottleTimeMs)
	}

	if secondLevelFields != nil {
		// If secondLevelFields is not nil, then we match the length of the topics array
		if len(a.ActualValue.Topics) != len(secondLevelFields) {
			return fmt.Errorf("Expected %s to have length %d, got %d", "TopicResponse", len(a.ExpectedValue.Topics), len(a.ActualValue.Topics))
		}

		// We will also assert each topic in the actual and expected arrays respectively
		for i, actualTopic := range a.ActualValue.Topics {
			expectedTopic := a.ExpectedValue.Topics[i]

			if Contains(secondLevelFields, "ErrorCode") {
				if actualTopic.ErrorCode != expectedTopic.ErrorCode {
					return fmt.Errorf("Expected %s to be %d, got %d", fmt.Sprintf("TopicResponse[%d] Error Code", i), expectedTopic.ErrorCode, actualTopic.ErrorCode)
				}

				errorCodeName, ok := errorCodes[int(actualTopic.ErrorCode)]
				if !ok {
					errorCodeName = "UNKNOWN"
				}
				logger.Successf("✓ TopicResponse[%d] Error code: %d (%s)", i, actualTopic.ErrorCode, errorCodeName)
			}

			if Contains(secondLevelFields, "Name") {
				if actualTopic.Name != expectedTopic.Name {
					return fmt.Errorf("Expected %s to be %s, got %s", fmt.Sprintf("TopicResponse[%d] Topic Name", i), expectedTopic.Name, actualTopic.Name)
				}
				logger.Successf("✓ TopicResponse[%d] Topic Name: %s", i, actualTopic.Name)
			}

			if Contains(secondLevelFields, "TopicID") {
				if actualTopic.TopicID != expectedTopic.TopicID {
					return fmt.Errorf("Expected %s to be %s, got %s", fmt.Sprintf("TopicResponse[%d] Topic UUID", i), expectedTopic.TopicID, actualTopic.TopicID)
				}
				logger.Successf("✓ TopicResponse[%d] Topic UUID: %s", i, actualTopic.TopicID)
			}

			if thirdLevelFields != nil {
				// If thirdLevelFields is not nil, then we match the length of the partitions array
				if len(actualTopic.Partitions) != len(expectedTopic.Partitions) {
					return fmt.Errorf("Expected %s to have length %d, got %d", "PartitionResponse", len(expectedTopic.Partitions), len(actualTopic.Partitions))
				}
			}
		}
	}

	return nil
}
