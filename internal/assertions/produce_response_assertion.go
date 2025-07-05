package assertions

import (
	"fmt"

	"github.com/codecrafters-io/kafka-tester/protocol"
	kafkaapi "github.com/codecrafters-io/kafka-tester/protocol/api"
	"github.com/codecrafters-io/tester-utils/logger"
)

type ProduceResponseAssertion struct {
	ActualValue   kafkaapi.ProduceResponseBody
	ExpectedValue kafkaapi.ProduceResponseBody
	logger        *logger.Logger
	err           error
}

func NewProduceResponseAssertion(actualValue kafkaapi.ProduceResponseBody, expectedValue kafkaapi.ProduceResponseBody, logger *logger.Logger) *ProduceResponseAssertion {
	return &ProduceResponseAssertion{
		ActualValue:   actualValue,
		ExpectedValue: expectedValue,
		logger:        logger,
	}
}

func (a *ProduceResponseAssertion) AssertBody(fields []string) *ProduceResponseAssertion {
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

func (a *ProduceResponseAssertion) AssertTopics(topicFields []string, partitionFields []string) *ProduceResponseAssertion {
	if a.err != nil {
		return a
	}

	if len(a.ActualValue.Responses) != len(a.ExpectedValue.Responses) {
		a.err = fmt.Errorf("Expected %s to be %d, got %d", "topics.length", len(a.ExpectedValue.Responses), len(a.ActualValue.Responses))
		return a
	}

	for i, actualTopic := range a.ActualValue.Responses {
		expectedTopic := a.ExpectedValue.Responses[i]

		if Contains(topicFields, "Name") {
			if actualTopic.Name != expectedTopic.Name {
				a.err = fmt.Errorf("Expected %s to be %s, got %s", fmt.Sprintf("TopicResponse[%d] Name", i), expectedTopic.Name, actualTopic.Name)
				return a
			}
			protocol.SuccessLogWithIndentation(a.logger, 1, "✓ TopicResponse[%d] Name: %s", i, actualTopic.Name)
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

func (a *ProduceResponseAssertion) assertPartitions(expectedPartitions []kafkaapi.ProducePartitionResponse, actualPartitions []kafkaapi.ProducePartitionResponse, fields []string) *ProduceResponseAssertion {
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

			errorCodeName, ok := errorCodes[int(actualPartition.ErrorCode)]
			if !ok {
				errorCodeName = "UNKNOWN"
			}

			protocol.SuccessLogWithIndentation(a.logger, 2, "✓ PartitionResponse[%d] Error code: %d (%s)", j, actualPartition.ErrorCode, errorCodeName)
		}

		if Contains(fields, "Index") {
			if actualPartition.Index != expectedPartition.Index {
				a.err = fmt.Errorf("Expected %s to be %d, got %d", fmt.Sprintf("Partition Response[%d] Index", j), expectedPartition.Index, actualPartition.Index)
				return a
			}
			protocol.SuccessLogWithIndentation(a.logger, 2, "✓ PartitionResponse[%d] Index: %d", j, actualPartition.Index)
		}

		if Contains(fields, "BaseOffset") {
			if actualPartition.BaseOffset != expectedPartition.BaseOffset {
				a.err = fmt.Errorf("Expected %s to be %d, got %d", fmt.Sprintf("Partition Response[%d] BaseOffset", j), expectedPartition.BaseOffset, actualPartition.BaseOffset)
				return a
			}
			protocol.SuccessLogWithIndentation(a.logger, 2, "✓ PartitionResponse[%d] BaseOffset: %d", j, actualPartition.BaseOffset)
		}

		if Contains(fields, "LogStartOffset") {
			if actualPartition.LogStartOffset != expectedPartition.LogStartOffset {
				a.err = fmt.Errorf("Expected %s to be %d, got %d", fmt.Sprintf("Partition Response[%d] LogStartOffset", j), expectedPartition.LogStartOffset, actualPartition.LogStartOffset)
				return a
			}
			protocol.SuccessLogWithIndentation(a.logger, 2, "✓ PartitionResponse[%d] LogStartOffset: %d", j, actualPartition.LogStartOffset)
		}

		if Contains(fields, "LogAppendTimeMs") {
			if actualPartition.LogAppendTimeMs != expectedPartition.LogAppendTimeMs {
				a.err = fmt.Errorf("Expected %s to be %d, got %d", fmt.Sprintf("Partition Response[%d] LogAppendTimeMs", j), expectedPartition.LogAppendTimeMs, actualPartition.LogAppendTimeMs)
				return a
			}
			protocol.SuccessLogWithIndentation(a.logger, 2, "✓ PartitionResponse[%d] LogAppendTimeMs: %d", j, actualPartition.LogAppendTimeMs)
		}
	}

	return a
}

func (a ProduceResponseAssertion) Run() error {
	// firstLevelFields: ["ThrottleTimeMs"]
	// secondLevelFields (Topics): ["Name"]
	// thirdLevelFields (Partitions): ["ErrorCode", "Index", "BaseOffset", "LogStartOffset", "LogAppendTimeMs"]
	return a.err
}
