package assertions

import (
	"fmt"
	"sort"

	"github.com/codecrafters-io/kafka-tester/protocol"
	kafkaapi "github.com/codecrafters-io/kafka-tester/protocol/api"
	"github.com/codecrafters-io/tester-utils/logger"
)

type ProduceResponseAssertion struct {
	ActualValue             kafkaapi.ProduceResponse
	ExpectedValue           kafkaapi.ProduceResponse
	excludedBodyFields      []string
	excludedTopicFields     []string
	excludedPartitionFields []string
}

func NewProduceResponseAssertion(actualValue kafkaapi.ProduceResponse, expectedValue kafkaapi.ProduceResponse, logger *logger.Logger) *ProduceResponseAssertion {
	// Sort both responses for consistent comparison
	actualValue.Body = sortResponseBodies(actualValue.Body)
	expectedValue.Body = sortResponseBodies(expectedValue.Body)

	return &ProduceResponseAssertion{
		ActualValue:             actualValue,
		ExpectedValue:           expectedValue,
		excludedBodyFields:      []string{},
		excludedTopicFields:     []string{},
		excludedPartitionFields: []string{},
	}
}

func (a *ProduceResponseAssertion) ExcludeBodyFields(fields ...string) *ProduceResponseAssertion {
	a.excludedBodyFields = fields
	return a
}

func (a *ProduceResponseAssertion) ExcludeTopicFields(fields ...string) *ProduceResponseAssertion {
	a.excludedTopicFields = fields
	return a
}

func (a *ProduceResponseAssertion) ExcludePartitionFields(fields ...string) *ProduceResponseAssertion {
	a.excludedPartitionFields = fields
	return a
}

func (a *ProduceResponseAssertion) SkipTopicFields() *ProduceResponseAssertion {
	a.excludedBodyFields = append(a.excludedBodyFields, "Topics")
	return a
}

func (a *ProduceResponseAssertion) SkipPartitionFields() *ProduceResponseAssertion {
	a.excludedTopicFields = append(a.excludedTopicFields, "Partitions")
	return a
}

func (a *ProduceResponseAssertion) assertBody(logger *logger.Logger) error {
	if !Contains(a.excludedBodyFields, "ThrottleTimeMs") {
		if a.ActualValue.Body.ThrottleTimeMs != a.ExpectedValue.Body.ThrottleTimeMs {
			return fmt.Errorf("Expected %s to be %d, got %d", "ThrottleTimeMs", a.ExpectedValue.Body.ThrottleTimeMs, a.ActualValue.Body.ThrottleTimeMs)
		}
		protocol.SuccessLogWithIndentation(logger, 0, "✓ ThrottleTimeMs: %d", a.ActualValue.Body.ThrottleTimeMs)
	}

	if !Contains(a.excludedBodyFields, "Topics") {
		if err := a.assertTopics(logger); err != nil {
			return err
		}
	}

	return nil
}

func (a *ProduceResponseAssertion) assertTopics(logger *logger.Logger) error {
	if len(a.ActualValue.Body.TopicResponses) != len(a.ExpectedValue.Body.TopicResponses) {
		return fmt.Errorf("Expected Topics.length to be %d, got %d", len(a.ExpectedValue.Body.TopicResponses), len(a.ActualValue.Body.TopicResponses))
	}

	for i, actualTopic := range a.ActualValue.Body.TopicResponses {
		expectedTopic := a.ExpectedValue.Body.TopicResponses[i]

		if !Contains(a.excludedTopicFields, "Name") {
			if actualTopic.Name != expectedTopic.Name {
				return fmt.Errorf("Expected TopicResponse[%d] Name to be %s, got %s", i, expectedTopic.Name, actualTopic.Name)
			}
			protocol.SuccessLogWithIndentation(logger, 1, "✓ TopicResponse[%d] Name: %s", i, actualTopic.Name)
		}

		expectedPartitions := expectedTopic.PartitionResponses
		actualPartitions := actualTopic.PartitionResponses

		if !Contains(a.excludedTopicFields, "Partitions") {
			if err := a.assertPartitions(expectedPartitions, actualPartitions, logger); err != nil {
				return err
			}
		}
	}

	return nil
}

func (a *ProduceResponseAssertion) assertPartitions(expectedPartitions []kafkaapi.ProducePartitionResponse, actualPartitions []kafkaapi.ProducePartitionResponse, logger *logger.Logger) error {
	if len(actualPartitions) != len(expectedPartitions) {
		return fmt.Errorf("Expected %s to be %d, got %d", "partitions.length", len(expectedPartitions), len(actualPartitions))
	}

	for j, actualPartition := range actualPartitions {
		expectedPartition := expectedPartitions[j]

		if !Contains(a.excludedPartitionFields, "ErrorCode") {
			if actualPartition.ErrorCode != expectedPartition.ErrorCode {
				return fmt.Errorf("Expected %s to be %d, got %d", fmt.Sprintf("PartitionResponse[%d] Error Code", j), expectedPartition.ErrorCode, actualPartition.ErrorCode)
			}

			errorCodeName, ok := errorCodes[int(actualPartition.ErrorCode)]
			if !ok {
				panic(fmt.Sprintf("CodeCrafters Internal Error: Expected %d to be in errorCodes map", actualPartition.ErrorCode))
			}

			protocol.SuccessLogWithIndentation(logger, 2, "✓ PartitionResponse[%d] ErrorCode: %d (%s)", j, actualPartition.ErrorCode, errorCodeName)
		}

		if !Contains(a.excludedPartitionFields, "Index") {
			if actualPartition.Index != expectedPartition.Index {
				return fmt.Errorf("Expected %s to be %d, got %d", fmt.Sprintf("Partition Response[%d] Index", j), expectedPartition.Index, actualPartition.Index)
			}
			protocol.SuccessLogWithIndentation(logger, 2, "✓ PartitionResponse[%d] Index: %d", j, actualPartition.Index)
		}

		if !Contains(a.excludedPartitionFields, "BaseOffset") {
			if actualPartition.BaseOffset != expectedPartition.BaseOffset {
				return fmt.Errorf("Expected %s to be %d, got %d", fmt.Sprintf("Partition Response[%d] BaseOffset", j), expectedPartition.BaseOffset, actualPartition.BaseOffset)
			}
			protocol.SuccessLogWithIndentation(logger, 2, "✓ PartitionResponse[%d] BaseOffset: %d", j, actualPartition.BaseOffset)
		}

		if !Contains(a.excludedPartitionFields, "LogStartOffset") {
			if actualPartition.LogStartOffset != expectedPartition.LogStartOffset {
				return fmt.Errorf("Expected %s to be %d, got %d", fmt.Sprintf("Partition Response[%d] LogStartOffset", j), expectedPartition.LogStartOffset, actualPartition.LogStartOffset)
			}
			protocol.SuccessLogWithIndentation(logger, 2, "✓ PartitionResponse[%d] LogStartOffset: %d", j, actualPartition.LogStartOffset)
		}

		if !Contains(a.excludedPartitionFields, "LogAppendTimeMs") {
			if actualPartition.LogAppendTimeMs != expectedPartition.LogAppendTimeMs {
				return fmt.Errorf("Expected %s to be %d, got %d", fmt.Sprintf("Partition Response[%d] LogAppendTimeMs", j), expectedPartition.LogAppendTimeMs, actualPartition.LogAppendTimeMs)
			}
			protocol.SuccessLogWithIndentation(logger, 2, "✓ PartitionResponse[%d] LogAppendTimeMs: %d", j, actualPartition.LogAppendTimeMs)
		}
	}

	return nil
}

func (a *ProduceResponseAssertion) Run(logger *logger.Logger) error {
	// firstLevelFields: ["ThrottleTimeMs"]
	// secondLevelFields (Topics): ["Name"]
	// thirdLevelFields (Partitions): ["ErrorCode", "Index", "BaseOffset", "LogStartOffset", "LogAppendTimeMs"]
	if err := NewResponseHeaderAssertion(a.ActualValue.Header, a.ExpectedValue.Header).Run(logger); err != nil {
		return err
	}

	if err := a.assertBody(logger); err != nil {
		return err
	}

	return nil
}

// sortResponseBodies sorts topics by name and partitions within each topic by index
func sortResponseBodies(response kafkaapi.ProduceResponseBody) kafkaapi.ProduceResponseBody {
	sortedResponse := response
	sortedResponse.TopicResponses = make([]kafkaapi.ProduceTopicResponse, len(response.TopicResponses))
	copy(sortedResponse.TopicResponses, response.TopicResponses)

	// Sort topics by name
	sort.Slice(sortedResponse.TopicResponses, func(i, j int) bool {
		return sortedResponse.TopicResponses[i].Name < sortedResponse.TopicResponses[j].Name
	})

	// Sort partitions within each topic by index
	for i := range sortedResponse.TopicResponses {
		partitions := make([]kafkaapi.ProducePartitionResponse, len(sortedResponse.TopicResponses[i].PartitionResponses))
		copy(partitions, sortedResponse.TopicResponses[i].PartitionResponses)

		sort.Slice(partitions, func(x, y int) bool {
			return partitions[x].Index < partitions[y].Index
		})

		sortedResponse.TopicResponses[i].PartitionResponses = partitions
	}

	return sortedResponse
}
