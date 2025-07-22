package assertions

import (
	"fmt"
	"sort"

	"github.com/codecrafters-io/kafka-tester/protocol"
	kafkaapi "github.com/codecrafters-io/kafka-tester/protocol/api"
	"github.com/codecrafters-io/tester-utils/logger"
)

type ProduceResponseAssertion struct {
	ActualValue   kafkaapi.ProduceResponseBody
	ExpectedValue kafkaapi.ProduceResponseBody

	// empty slice = assert all fields (default)
	// non-empty slice = assert with exclusions
	// if excludedBodyFields contains "topics", then Topics are not asserted
	// if excludedTopicFields contains "partitions", then Partitions are not asserted
	excludedBodyFields      []string
	excludedTopicFields     []string
	excludedPartitionFields []string
}

func NewProduceResponseAssertion(actualValue kafkaapi.ProduceResponseBody, expectedValue kafkaapi.ProduceResponseBody, logger *logger.Logger) *ProduceResponseAssertion {
	// Sort both responses for consistent comparison
	sortedActual := sortResponses(actualValue)
	sortedExpected := sortResponses(expectedValue)

	return &ProduceResponseAssertion{
		ActualValue:             sortedActual,
		ExpectedValue:           sortedExpected,
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
	a.excludedBodyFields = append(a.excludedBodyFields, "topics")
	return a
}

func (a *ProduceResponseAssertion) SkipPartitionFields() *ProduceResponseAssertion {
	a.excludedTopicFields = append(a.excludedTopicFields, "partitions")
	return a
}

func (a *ProduceResponseAssertion) assertBody(logger *logger.Logger) error {
	if !Contains(a.excludedBodyFields, "ThrottleTimeMs") {
		if a.ActualValue.ThrottleTimeMs != a.ExpectedValue.ThrottleTimeMs {
			return fmt.Errorf("Expected %s to be %d, got %d", "ThrottleTimeMs", a.ExpectedValue.ThrottleTimeMs, a.ActualValue.ThrottleTimeMs)
		}
		protocol.SuccessLogWithIndentation(logger, 0, "✓ Throttle Time: %d", a.ActualValue.ThrottleTimeMs)
	}

	if !Contains(a.excludedBodyFields, "topics") {
		if err := a.assertTopics(logger); err != nil {
			return err
		}
	}

	return nil
}

func (a *ProduceResponseAssertion) assertTopics(logger *logger.Logger) error {
	if len(a.ActualValue.Responses) != len(a.ExpectedValue.Responses) {
		return fmt.Errorf("Expected %s to be %d, got %d", "topics.length", len(a.ExpectedValue.Responses), len(a.ActualValue.Responses))
	}

	for i, actualTopic := range a.ActualValue.Responses {
		expectedTopic := a.ExpectedValue.Responses[i]

		if !Contains(a.excludedTopicFields, "Name") {
			if actualTopic.Name != expectedTopic.Name {
				return fmt.Errorf("Expected %s to be %s, got %s", fmt.Sprintf("TopicResponse[%d] Name", i), expectedTopic.Name, actualTopic.Name)
			}
			protocol.SuccessLogWithIndentation(logger, 1, "✓ TopicResponse[%d] Name: %s", i, actualTopic.Name)
		}

		expectedPartitions := expectedTopic.PartitionResponses
		actualPartitions := actualTopic.PartitionResponses

		if !Contains(a.excludedTopicFields, "partitions") {
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

			protocol.SuccessLogWithIndentation(logger, 2, "✓ PartitionResponse[%d] Error code: %d (%s)", j, actualPartition.ErrorCode, errorCodeName)
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
	if err := a.assertBody(logger); err != nil {
		return err
	}

	return nil
}

// sortResponses sorts topics by name and partitions within each topic by index
func sortResponses(response kafkaapi.ProduceResponseBody) kafkaapi.ProduceResponseBody {
	sortedResponse := response
	sortedResponse.Responses = make([]kafkaapi.ProduceTopicResponse, len(response.Responses))
	copy(sortedResponse.Responses, response.Responses)

	// Sort topics by name
	sort.Slice(sortedResponse.Responses, func(i, j int) bool {
		return sortedResponse.Responses[i].Name < sortedResponse.Responses[j].Name
	})

	// Sort partitions within each topic by index
	for i := range sortedResponse.Responses {
		partitions := make([]kafkaapi.ProducePartitionResponse, len(sortedResponse.Responses[i].PartitionResponses))
		copy(partitions, sortedResponse.Responses[i].PartitionResponses)

		sort.Slice(partitions, func(x, y int) bool {
			return partitions[x].Index < partitions[y].Index
		})

		sortedResponse.Responses[i].PartitionResponses = partitions
	}

	return sortedResponse
}
