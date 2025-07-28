package assertions

import (
	"fmt"
	"sort"

	"github.com/codecrafters-io/kafka-tester/protocol"
	kafkaapi "github.com/codecrafters-io/kafka-tester/protocol/api"
	"github.com/codecrafters-io/tester-utils/logger"
)

type ProduceResponseAssertion struct {
	ActualValue   kafkaapi.ProduceResponse
	ExpectedValue kafkaapi.ProduceResponse
}

func NewProduceResponseAssertion(actualValue kafkaapi.ProduceResponse, expectedValue kafkaapi.ProduceResponse) *ProduceResponseAssertion {
	// Sort both responses for consistent comparison
	actualValue.Body = sortResponseBodies(actualValue.Body)
	expectedValue.Body = sortResponseBodies(expectedValue.Body)

	return &ProduceResponseAssertion{
		ActualValue:   actualValue,
		ExpectedValue: expectedValue,
	}
}

func (a *ProduceResponseAssertion) assertBody(logger *logger.Logger) error {
	if a.ActualValue.Body.ThrottleTimeMs != a.ExpectedValue.Body.ThrottleTimeMs {
		return fmt.Errorf("Expected throttle_time_ms to be %d, got %d", a.ExpectedValue.Body.ThrottleTimeMs, a.ActualValue.Body.ThrottleTimeMs)
	}
	protocol.SuccessLogWithIndentation(logger, 0, "✓ throttle_time_ms: %d", a.ActualValue.Body.ThrottleTimeMs)

	if err := a.assertTopics(logger); err != nil {
		return err
	}

	return nil
}

func (a *ProduceResponseAssertion) assertTopics(logger *logger.Logger) error {
	if len(a.ActualValue.Body.TopicResponses) != len(a.ExpectedValue.Body.TopicResponses) {
		return fmt.Errorf("Expected responses.length to be %d, got %d", len(a.ExpectedValue.Body.TopicResponses), len(a.ActualValue.Body.TopicResponses))
	}

	for i, actualTopic := range a.ActualValue.Body.TopicResponses {
		expectedTopic := a.ExpectedValue.Body.TopicResponses[i]

		if actualTopic.Name != expectedTopic.Name {
			return fmt.Errorf("Expected responses[%d].name to be %s, got %s", i, expectedTopic.Name, actualTopic.Name)
		}
		protocol.SuccessLogWithIndentation(logger, 1, "✓ responses[%d].name: %s", i, actualTopic.Name)

		expectedPartitions := expectedTopic.PartitionResponses
		actualPartitions := actualTopic.PartitionResponses

		if err := a.assertPartitions(expectedPartitions, actualPartitions, logger); err != nil {
			return err
		}
	}

	return nil
}

func (a *ProduceResponseAssertion) assertPartitions(expectedPartitions []kafkaapi.ProducePartitionResponse, actualPartitions []kafkaapi.ProducePartitionResponse, logger *logger.Logger) error {
	if len(actualPartitions) != len(expectedPartitions) {
		return fmt.Errorf("Expected partition_responses.length to be %d, got %d", len(expectedPartitions), len(actualPartitions))
	}

	for j, actualPartition := range actualPartitions {
		expectedPartition := expectedPartitions[j]

		if actualPartition.ErrorCode != expectedPartition.ErrorCode {
			return fmt.Errorf("Expected partition_responses[%d].error_code to be %d, got %d", j, expectedPartition.ErrorCode, actualPartition.ErrorCode)
		}

		errorCodeName, ok := errorCodes[int(actualPartition.ErrorCode)]
		if !ok {
			panic(fmt.Sprintf("CodeCrafters Internal Error: Expected %d to be in errorCodes map", actualPartition.ErrorCode))
		}

		protocol.SuccessLogWithIndentation(logger, 2, "✓ partition_responses[%d].error_code: %d (%s)", j, actualPartition.ErrorCode, errorCodeName)

		if actualPartition.Index != expectedPartition.Index {
			return fmt.Errorf("Expected partition_responses[%d].index to be %d, got %d", j, expectedPartition.Index, actualPartition.Index)
		}
		protocol.SuccessLogWithIndentation(logger, 2, "✓ partition_responses[%d].index: %d", j, actualPartition.Index)

		if actualPartition.BaseOffset != expectedPartition.BaseOffset {
			return fmt.Errorf("Expected partition_responses[%d].base_offset to be %d, got %d", j, expectedPartition.BaseOffset, actualPartition.BaseOffset)
		}
		protocol.SuccessLogWithIndentation(logger, 2, "✓ partition_responses[%d].base_offset: %d", j, actualPartition.BaseOffset)

		if actualPartition.LogStartOffset != expectedPartition.LogStartOffset {
			return fmt.Errorf("Expected partition_responses[%d].log_start_offset to be %d, got %d", j, expectedPartition.LogStartOffset, actualPartition.LogStartOffset)
		}
		protocol.SuccessLogWithIndentation(logger, 2, "✓ partition_responses[%d].log_start_offset: %d", j, actualPartition.LogStartOffset)

		if actualPartition.LogAppendTimeMs != expectedPartition.LogAppendTimeMs {
			return fmt.Errorf("Expected partition_responses[%d].log_append_time_ms to be %d, got %d", j, expectedPartition.LogAppendTimeMs, actualPartition.LogAppendTimeMs)
		}
		protocol.SuccessLogWithIndentation(logger, 2, "✓ partition_responses[%d].log_append_time_ms: %d", j, actualPartition.LogAppendTimeMs)
	}

	return nil
}

func (a *ProduceResponseAssertion) Run(logger *logger.Logger) error {
	if err := NewResponseHeaderAssertion(a.ActualValue.Header, a.ExpectedValue.Header).Run(logger); err != nil {
		return err
	}

	if err := a.assertBody(logger); err != nil {
		return err
	}

	return nil
}

// sortResponseBodies sorts topics by name and partitions within each topic by index
// This is required because the order of the topics and partitions in the response
// is not guaranteed to be the same as the order in the expectedResponse.
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
