package response_assertions

import (
	"bytes"
	"fmt"
	"regexp"

	"github.com/codecrafters-io/kafka-tester/internal/field_decoder"
	compact_array_length_assertions "github.com/codecrafters-io/kafka-tester/internal/value_assertions/compact_array_length"
	int16_assertions "github.com/codecrafters-io/kafka-tester/internal/value_assertions/int16"
	int32_assertions "github.com/codecrafters-io/kafka-tester/internal/value_assertions/int32"
	"github.com/codecrafters-io/kafka-tester/protocol/encoder"
	"github.com/codecrafters-io/kafka-tester/protocol/kafkaapi"
	"github.com/codecrafters-io/kafka-tester/protocol/utils"
	"github.com/codecrafters-io/kafka-tester/protocol/value"
	"github.com/codecrafters-io/tester-utils/bytes_diff_visualizer"
	"github.com/codecrafters-io/tester-utils/logger"
)

type FetchResponseAssertion struct {
	expectedCorrelationID        int32
	expectedThrottleTimeMs       int32
	expectedErrorCodeInBody      int16
	expectedTopicUUID            *string
	expectedPartitionId          *int32
	expectedErrorCodeInPartition *int16
	expectedRecordBatches        kafkaapi.RecordBatches
}

func NewFetchResponseAssertion() *FetchResponseAssertion {
	return &FetchResponseAssertion{}
}

func (a *FetchResponseAssertion) ExpectCorrelationId(expectedCorrelationID int32) *FetchResponseAssertion {
	a.expectedCorrelationID = expectedCorrelationID
	return a
}

func (a *FetchResponseAssertion) ExpectThrottleTimeMs(expectedThrottleTimeMs int32) *FetchResponseAssertion {
	a.expectedThrottleTimeMs = expectedThrottleTimeMs
	return a
}

func (a *FetchResponseAssertion) ExpectErrorCodeInBody(expectedErorrCode int16) *FetchResponseAssertion {
	a.expectedErrorCodeInBody = expectedErorrCode
	return a
}

func (a *FetchResponseAssertion) ExpectErrorCodeInPartition(expectedErorrCode int16) *FetchResponseAssertion {
	a.expectedErrorCodeInPartition = &expectedErorrCode
	return a
}

func (a *FetchResponseAssertion) ExpectTopicUUID(expectedTopicUUID string) *FetchResponseAssertion {
	a.expectedTopicUUID = &expectedTopicUUID
	return a
}

func (a *FetchResponseAssertion) ExpectPartitionID(expectedPartitionId int32) *FetchResponseAssertion {
	a.expectedPartitionId = &expectedPartitionId
	return a
}

func (a *FetchResponseAssertion) ExpectRecordBatches(recordBatches kafkaapi.RecordBatches) *FetchResponseAssertion {
	a.expectedRecordBatches = recordBatches
	return a
}

func (a *FetchResponseAssertion) AssertAcrossFields(response kafkaapi.FetchResponse, logger *logger.Logger) error {
	if err := a.assertTopicResponses(response, logger); err != nil {
		return err
	}
	return nil
}

func (a *FetchResponseAssertion) assertTopicResponses(response kafkaapi.FetchResponse, logger *logger.Logger) error {
	expectedTopicCount := 1
	if a.expectedTopicUUID == nil {
		expectedTopicCount = 0
	}

	actualTopicCount := len(response.Body.TopicResponses)
	if actualTopicCount != expectedTopicCount {
		return fmt.Errorf("Expected topics.length to be %d, got %d", expectedTopicCount, actualTopicCount)
	}
	logger.Successf("✓ TopicResponses Length: %v", actualTopicCount)

	if expectedTopicCount > 0 {
		actualTopic := response.Body.TopicResponses[0]

		// Check topic UUID
		actualTopicUUID := actualTopic.UUID.Value
		if actualTopicUUID != *a.expectedTopicUUID {
			return fmt.Errorf("Expected TopicResponse[0] TopicUUID to be %s, got %s", *a.expectedTopicUUID, actualTopicUUID)
		}
		logger.Successf("✓ TopicResponse[0] TopicUUID: %s", actualTopicUUID)

		if err := a.assertPartitionResponses(actualTopic.PartitionResponses, logger); err != nil {
			return err
		}
	}

	return nil
}

func (a *FetchResponseAssertion) assertPartitionResponses(partitionResponses []kafkaapi.PartitionResponse, logger *logger.Logger) error {
	expectedPartitionCount := 1 // Based on pattern, we expect one partition when partition ID is set
	if a.expectedPartitionId == nil {
		expectedPartitionCount = 0
	}

	actualPartitionCount := len(partitionResponses)
	if actualPartitionCount != expectedPartitionCount {
		return fmt.Errorf("Expected partitions.length to be %d, got %d", expectedPartitionCount, actualPartitionCount)
	}
	logger.Successf("✓ PartitionResponses Length: %v", actualPartitionCount)

	if expectedPartitionCount > 0 {
		actualPartition := partitionResponses[0]

		// Check partition error code
		actualErrorCode := actualPartition.ErrorCode.Value
		expectedErrorCodeName := utils.ErrorCodeToName(*a.expectedErrorCodeInPartition)
		if actualErrorCode != *a.expectedErrorCodeInPartition {
			return fmt.Errorf("Expected PartitionResponse[0] Error Code to be %d (%s), got %d", *a.expectedErrorCodeInPartition, expectedErrorCodeName, actualErrorCode)
		}
		logger.Successf("✓ PartitionResponse[0] ErrorCode: %d (%s)", actualErrorCode, expectedErrorCodeName)

		// Check partition ID
		actualPartitionId := actualPartition.Id.Value
		if actualPartitionId != *a.expectedPartitionId {
			return fmt.Errorf("Expected PartitionResponse[0] PartitionId to be %d, got %d", *a.expectedPartitionId, actualPartitionId)
		}
		logger.Successf("✓ PartitionResponse[0] PartitionId: %d", actualPartitionId)

		// Assert record batches if they are set
		if a.expectedRecordBatches != nil {
			// Perform byte-level comparison as stated in the instructions
			if err := a.assertRecordBatchBytes(actualPartition.RecordBatches, logger); err != nil {
				return err
			}
		}
	}

	return nil
}

func (a *FetchResponseAssertion) assertRecordBatchBytes(actualRecordBatches []kafkaapi.RecordBatch, logger *logger.Logger) error {
	// Encode expected record batches to bytes
	expectedEncoder := encoder.NewEncoder()
	a.expectedRecordBatches.Encode(expectedEncoder)
	expectedRecordBatchBytes := expectedEncoder.Bytes()

	// Encode actual record batches to bytes
	actualEncoder := encoder.NewEncoder()
	actualRecordBatchesAsType := kafkaapi.RecordBatches(actualRecordBatches)
	actualRecordBatchesAsType.Encode(actualEncoder)
	actualRecordBatchBytes := actualEncoder.Bytes()

	// Byte comparison for expected vs actual RecordBatch bytes
	// As we write them to disk, and expect users to not change the values
	// we can use a simple byte comparison here.
	if !bytes.Equal(expectedRecordBatchBytes, actualRecordBatchBytes) {
		result := bytes_diff_visualizer.VisualizeByteDiff(expectedRecordBatchBytes, actualRecordBatchBytes)
		logger.Errorf("")
		for _, line := range result {
			logger.Errorf("%s", line)
		}
		logger.Errorf("")
		return fmt.Errorf("RecordBatch bytes do not match with the contents on disk")
	}

	logger.Successf("✓ RecordBatch bytes match with the contents on disk")
	return nil
}

func (a *FetchResponseAssertion) AssertSingleField(field field_decoder.DecodedField) error {
	fieldPath := field.Path.String()

	// Header fields
	if fieldPath == "FetchResponse.Header.CorrelationID" {
		return int32_assertions.IsEqualTo(a.expectedCorrelationID, field.Value)
	}

	// Body level fields
	if fieldPath == "FetchResponse.Body.ThrottleTimeMS" {
		return int32_assertions.IsEqualTo(a.expectedThrottleTimeMs, field.Value)
	}

	if fieldPath == "FetchResponse.Body.ErrorCode" {
		return int16_assertions.IsEqualTo(a.expectedErrorCodeInBody, field.Value)
	}

	if fieldPath == "FetchResponse.Body.SessionID" {
		return nil
	}

	if fieldPath == "FetchResponse.Body.Topics.Length" {
		// Empty array
		compactArrayLength := value.CompactArrayLength{Value: 1}

		// With one element (Hardcoded because we use only one topic for the extension)
		if a.expectedTopicUUID != nil {
			compactArrayLength = value.CompactArrayLength{Value: 2}
		}

		return compact_array_length_assertions.IsEqualTo(compactArrayLength, field.Value)
	}

	// Partitions array and its elements
	if regexp.MustCompile(`\.Topics\..*$`).MatchString(fieldPath) {
		return nil
	}

	// This ensures that we're handling ALL possible fields
	panic("CodeCrafters Internal Error: Unhandled field path: " + fieldPath)
}
