package response_assertions

import (
	"bytes"
	"fmt"

	"github.com/codecrafters-io/kafka-tester/internal/field_decoder"
	int16_assertions "github.com/codecrafters-io/kafka-tester/internal/value_assertions/int16"
	int32_assertions "github.com/codecrafters-io/kafka-tester/internal/value_assertions/int32"
	"github.com/codecrafters-io/kafka-tester/protocol/encoder"
	"github.com/codecrafters-io/kafka-tester/protocol/kafkaapi"
	"github.com/codecrafters-io/kafka-tester/protocol/utils"
	"github.com/codecrafters-io/tester-utils/bytes_diff_visualizer"
	"github.com/codecrafters-io/tester-utils/logger"
)

type FetchResponseAssertion struct {
	expectedCorrelationID        int32
	expectedThrottleTimeMs       int32
	expectedErrorCodeInBody      int16
	expectedSessionId            *int32
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

func (a *FetchResponseAssertion) ExpectSessionId(expectedSessionId int32) *FetchResponseAssertion {
	a.expectedSessionId = &expectedSessionId
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

func (a *FetchResponseAssertion) AssertSingleField(field field_decoder.DecodedField) error {
	if field.Path.String() == "FetchResponse.Header.CorrelationID" {
		return int32_assertions.IsEqualTo(a.expectedCorrelationID, field.Value)
	}

	if field.Path.String() == "FetchResponse.Body.SessionID" {
		if a.expectedSessionId != nil {
			return int32_assertions.IsEqualTo(*a.expectedSessionId, field.Value)
		}
	}

	if field.Path.String() == "FetchResponse.Body.ThrottleTimeMS" {
		return int32_assertions.IsEqualTo(a.expectedThrottleTimeMs, field.Value)
	}

	if field.Path.String() == "FetchResponse.Body.ErrorCode" {
		return int16_assertions.IsEqualTo(a.expectedErrorCodeInBody, field.Value)
	}

	return nil
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
			if err := a.assertRecordBatches(actualPartition.RecordBatches, logger); err != nil {
				return err
			}

			// Perform byte-level comparison for more robust validation
			if err := a.assertRecordBatchBytes(actualPartition.RecordBatches, logger); err != nil {
				return err
			}
		}
	}

	return nil
}

func (a *FetchResponseAssertion) assertRecordBatches(actualRecordBatches []kafkaapi.RecordBatch, logger *logger.Logger) error {
	expectedRecordBatchCount := len(a.expectedRecordBatches)
	actualRecordBatchCount := len(actualRecordBatches)

	if actualRecordBatchCount != expectedRecordBatchCount {
		return fmt.Errorf("Expected recordBatches.length to be %d, got %d", expectedRecordBatchCount, actualRecordBatchCount)
	}
	logger.Successf("✓ RecordBatches Length: %v", actualRecordBatchCount)

	for i, actualRecordBatch := range actualRecordBatches {
		expectedRecordBatch := a.expectedRecordBatches[i]

		// Check base offset
		actualBaseOffset := actualRecordBatch.BaseOffset.Value
		expectedBaseOffset := expectedRecordBatch.BaseOffset.Value
		if actualBaseOffset != expectedBaseOffset {
			return fmt.Errorf("Expected RecordBatch[%d] BaseOffset to be %d, got %d", i, expectedBaseOffset, actualBaseOffset)
		}
		logger.Successf("✓ RecordBatch[%d] BaseOffset: %d", i, actualBaseOffset)

		if err := a.assertRecords(expectedRecordBatch.Records, actualRecordBatch.Records, i, logger); err != nil {
			return err
		}
	}

	return nil
}

func (a *FetchResponseAssertion) assertRecords(expectedRecords []kafkaapi.Record, actualRecords []kafkaapi.Record, batchIndex int, logger *logger.Logger) error {
	expectedRecordCount := len(expectedRecords)
	actualRecordCount := len(actualRecords)

	if actualRecordCount != expectedRecordCount {
		return fmt.Errorf("Expected records.length to be %d, got %d", expectedRecordCount, actualRecordCount)
	}
	logger.Successf("✓ Records Length: %v", actualRecordCount)

	for i, actualRecord := range actualRecords {
		expectedRecord := expectedRecords[i]

		// Check record value
		if !bytes.Equal(actualRecord.Value, expectedRecord.Value) {
			return fmt.Errorf("Expected Record[%d] Value to be %v, got %v", i, expectedRecord.Value, actualRecord.Value)
		}
		logger.Successf("✓ Record[%d] Value: %s", i, actualRecord.Value)
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
