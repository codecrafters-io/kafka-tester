package assertions

import (
	"bytes"
	"fmt"

	"github.com/codecrafters-io/kafka-tester/protocol"
	"github.com/codecrafters-io/kafka-tester/protocol/kafkaapi_legacy"
	"github.com/codecrafters-io/kafka-tester/protocol/serializer"
	"github.com/codecrafters-io/tester-utils/bytes_diff_visualizer"
	"github.com/codecrafters-io/tester-utils/logger"
)

type FetchResponseAssertion struct {
	ActualValue             kafkaapi_legacy.FetchResponse
	ExpectedValue           kafkaapi_legacy.FetchResponse
	excludedPartitionFields []string
}

func NewFetchResponseAssertion(actualValue kafkaapi_legacy.FetchResponse, expectedValue kafkaapi_legacy.FetchResponse, logger *logger.Logger) *FetchResponseAssertion {
	return &FetchResponseAssertion{
		ActualValue:             actualValue,
		ExpectedValue:           expectedValue,
		excludedPartitionFields: []string{},
	}
}

func (a *FetchResponseAssertion) SkipRecordBatches() *FetchResponseAssertion {
	a.excludedPartitionFields = append(a.excludedPartitionFields, "RecordBatches")
	return a
}

func (a *FetchResponseAssertion) assertThrottleTimeMs(logger *logger.Logger) error {
	if a.ActualValue.ThrottleTimeMs != a.ExpectedValue.ThrottleTimeMs {
		return fmt.Errorf("Expected ThrottleTimeMs to be %d, got %d", a.ExpectedValue.ThrottleTimeMs, a.ActualValue.ThrottleTimeMs)
	}
	logger.Successf("✓ ThrottleTimeMs: %d", a.ActualValue.ThrottleTimeMs)

	return nil
}

func (a *FetchResponseAssertion) assertErrorCode(logger *logger.Logger) error {
	expectedErrorCodeName, ok := errorCodes[int(a.ExpectedValue.ErrorCode)]
	if !ok {
		panic(fmt.Sprintf("CodeCrafters Internal Error: Expected %d to be in errorCodes map", a.ExpectedValue.ErrorCode))
	}

	if a.ActualValue.ErrorCode != a.ExpectedValue.ErrorCode {
		return fmt.Errorf("Expected ErrorCode to be %d (%s), got %d", a.ExpectedValue.ErrorCode, expectedErrorCodeName, a.ActualValue.ErrorCode)
	}

	logger.Successf("✓ ErrorCode: %d (%s)", a.ActualValue.ErrorCode, expectedErrorCodeName)

	return nil
}

func (a *FetchResponseAssertion) assertTopics(logger *logger.Logger) error {
	if len(a.ActualValue.TopicResponses) != len(a.ExpectedValue.TopicResponses) {
		return fmt.Errorf("Expected topics.length to be %d, got %d", len(a.ExpectedValue.TopicResponses), len(a.ActualValue.TopicResponses))
	}
	protocol.SuccessLogWithIndentation(logger, 0, "✓ TopicResponses Length: %v", len(a.ActualValue.TopicResponses))

	for i, actualTopic := range a.ActualValue.TopicResponses {
		expectedTopic := a.ExpectedValue.TopicResponses[i]
		if actualTopic.Topic != expectedTopic.Topic {
			return fmt.Errorf("Expected TopicResponse[%d] TopicID to be %s, got %s", i, expectedTopic.Topic, actualTopic.Topic)
		}
		protocol.SuccessLogWithIndentation(logger, 1, "✓ TopicResponse[%d] TopicID: %s", i, actualTopic.Topic)

		expectedPartitions := expectedTopic.PartitionResponses
		actualPartitions := actualTopic.PartitionResponses

		if err := a.assertPartitions(expectedPartitions, actualPartitions, logger); err != nil {
			return err
		}
	}

	return nil
}

func (a *FetchResponseAssertion) assertPartitions(expectedPartitions []kafkaapi_legacy.PartitionResponse, actualPartitions []kafkaapi_legacy.PartitionResponse, logger *logger.Logger) error {
	if len(actualPartitions) != len(expectedPartitions) {
		return fmt.Errorf("Expected partitions.length to be %d, got %d", len(expectedPartitions), len(actualPartitions))
	}
	protocol.SuccessLogWithIndentation(logger, 1, "✓ PartitionResponses Length: %v", len(actualPartitions))

	for j, actualPartition := range actualPartitions {
		expectedPartition := expectedPartitions[j]

		expectedErrorCodeName, ok := errorCodes[int(expectedPartition.ErrorCode)]
		if !ok {
			panic(fmt.Sprintf("CodeCrafters Internal Error: Expected %d to be in errorCodes map", expectedPartition.ErrorCode))
		}
		if actualPartition.ErrorCode != expectedPartition.ErrorCode {
			return fmt.Errorf("Expected PartitionResponse[%d] Error Code to be %d (%s), got %d", j, expectedPartition.ErrorCode, expectedErrorCodeName, actualPartition.ErrorCode)
		}

		protocol.SuccessLogWithIndentation(logger, 2, "✓ PartitionResponse[%d] ErrorCode: %d (%s)", j, actualPartition.ErrorCode, expectedErrorCodeName)

		if actualPartition.PartitionIndex != expectedPartition.PartitionIndex {
			return fmt.Errorf("Expected PartitionResponse[%d] PartitionIndex to be %d, got %d", j, expectedPartition.PartitionIndex, actualPartition.PartitionIndex)
		}
		protocol.SuccessLogWithIndentation(logger, 2, "✓ PartitionResponse[%d] PartitionIndex: %d", j, actualPartition.PartitionIndex)

		expectedRecordBatches := expectedPartition.RecordBatches
		actualRecordBatches := actualPartition.RecordBatches

		// If RecordBatches are not excluded from assertion,
		// They will be compared with their on-disk counterparts too
		if !Contains(a.excludedPartitionFields, "RecordBatches") {
			if err := a.assertRecordBatches(expectedRecordBatches, actualRecordBatches, logger); err != nil {
				return err
			}

			if err := a.assertRecordBatchBytes(logger); err != nil {
				return err
			}
		}
	}

	return nil
}

func (a *FetchResponseAssertion) assertRecordBatches(expectedRecordBatches []kafkaapi_legacy.RecordBatch, actualRecordBatches []kafkaapi_legacy.RecordBatch, logger *logger.Logger) error {
	if len(actualRecordBatches) != len(expectedRecordBatches) {
		return fmt.Errorf("Expected recordBatches.length to be %d, got %d", len(expectedRecordBatches), len(actualRecordBatches))
	}
	protocol.SuccessLogWithIndentation(logger, 2, "✓ RecordBatches Length: %v", len(actualRecordBatches))

	for k, actualRecordBatch := range actualRecordBatches {
		expectedRecordBatch := expectedRecordBatches[k]

		if actualRecordBatch.BaseOffset != expectedRecordBatch.BaseOffset {
			return fmt.Errorf("Expected RecordBatch[%d] BaseOffset to be %d, got %d", k, expectedRecordBatch.BaseOffset, actualRecordBatch.BaseOffset)
		}
		protocol.SuccessLogWithIndentation(logger, 3, "✓ RecordBatch[%d] BaseOffset: %d", k, actualRecordBatch.BaseOffset)

		// TODO: BatchLength can't be hardcoded in the expected response,
		// Once we have the FetchResponseBuilder, assert for BatchLength too
		// if actualRecordBatch.BatchLength != expectedRecordBatch.BatchLength {
		// 	return fmt.Errorf("Expected RecordBatch[%d] BatchLength to be %d, got %d", k, expectedRecordBatch.BatchLength, actualRecordBatch.BatchLength)
		// }
		// protocol.SuccessLogWithIndentation(logger, 3, "✓ RecordBatch[%d] BatchLength: %d", k, actualRecordBatch.BatchLength)

		if err := a.assertRecords(expectedRecordBatch.Records, actualRecordBatch.Records, logger); err != nil {
			return err
		}
	}

	return nil
}

func (a *FetchResponseAssertion) assertRecords(expectedRecords []kafkaapi_legacy.Record, actualRecords []kafkaapi_legacy.Record, logger *logger.Logger) error {
	if len(actualRecords) != len(expectedRecords) {
		return fmt.Errorf("Expected records.length to be %d, got %d", len(expectedRecords), len(actualRecords))
	}
	protocol.SuccessLogWithIndentation(logger, 3, "✓ Records Length: %v", len(actualRecords))

	for l, actualRecord := range actualRecords {
		expectedRecord := expectedRecords[l]

		if !bytes.Equal(actualRecord.Value, expectedRecord.Value) {
			return fmt.Errorf("Expected Record[%d] Value to be %v, got %v", l, expectedRecord.Value, actualRecord.Value)
		}
		protocol.SuccessLogWithIndentation(logger, 4, "✓ Record[%d] Value: %s", l, actualRecord.Value)
	}

	return nil
}

func (a *FetchResponseAssertion) assertRecordBatchBytes(logger *logger.Logger) error {
	actualRecordBatches := kafkaapi_legacy.RecordBatches{}
	for _, topic := range a.ActualValue.TopicResponses {
		for _, partition := range topic.PartitionResponses {
			actualRecordBatches = append(actualRecordBatches, partition.RecordBatches...)
		}
	}

	expectedRecordBatches := kafkaapi_legacy.RecordBatches{}
	for _, topic := range a.ExpectedValue.TopicResponses {
		for _, partition := range topic.PartitionResponses {
			expectedRecordBatches = append(expectedRecordBatches, partition.RecordBatches...)
		}
	}

	expectedRecordBatchBytes := serializer.GetEncodedBytes(expectedRecordBatches)
	actualRecordBatchBytes := serializer.GetEncodedBytes(actualRecordBatches)
	// Byte Comparison for expected v actual RecordBatch bytes
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

func (a *FetchResponseAssertion) Run(logger *logger.Logger) error {
	if err := a.assertThrottleTimeMs(logger); err != nil {
		return err
	}

	if err := a.assertErrorCode(logger); err != nil {
		return err
	}

	if err := a.assertTopics(logger); err != nil {
		return err
	}

	return nil
}
