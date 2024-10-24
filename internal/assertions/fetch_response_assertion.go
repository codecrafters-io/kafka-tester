package assertions

import (
	"bytes"
	"fmt"

	"github.com/codecrafters-io/kafka-tester/protocol"
	kafkaapi "github.com/codecrafters-io/kafka-tester/protocol/api"
	realencoder "github.com/codecrafters-io/kafka-tester/protocol/encoder"
	"github.com/codecrafters-io/tester-utils/bytes_diff_visualizer"
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

		errorCodeName, ok := errorCodes[int(a.ActualValue.ErrorCode)]
		if !ok {
			errorCodeName = "UNKNOWN"
		}
		a.logger.Successf("✓ Error Code: %d (%s)", a.ActualValue.ErrorCode, errorCodeName)
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

func (a *FetchResponseAssertion) AssertNoTopics() *FetchResponseAssertion {
	if a.err != nil {
		return a
	}

	if len(a.ActualValue.TopicResponses) != 0 {
		a.err = fmt.Errorf("Expected %s to be %d, got %d", "topics.length", 0, len(a.ActualValue.TopicResponses))
		return a
	}
	protocol.SuccessLogWithIndentation(a.logger, 1, "✓ TopicResponses: %v", a.ActualValue.TopicResponses)

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
			a.assertPartitions(expectedPartitions, actualPartitions, partitionFields, recordBatchFields, recordFields)
		} else {
			if len(actualPartitions) != 0 {
				a.err = fmt.Errorf("Expected %s to be %d, got %d", "partitions.length", 0, len(actualPartitions))
				return a
			}
		}
	}

	return a
}

func (a *FetchResponseAssertion) assertPartitions(expectedPartitions []kafkaapi.PartitionResponse, actualPartitions []kafkaapi.PartitionResponse, fields []string, recordBatchFields []string, recordFields []string) *FetchResponseAssertion {
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

		if Contains(fields, "PartitionIndex") {
			if actualPartition.PartitionIndex != expectedPartition.PartitionIndex {
				a.err = fmt.Errorf("Expected %s to be %d, got %d", fmt.Sprintf("Partition Response[%d] Partition Index", j), expectedPartition.PartitionIndex, actualPartition.PartitionIndex)
				return a
			}
			protocol.SuccessLogWithIndentation(a.logger, 2, "✓ PartitionResponse[%d] Partition Index: %d", j, actualPartition.PartitionIndex)
		}

		expectedRecordBatches := expectedPartition.RecordBatches
		actualRecordBatches := actualPartition.RecordBatches

		if (recordBatchFields) != nil {
			a.assertRecordBatches(expectedRecordBatches, actualRecordBatches, recordBatchFields, recordFields)
		} else {
			if len(actualRecordBatches) != 0 {
				a.err = fmt.Errorf("Expected %s to be %d, got %d", "recordBatches.length", 0, len(actualRecordBatches))
				return a
			}
			protocol.SuccessLogWithIndentation(a.logger, 2, "✓ RecordBatches: %v", actualPartition.RecordBatches)
		}
	}

	return a
}

func (a *FetchResponseAssertion) assertRecordBatches(expectedRecordBatches []kafkaapi.RecordBatch, actualRecordBatches []kafkaapi.RecordBatch, fields []string, recordFields []string) *FetchResponseAssertion {
	if len(actualRecordBatches) != len(expectedRecordBatches) {
		a.err = fmt.Errorf("Expected %s to be %d, got %d", "recordBatches.length", len(expectedRecordBatches), len(actualRecordBatches))
		return a
	}

	for k, actualRecordBatch := range actualRecordBatches {
		expectedRecordBatch := expectedRecordBatches[k]

		if Contains(fields, "BaseOffset") {
			if actualRecordBatch.BaseOffset != expectedRecordBatch.BaseOffset {
				a.err = fmt.Errorf("Expected %s to be %d, got %d", fmt.Sprintf("RecordBatch[%d] BaseOffset", k), expectedRecordBatch.BaseOffset, actualRecordBatch.BaseOffset)
				return a
			}
			protocol.SuccessLogWithIndentation(a.logger, 3, "✓ RecordBatch[%d] BaseOffset: %d", k, actualRecordBatch.BaseOffset)
		}

		if Contains(fields, "BatchLength") {
			if actualRecordBatch.BatchLength != expectedRecordBatch.BatchLength {
				a.err = fmt.Errorf("Expected %s to be %d, got %d", fmt.Sprintf("RecordBatch[%d] BatchLength", k), expectedRecordBatch.BatchLength, actualRecordBatch.BatchLength)
				return a
			}
			protocol.SuccessLogWithIndentation(a.logger, 3, "✓ RecordBatch[%d] BatchLength: %d", k, actualRecordBatch.BatchLength)
		}

		expectedRecords := expectedRecordBatch.Records
		actualRecords := actualRecordBatch.Records

		if (recordFields) != nil {
			a.assertRecords(expectedRecords, actualRecords, recordFields)
		} else {
			if len(actualRecords) != 0 {
				a.err = fmt.Errorf("Expected %s to be %d, got %d", "records.length", 0, len(actualRecords))
				return a
			}
		}
	}

	return a
}

func (a *FetchResponseAssertion) assertRecords(expectedRecords []kafkaapi.Record, actualRecords []kafkaapi.Record, fields []string) *FetchResponseAssertion {
	if len(actualRecords) != len(expectedRecords) {
		a.err = fmt.Errorf("Expected %s to be %d, got %d", "records.length", len(expectedRecords), len(actualRecords))
		return a
	}

	for l, actualRecord := range actualRecords {
		expectedRecord := expectedRecords[l]

		if Contains(fields, "Value") {
			if !bytes.Equal(actualRecord.Value, expectedRecord.Value) {
				a.err = fmt.Errorf("Expected %s to be %d, got %d", fmt.Sprintf("Record[%d] Value", l), expectedRecord.Value, actualRecord.Value)
				return a
			}
			protocol.SuccessLogWithIndentation(a.logger, 4, "✓ Record[%d] Value: %s", l, actualRecord.Value)
		}
	}

	return a
}

func (a *FetchResponseAssertion) AssertRecordBatchBytes() *FetchResponseAssertion {
	if a.err != nil {
		return a
	}

	actualRecordBatches := []kafkaapi.RecordBatch{}
	for _, topic := range a.ActualValue.TopicResponses {
		for _, partition := range topic.PartitionResponses {
			actualRecordBatches = append(actualRecordBatches, partition.RecordBatches...)
		}
	}

	expectedRecordBatches := []kafkaapi.RecordBatch{}
	for _, topic := range a.ExpectedValue.TopicResponses {
		for _, partition := range topic.PartitionResponses {
			expectedRecordBatches = append(expectedRecordBatches, partition.RecordBatches...)
		}
	}

	expectedRecordBatchBytes := encodeRecordBatches(expectedRecordBatches)
	actualRecordBatchBytes := encodeRecordBatches(actualRecordBatches)
	// Byte Comparison for expected v actual RecordBatch bytes
	// As we write them to disk, and expect users to not change the values
	// we can use a simple byte comparison here.
	if !bytes.Equal(expectedRecordBatchBytes, actualRecordBatchBytes) {
		result := bytes_diff_visualizer.VisualizeByteDiff(expectedRecordBatchBytes, actualRecordBatchBytes)
		a.logger.Errorf("")
		for _, line := range result {
			a.logger.Errorf(line)
		}
		a.logger.Errorf("")
		a.err = fmt.Errorf("RecordBatch bytes do not match with the contents on disk")
		return a
	}

	a.logger.Successf("✓ RecordBatch bytes match with the contents on disk")
	return a

}

func (a FetchResponseAssertion) Run() error {
	// firstLevelFields: ["ThrottleTimeMs", "ErrorCode", "SessionID"]
	// secondLevelFields (Topics): ["Topic"]
	// thirdLevelFields (Partitions): ["ErrorCode, "PartitionIndex"]
	// fourthLevelFields (RecordBatches): ["BaseOffset", "BatchLength"]
	// fifthLevelFields (Records): ["Value"]
	return a.err
}

func encodeRecordBatches(recordBatches []kafkaapi.RecordBatch) []byte {
	// Given an array of RecordBatch, encodes them using the encoder.RealEncoder
	// and returns the resulting bytes.

	encoder := realencoder.RealEncoder{}
	encoder.Init(make([]byte, 4096))
	for _, recordBatch := range recordBatches {
		recordBatch.Encode(&encoder)
	}
	return encoder.Bytes()[:encoder.Offset()]
}
