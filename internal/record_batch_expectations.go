package internal

import (
	"fmt"

	"github.com/codecrafters-io/kafka-tester/internal/assertions"
	"github.com/codecrafters-io/kafka-tester/internal/logparser"
	kafkaapi "github.com/codecrafters-io/kafka-tester/protocol/api"
	"github.com/codecrafters-io/kafka-tester/protocol/builder"
	"github.com/codecrafters-io/kafka-tester/protocol/common"
	"github.com/codecrafters-io/kafka-tester/protocol/serializer"
	"github.com/codecrafters-io/tester-utils/logger"
)

// buildExpectedRecordBatch builds the expected RecordBatch for a given topic and partition
// based on the messages that were sent in the ProduceRequest
func buildExpectedRecordBatch(messages []string) kafkaapi.RecordBatch {
	return builder.NewRecordBatchBuilder().
		WithBaseOffset(0). // New messages start at offset 0
		WithPartitionLeaderEpoch(0).
		AddRecords(messages).
		Build()
}

// buildExpectedRecordBatchForStageP5 builds the expected RecordBatch for stage p5
// Stage p5 sends 3 messages to a single partition
func buildExpectedRecordBatchForStageP4() kafkaapi.RecordBatch {
	messages := []string{common.HELLO_MSG1}
	return buildExpectedRecordBatch(messages)
}

// buildExpectedRecordBatchForStageP5 builds the expected RecordBatch for stage p5
// Stage p5 sends 3 messages to a single partition
func buildExpectedRecordBatchForStageP5() kafkaapi.RecordBatch {
	messages := []string{common.HELLO_MSG1, common.HELLO_MSG2, common.HELLO_MSG3}
	return buildExpectedRecordBatch(messages)
}

// buildExpectedRecordBatchForStageP6 builds the expected RecordBatch for stage p6
// Stage p6 sends 1 message each to 2 different partitions
func buildExpectedRecordBatchForStageP6(message string) kafkaapi.RecordBatch {
	messages := []string{message}
	return buildExpectedRecordBatch(messages)
}

// buildExpectedRecordBatchForStageP7 builds the expected RecordBatch for stage p7
// Stage p7 sends 1 message each to 2 different topics
func buildExpectedRecordBatchForStageP7(message string) kafkaapi.RecordBatch {
	messages := []string{message}
	return buildExpectedRecordBatch(messages)
}

// getLogFilePathForTopic builds the log file path for a given topic and partition
func getLogFilePathForTopic(topicName string, partition int32) string {
	return fmt.Sprintf("%s/%s-%d/00000000000000000000.log", common.LOG_DIR, topicName, partition)
}

// validateRecordBatchInLogFile validates that the expected RecordBatch matches what's in the log file
func validateRecordBatchInLogFile(topicName string, partition int32, expectedBatch kafkaapi.RecordBatch, stageLogger *logger.Logger) error {
	logFilePath := getLogFilePathForTopic(topicName, partition)

	// Parse the log file to get actual RecordBatch data
	logParser := logparser.NewLogFileParser(stageLogger)
	result, err := logParser.ParseLogFile(logFilePath)
	if err != nil {
		return fmt.Errorf("failed to parse log file %s: %w", logFilePath, err)
	}

	fmt.Println("result", result)
	fmt.Println("Found", len(result.RecordBatches), "RecordBatches in", logFilePath)

	// Validate we have exactly one RecordBatch
	if len(result.RecordBatches) != 1 {
		return fmt.Errorf("expected 1 RecordBatch in %s, got %d", logFilePath, len(result.RecordBatches))
	}

	actualBatch := result.RecordBatches[0]

	// Create assertion and validate
	assertion := assertions.NewRecordBatchAssertion(actualBatch, expectedBatch, stageLogger)

	// Exclude fields that are computed/set by the system
	return assertion.
		ExcludeBatchFields("BatchLength", "Magic", "CRC", "FirstTimestamp", "MaxTimestamp").
		ExcludeRecordFields("Length", "TimestampDelta", "OffsetDelta").
		AssertBatchAndRecords().
		Run()
}

// encodeRecordBatchesInLogFile encodes the RecordBatches in the log file
func encodeRecordBatchesInLogFile(topicName string, partition int32, stageLogger *logger.Logger) ([][]byte, error) {
	logFilePath := getLogFilePathForTopic(topicName, partition)

	// Parse the log file to get actual RecordBatch data
	quietLogger := logger.GetQuietLogger("logparser")
	logParser := logparser.NewLogFileParser(quietLogger)
	result, err := logParser.ParseLogFile(logFilePath)
	if err != nil {
		return nil, fmt.Errorf("failed to parse log file %s: %w", logFilePath, err)
	}

	encodedBatches := make([][]byte, 0)

	for _, batch := range result.RecordBatches {
		as := serializer.GetAnyEncodedBytes(batch)
		encodedBatches = append(encodedBatches, as)
	}

	stageLogger.Infof("Found %d RecordBatches in %s", len(result.RecordBatches), logFilePath)
	return encodedBatches, nil
}

func encodeRecordBatches(recordBatches []kafkaapi.RecordBatch) [][]byte {
	encodedBatches := make([][]byte, 0)
	for i, recordBatch := range recordBatches {
		recordBatch.BaseOffset = int64(i)
		as := serializer.GetAnyEncodedBytes(recordBatch)
		encodedBatches = append(encodedBatches, as)
	}
	return encodedBatches
}
