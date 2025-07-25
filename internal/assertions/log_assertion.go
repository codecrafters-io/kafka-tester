package assertions

import (
	"bytes"
	"fmt"
	"strings"

	"github.com/codecrafters-io/kafka-tester/internal/logparser"
	kafkaapi "github.com/codecrafters-io/kafka-tester/protocol/api"
	"github.com/codecrafters-io/kafka-tester/protocol/common"
	"github.com/codecrafters-io/kafka-tester/protocol/decoder"
	"github.com/codecrafters-io/kafka-tester/protocol/serializer"
	"github.com/codecrafters-io/tester-utils/logger"
)

type TopicPartitionLogAssertion struct {
	topic         string
	partition     int32
	RecordBatches []kafkaapi.RecordBatch
	logger        *logger.Logger
}

func NewTopicPartitionLogAssertion(topic string, partition int32, recordBatches []kafkaapi.RecordBatch, logger *logger.Logger) TopicPartitionLogAssertion {
	return TopicPartitionLogAssertion{
		topic:         topic,
		partition:     partition,
		RecordBatches: recordBatches,
		logger:        logger,
	}
}

func (a TopicPartitionLogAssertion) Run() error {
	logFilePath := getLogFilePathForTopic(a.topic, a.partition)
	logParser := logparser.NewLogFileParser(logFilePath)

	err := logParser.ReadLogFile(a.logger)
	if err != nil {
		return fmt.Errorf("Failed to read log file %s: %w", logFilePath, err)
	}

	expectedEncodedRecordBatches := encodeRecordBatches(a.RecordBatches)
	if len(expectedEncodedRecordBatches) == 0 {
		panic("Codecrafters Internal Error: RecordBatches are empty")
	}

	if bytes.Equal(expectedEncodedRecordBatches, logParser.RawBytes) {
		a.logger.Successf("✓ RecordBatches in log file (%s) match expected RecordBatches", logFilePath)
		return nil
	}

	mismatchOffset := findMisMatchOffset(expectedEncodedRecordBatches, logParser.RawBytes)
	finalErr := fmt.Errorf("RecordBatches in log file (%s) do not match expected RecordBatches", logFilePath)

	a.logger.UpdateLastSecondaryPrefix("logparser")
	defer a.logger.ResetSecondaryPrefixes()

	a.logger.Infof("Parsing log file (%s) belonging to topic (%s) and partition (%d)", logFilePath, a.topic, a.partition)
	err = logParser.ParseLogFile(a.logger)
	if err != nil {
		return fmt.Errorf("Failed to parse log file (%s): %w", logFilePath, err)
	}

	if len(logParser.RecordBatches) != len(a.RecordBatches) {
		return fmt.Errorf("Expected %d RecordBatches in log file (%s), got %d", len(a.RecordBatches), logFilePath, len(logParser.RecordBatches))
	}

	for i, actualRecordBatch := range logParser.RecordBatches {
		expectedRecordBatch := a.RecordBatches[i]
		if !bytes.Equal(serializer.GetEncodedBytes(actualRecordBatch), serializer.GetEncodedBytes(expectedRecordBatch)) {
			return fmt.Errorf("RecordBatches in log file (%s) do not match expected RecordBatches", logFilePath)
		}
	}

	return formattedError("mismatch here\n"+finalErr.Error(), mismatchOffset, logParser.RawBytes)
}

func getLogFilePathForTopic(topicName string, partition int32) string {
	return fmt.Sprintf("%s/%s-%d/00000000000000000000.log", common.LOG_DIR, topicName, partition)
}

func formattedError(message string, offset int, receivedBytes []byte) error {
	lines := []string{}

	receivedByteString := decoder.NewInspectableHexDump(receivedBytes)

	lines = append(lines, "Received:")
	lines = append(lines, receivedByteString.FormatWithHighlightedOffset(offset))
	lines = append(lines, message)

	return fmt.Errorf("%s", strings.Join(lines, "\n"))
}

func findMisMatchOffset(expectedBytes []byte, actualBytes []byte) int {
	for i := range expectedBytes {
		if i >= len(actualBytes) {
			return i
		}

		if expectedBytes[i] != actualBytes[i] {
			return i
		}
	}
	return -1
}
