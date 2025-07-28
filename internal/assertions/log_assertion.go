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
	RecordBatches kafkaapi.RecordBatches
}

func NewTopicPartitionLogAssertion(topic string, partition int32, recordBatches kafkaapi.RecordBatches) TopicPartitionLogAssertion {
	return TopicPartitionLogAssertion{
		topic:         topic,
		partition:     partition,
		RecordBatches: recordBatches,
	}
}

func (a TopicPartitionLogAssertion) Run(logger *logger.Logger) error {
	logFilePath := getLogFilePathForTopic(a.topic, a.partition)
	logParser := logparser.NewLogFileParser(logFilePath)

	err := logParser.ReadLogFile(logger)
	if err != nil {
		return fmt.Errorf("Failed to read log file %s: %w", logFilePath, err)
	}

	expectedEncodedRecordBatches := serializer.GetEncodedBytes(a.RecordBatches)
	if len(expectedEncodedRecordBatches) == 0 {
		panic("Codecrafters Internal Error: RecordBatches are empty")
	}

	if bytes.Equal(expectedEncodedRecordBatches, logParser.RawBytes) {
		logger.Successf("✓ RecordBatches in log file (%s) match expected RecordBatches", logFilePath)
		return nil
	}

	mismatchOffset := findMisMatchOffset(expectedEncodedRecordBatches, logParser.RawBytes)
	expectedBytes := expectedEncodedRecordBatches[mismatchOffset:min(len(expectedEncodedRecordBatches), mismatchOffset+4)]
	actualBytes := logParser.RawBytes[mismatchOffset:min(len(logParser.RawBytes), mismatchOffset+4)]
	finalErr := fmt.Errorf("Expected bytes %d - %d, to be '%x', but got '%x'\nRecordBatches in log file (%s) do not match expected RecordBatches", mismatchOffset, min(len(logParser.RawBytes), mismatchOffset+4), expectedBytes, actualBytes, logFilePath)

	logger.UpdateLastSecondaryPrefix("logparser")
	defer logger.ResetSecondaryPrefixes()

	logger.Infof("Parsing log file (%s) belonging to topic (%s) and partition (%d)", logFilePath, a.topic, a.partition)
	err = logParser.ParseLogFile(logger)
	if err != nil {
		return fmt.Errorf("Failed to parse log file (%s): %w", logFilePath, err)
	}

	if len(logParser.RecordBatches) != len(a.RecordBatches) {
		return fmt.Errorf("Expected %d RecordBatches in log file (%s), got %d", len(a.RecordBatches), logFilePath, len(logParser.RecordBatches))
	}

	return formattedError(finalErr.Error(), mismatchOffset, logParser.RawBytes)
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
	return len(expectedBytes)
}
