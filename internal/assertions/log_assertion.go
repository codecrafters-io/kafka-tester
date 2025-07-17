package assertions

import (
	"bytes"
	"fmt"

	"github.com/codecrafters-io/kafka-tester/internal/logparser"
	kafkaapi "github.com/codecrafters-io/kafka-tester/protocol/api"
	"github.com/codecrafters-io/kafka-tester/protocol/common"
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
	a.logger.UpdateSecondaryPrefix("logparser")
	actualEncodedRecordBatches, err := encodeRecordBatchesInLogFile(a.topic, a.partition, a.logger)
	a.logger.ResetSecondaryPrefix()
	if err != nil {
		return err
	}

	expectedEncodedBatches := encodeMultipleRecordBatches(a.RecordBatches)

	if bytes.Equal(actualEncodedRecordBatches[0], expectedEncodedBatches[0]) && bytes.Equal(actualEncodedRecordBatches[1], expectedEncodedBatches[1]) {
		a.logger.Infof("RecordBatches in log file match expected RecordBatches")
	} else {
		fmt.Println("encodedBatchesFromRequests[0]", expectedEncodedBatches[0])
		fmt.Println("encodedBatches[0]", actualEncodedRecordBatches[0])
		fmt.Println("encodedBatchesFromRequests[1]", expectedEncodedBatches[1])
		fmt.Println("encodedBatches[1]", actualEncodedRecordBatches[1])
		a.logger.Errorf("RecordBatches in log file do not match expected RecordBatches")
		return fmt.Errorf("RecordBatches in log file do not match expected RecordBatches")
	}

	return nil
}

// getLogFilePathForTopic builds the log file path for a given topic and partition
func getLogFilePathForTopic(topicName string, partition int32) string {
	return fmt.Sprintf("%s/%s-%d/00000000000000000000.log", common.LOG_DIR, topicName, partition)
}

func encodeRecordBatchesInLogFile(topicName string, partition int32, logger *logger.Logger) ([][]byte, error) {
	logFilePath := getLogFilePathForTopic(topicName, partition)

	logParser := logparser.NewLogFileParser(logger)
	result, err := logParser.ParseLogFile(logFilePath)
	if err != nil {
		return nil, fmt.Errorf("failed to parse log file %s: %w", logFilePath, err)
	}

	encodedBatches := make([][]byte, 0)

	for _, batch := range result.RecordBatches {
		as := serializer.GetAnyEncodedBytes(batch)
		encodedBatches = append(encodedBatches, as)
	}

	logger.Infof("Found %d RecordBatches in %s", len(result.RecordBatches), logFilePath)
	return encodedBatches, nil
}

func encodeMultipleRecordBatches(recordBatches []kafkaapi.RecordBatch) [][]byte {
	encodedBatches := make([][]byte, 0)
	for i, recordBatch := range recordBatches {
		recordBatch.BaseOffset = int64(i)
		as := serializer.GetAnyEncodedBytes(recordBatch)
		encodedBatches = append(encodedBatches, as)
	}
	return encodedBatches
}
