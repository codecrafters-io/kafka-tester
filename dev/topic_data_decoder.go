package dev

import (
	"fmt"
	"os"

	kafkaapi "github.com/codecrafters-io/kafka-tester/protocol/api"
	"github.com/codecrafters-io/kafka-tester/protocol/decoder"
	"github.com/codecrafters-io/tester-utils/logger"
)

type TopicDataResult struct {
	RecordBatches []kafkaapi.RecordBatch
	Messages      []string
}

func DecodeTopicDataFile(filePath string, logger *logger.Logger) (*TopicDataResult, error) {
	data, err := os.ReadFile(filePath)
	if err != nil {
		return nil, fmt.Errorf("error reading file %s: %w", filePath, err)
	}

	if len(data) == 0 {
		logger.Debugf("File %s is empty", filePath)
		return &TopicDataResult{
			RecordBatches: []kafkaapi.RecordBatch{},
			Messages:      []string{},
		}, nil
	}

	return DecodeTopicData(data, logger)
}

func DecodeTopicData(data []byte, logger *logger.Logger) (*TopicDataResult, error) {
	realDecoder := &decoder.RealDecoder{}
	realDecoder.Init(data)

	result := &TopicDataResult{
		RecordBatches: []kafkaapi.RecordBatch{},
		Messages:      []string{},
	}

	batchIndex := 0
	for realDecoder.Remaining() > 0 {
		recordBatch := kafkaapi.RecordBatch{}

		logger.Debugf("Decoding RecordBatch[%d] at offset %d", batchIndex, realDecoder.Offset())

		err := recordBatch.Decode(realDecoder, logger, 1)
		if err != nil {
			return nil, fmt.Errorf("error decoding record batch %d: %w", batchIndex, err)
		}

		result.RecordBatches = append(result.RecordBatches, recordBatch)

		for _, record := range recordBatch.Records {
			if record.Value != nil {
				result.Messages = append(result.Messages, string(record.Value))
			}
		}

		batchIndex++
	}

	logger.Debugf("Successfully decoded %d record batches with %d total messages",
		len(result.RecordBatches), len(result.Messages))

	return result, nil
}

func ExtractMessagesFromFile(filePath string, logger *logger.Logger) ([]string, error) {
	result, err := DecodeTopicDataFile(filePath, logger)
	if err != nil {
		return nil, err
	}
	return result.Messages, nil
}

func (result *TopicDataResult) PrintSummary(logger *logger.Logger) {
	logger.Infof("=== Topic Data Summary ===")
	logger.Infof("Total Record Batches: %d", len(result.RecordBatches))
	logger.Infof("Total Messages: %d", len(result.Messages))

	for i, batch := range result.RecordBatches {
		logger.Infof("RecordBatch[%d]:", i)
		logger.Infof("  - BaseOffset: %d", batch.BaseOffset)
		logger.Infof("  - Records: %d", len(batch.Records))
		logger.Infof("  - FirstTimestamp: %d", batch.FirstTimestamp)
		logger.Infof("  - ProducerId: %d", batch.ProducerId)

		for j, record := range batch.Records {
			logger.Infof("    Record[%d]: %q", j, string(record.Value))
		}
	}

	logger.Infof("=== Messages ===")
	for i, msg := range result.Messages {
		logger.Infof("Message[%d]: %q", i, msg)
	}
}
