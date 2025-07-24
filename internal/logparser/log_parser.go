package logparser

import (
	"fmt"
	"os"

	kafkaapi "github.com/codecrafters-io/kafka-tester/protocol/api"
	"github.com/codecrafters-io/kafka-tester/protocol/decoder"
	"github.com/codecrafters-io/tester-utils/logger"
)

type LogFileParser struct {
	logger *logger.Logger
}

// LogFileResult contains the parsed data from a log file
type LogFileResult struct {
	FilePath      string
	RecordBatches []kafkaapi.RecordBatch
	Messages      []string
	TotalRecords  int
}

func NewLogFileParser(logger *logger.Logger) *LogFileParser {
	return &LogFileParser{
		logger: logger,
	}
}

func (p *LogFileParser) ParseLogFile(filePath string) (*LogFileResult, error) {
	data, err := os.ReadFile(filePath)
	if err != nil {
		return nil, fmt.Errorf("error reading file %s: %w", filePath, err)
	}

	if len(data) == 0 {
		p.logger.Debugf("File %s is empty", filePath)
		return &LogFileResult{
			FilePath:      filePath,
			RecordBatches: []kafkaapi.RecordBatch{},
			Messages:      []string{},
			TotalRecords:  0,
		}, nil
	}

	return p.parseLogData(filePath, data)
}

func (p *LogFileParser) parseLogData(filePath string, data []byte) (*LogFileResult, error) {
	realDecoder := &decoder.Decoder{}
	realDecoder.Init(data)

	result := &LogFileResult{
		FilePath:      filePath,
		RecordBatches: []kafkaapi.RecordBatch{},
		Messages:      []string{},
		TotalRecords:  0,
	}

	batchIndex := 0
	for realDecoder.Remaining() > 0 {
		recordBatch := kafkaapi.RecordBatch{}

		p.logger.Debugf("Decoding RecordBatch[%d] at offset %d", batchIndex, realDecoder.Offset())

		err := recordBatch.Decode(realDecoder, p.logger, 1)
		if err != nil {
			return nil, fmt.Errorf("error decoding record batch %d: %w", batchIndex, err)
		}

		result.RecordBatches = append(result.RecordBatches, recordBatch)

		for _, record := range recordBatch.Records {
			if record.Value != nil {
				result.Messages = append(result.Messages, string(record.Value))
			}
			result.TotalRecords++
		}

		batchIndex++
	}

	p.logger.Debugf("Successfully decoded %d record batches with %d total records",
		len(result.RecordBatches), result.TotalRecords)

	return result, nil
}
