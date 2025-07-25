package logparser

import (
	"fmt"
	"os"

	kafkaapi "github.com/codecrafters-io/kafka-tester/protocol/api"
	"github.com/codecrafters-io/kafka-tester/protocol/decoder"
	"github.com/codecrafters-io/tester-utils/logger"
)

type LogFileParser struct {
	filePath      string
	RawBytes      []byte
	RecordBatches []kafkaapi.RecordBatch
}

func NewLogFileParser(filePath string) *LogFileParser {
	return &LogFileParser{
		filePath: filePath,
	}
}

func (p *LogFileParser) ReadLogFile(logger *logger.Logger) error {
	data, err := os.ReadFile(p.filePath)
	if err != nil {
		return fmt.Errorf("error reading file %s: %w", p.filePath, err)
	}

	if len(data) == 0 {
		logger.Errorf("File %s is empty", p.filePath)
	}
	p.RawBytes = data

	return nil
}

func (p *LogFileParser) ParseLogFile(logger *logger.Logger) error {
	realDecoder := &decoder.Decoder{}
	realDecoder.Init(p.RawBytes)

	batchIndex := 0
	for realDecoder.Remaining() > 0 {
		recordBatch := kafkaapi.RecordBatch{}

		logger.Debugf("Decoding RecordBatch[%d] at offset %d", batchIndex, realDecoder.Offset())

		err := recordBatch.Decode(realDecoder, logger, 1)
		if err != nil {
			return fmt.Errorf("Error decoding record batch %d: %w", batchIndex, err)
		}

		p.RecordBatches = append(p.RecordBatches, recordBatch)
		batchIndex++
	}

	logger.Debugf("Successfully decoded %d record batches", len(p.RecordBatches))

	return nil
}
