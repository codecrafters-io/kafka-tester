package logparser

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	kafkaapi "github.com/codecrafters-io/kafka-tester/protocol/api"
	"github.com/codecrafters-io/kafka-tester/protocol/decoder"
	"github.com/codecrafters-io/tester-utils/logger"
)

// LogFileParser handles parsing and printing 000.log files
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

// NewLogFileParser creates a new LogFileParser instance
func NewLogFileParser(logger *logger.Logger) *LogFileParser {
	return &LogFileParser{
		logger: logger,
	}
}

// ParseLogFile parses a single .log file and returns the result
func (p *LogFileParser) ParseLogFile(filePath string) (*LogFileResult, error) {
	// Read the file
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

// parseLogData parses the binary log data using the builtin decoder
func (p *LogFileParser) parseLogData(filePath string, data []byte) (*LogFileResult, error) {
	realDecoder := &decoder.RealDecoder{}
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

		// Extract messages from records
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

// PrintLogFile parses and prints a single .log file
func (p *LogFileParser) PrintLogFile(filePath string) error {
	result, err := p.ParseLogFile(filePath)
	if err != nil {
		return err
	}

	p.printLogFileResult(result)
	return nil
}

// PrintAllLogFiles finds and prints all 000.log files in the given directory
func (p *LogFileParser) PrintAllLogFiles(baseDir string) error {
	p.logger.Infof("=== Scanning for 000.log files in %s ===", baseDir)

	logFiles, err := p.findLogFiles(baseDir)
	if err != nil {
		return err
	}

	if len(logFiles) == 0 {
		p.logger.Infof("No 000.log files found in %s", baseDir)
		return nil
	}

	p.logger.Infof("Found %d log files:", len(logFiles))
	for _, file := range logFiles {
		p.logger.Infof("  - %s", file)
	}

	// Parse and print each log file
	for _, filePath := range logFiles {
		p.logger.Infof("\n=== Parsing %s ===", filePath)
		err := p.PrintLogFile(filePath)
		if err != nil {
			p.logger.Errorf("Failed to parse %s: %v", filePath, err)
		}
	}

	return nil
}

// findLogFiles recursively finds all files named "00000000000000000000.log"
func (p *LogFileParser) findLogFiles(baseDir string) ([]string, error) {
	var logFiles []string

	err := filepath.Walk(baseDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			p.logger.Debugf("Error accessing %s: %v", path, err)
			return nil // Continue walking
		}

		if !info.IsDir() && info.Name() == "00000000000000000000.log" {
			logFiles = append(logFiles, path)
		}

		return nil
	})

	return logFiles, err
}

// printLogFileResult prints the detailed results of parsing a log file
func (p *LogFileParser) printLogFileResult(result *LogFileResult) {
	relativePath := result.FilePath
	if baseDir := "/tmp/kraft-combined-logs"; strings.HasPrefix(result.FilePath, baseDir) {
		if rel, err := filepath.Rel(baseDir, result.FilePath); err == nil {
			relativePath = rel
		}
	}

	p.logger.Infof("📄 File: %s", relativePath)
	p.logger.Infof("   Record Batches: %d", len(result.RecordBatches))
	p.logger.Infof("   Total Records: %d", result.TotalRecords)
	p.logger.Infof("   Messages: %d", len(result.Messages))

	// Print detailed record batch information
	for i, batch := range result.RecordBatches {
		p.logger.Infof("   RecordBatch[%d]:", i)
		p.logger.Infof("     - BaseOffset: %d", batch.BaseOffset)
		p.logger.Infof("     - RecordCount: %d", len(batch.Records))
		p.logger.Infof("     - FirstTimestamp: %d", batch.FirstTimestamp)
		p.logger.Infof("     - MaxTimestamp: %d", batch.MaxTimestamp)
		p.logger.Infof("     - ProducerId: %d", batch.ProducerId)
		p.logger.Infof("     - ProducerEpoch: %d", batch.ProducerEpoch)
		p.logger.Infof("     - BaseSequence: %d", batch.BaseSequence)

		// Print individual records
		for j, record := range batch.Records {
			p.logger.Infof("       Record[%d]:", j)
			p.logger.Infof("         - TimestampDelta: %d", record.TimestampDelta)
			p.logger.Infof("         - OffsetDelta: %d", record.OffsetDelta)

			if record.Key != nil {
				p.logger.Infof("         - Key: %q", string(record.Key))
			} else {
				p.logger.Infof("         - Key: null")
			}

			if record.Value != nil {
				p.logger.Infof("         - Value: %q", string(record.Value))
			} else {
				p.logger.Infof("         - Value: null")
			}

			p.logger.Infof("         - Headers: %d", len(record.Headers))
			for k, header := range record.Headers {
				p.logger.Infof("           [%d]: %s = %q", k, header.Key, string(header.Value))
			}
		}
	}

	// Print all messages
	if len(result.Messages) > 0 {
		p.logger.Infof("   Extracted Messages:")
		for i, msg := range result.Messages {
			p.logger.Infof("     [%d]: %q", i, msg)
		}
	}
}
