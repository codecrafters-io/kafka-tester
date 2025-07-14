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
// but excludes cluster metadata files
func (p *LogFileParser) findLogFiles(baseDir string) ([]string, error) {
	var logFiles []string

	err := filepath.Walk(baseDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			p.logger.Debugf("Error accessing %s: %v", path, err)
			return nil // Continue walking
		}

		if !info.IsDir() && info.Name() == "00000000000000000000.log" {
			// Skip cluster metadata files
			if strings.Contains(path, "__cluster_metadata-0") {
				p.logger.Debugf("Skipping cluster metadata file: %s", path)
				return nil
			}
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
				// Try to parse the payload if it looks like structured data
				if len(record.Value) > 4 { // Minimum size for a structured payload
					p.parseAndPrintPayload(record.Value)
				}
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

// parseAndPrintPayload attempts to parse the record value as a ClusterMetadataPayload
func (p *LogFileParser) parseAndPrintPayload(data []byte) {
	payload := kafkaapi.ClusterMetadataPayload{}
	err := payload.Decode(data)
	if err != nil {
		p.logger.Debugf("         - Failed to decode payload: %v", err)
		return
	}

	p.logger.Infof("         - Decoded Payload:")
	p.logger.Infof("           - FrameVersion: %d", payload.FrameVersion)
	p.logger.Infof("           - Type: %d", payload.Type)
	p.logger.Infof("           - Version: %d", payload.Version)
	
	p.printPayloadData(payload.Data, int16(payload.Type))
}

// printPayloadData prints the specific payload data based on its type
func (p *LogFileParser) printPayloadData(data interface{}, payloadType int16) {
	switch payloadType {
	case 2: // TopicRecord
		if topicRecord, ok := data.(*kafkaapi.TopicRecord); ok {
			p.logger.Infof("           - TopicRecord:")
			p.logger.Infof("             - TopicName: %s", topicRecord.TopicName)
			p.logger.Infof("             - TopicUUID: %s", topicRecord.TopicUUID)
		}
	case 3: // PartitionRecord
		if partitionRecord, ok := data.(*kafkaapi.PartitionRecord); ok {
			p.logger.Infof("           - PartitionRecord:")
			p.logger.Infof("             - PartitionID: %d", partitionRecord.PartitionID)
			p.logger.Infof("             - TopicUUID: %s", partitionRecord.TopicUUID)
			p.logger.Infof("             - Replicas: %v", partitionRecord.Replicas)
			p.logger.Infof("             - ISReplicas: %v", partitionRecord.ISReplicas)
			p.logger.Infof("             - Leader: %d", partitionRecord.Leader)
			p.logger.Infof("             - LeaderEpoch: %d", partitionRecord.LeaderEpoch)
			p.logger.Infof("             - PartitionEpoch: %d", partitionRecord.PartitionEpoch)
			if len(partitionRecord.Directories) > 0 {
				p.logger.Infof("             - Directories: %v", partitionRecord.Directories)
			}
		}
	case 12: // FeatureLevelRecord
		if featureRecord, ok := data.(*kafkaapi.FeatureLevelRecord); ok {
			p.logger.Infof("           - FeatureLevelRecord:")
			p.logger.Infof("             - Name: %s", featureRecord.Name)
			p.logger.Infof("             - FeatureLevel: %d", featureRecord.FeatureLevel)
		}
	case 21: // ZKMigrationStateRecord
		if zkRecord, ok := data.(*kafkaapi.ZKMigrationStateRecord); ok {
			p.logger.Infof("           - ZKMigrationStateRecord:")
			p.logger.Infof("             - MigrationState: %d", zkRecord.MigrationState)
		}
	case 23: // BeginTransactionRecord
		if beginRecord, ok := data.(*kafkaapi.BeginTransactionRecord); ok {
			p.logger.Infof("           - BeginTransactionRecord:")
			p.logger.Infof("             - Name: %s", beginRecord.Name)
		}
	case 24: // EndTransactionRecord
		if _, ok := data.(*kafkaapi.EndTransactionRecord); ok {
			p.logger.Infof("           - EndTransactionRecord: (no fields)")
		}
	default:
		p.logger.Infof("           - Unknown payload type: %d", payloadType)
	}
}

// PrintLogFilesInDirectory prints all 000.log files in the given directory with a prefix
func (p *LogFileParser) PrintLogFilesInDirectory(baseDir string, prefix string) error {
	p.logger.Infof("=== %s: Scanning for 000.log files in %s ===", prefix, baseDir)

	logFiles, err := p.findLogFiles(baseDir)
	if err != nil {
		return err
	}

	if len(logFiles) == 0 {
		p.logger.Infof("No 000.log files found in %s", baseDir)
		return nil
	}

	p.logger.Infof("Found %d log files", len(logFiles))

	// Parse and print each log file
	for _, filePath := range logFiles {
		relativePath := filePath
		if rel, err := filepath.Rel(baseDir, filePath); err == nil {
			relativePath = rel
		}
		p.logger.Infof("\n--- %s: %s ---", prefix, relativePath)
		
		result, err := p.ParseLogFile(filePath)
		if err != nil {
			p.logger.Errorf("Failed to parse %s: %v", filePath, err)
			continue
		}
		
		p.printLogFileResult(result)
	}

	return nil
}
