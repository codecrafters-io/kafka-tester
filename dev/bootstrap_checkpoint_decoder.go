package dev

import (
	"encoding/binary"
	"fmt"
	"os"

	"github.com/codecrafters-io/tester-utils/logger"
)

type BootstrapCheckpointEntry struct {
	Offset    int64  // Offset in the log
	Epoch     int32  // Leader epoch
	Timestamp int64  // Timestamp
	CRC       int32  // CRC checksum
	Length    int32  // Record length
	Data      []byte // Raw record data
}

// BootstrapCheckpointResult represents the parsed bootstrap checkpoint file
type BootstrapCheckpointResult struct {
	Header  []byte                      // File header
	Entries []BootstrapCheckpointEntry // Checkpoint entries
}

// DecodeBootstrapCheckpointFile reads and parses a Kafka bootstrap.checkpoint file
func DecodeBootstrapCheckpointFile(filePath string, logger *logger.Logger) (*BootstrapCheckpointResult, error) {
	data, err := os.ReadFile(filePath)
	if err != nil {
		return nil, fmt.Errorf("error reading bootstrap checkpoint file %s: %w", filePath, err)
	}

	if len(data) < 8 {
		return nil, fmt.Errorf("bootstrap checkpoint file too small: %d bytes", len(data))
	}

	return DecodeBootstrapCheckpoint(data, logger)
}

// DecodeBootstrapCheckpoint parses bootstrap checkpoint data from raw bytes
func DecodeBootstrapCheckpoint(data []byte, logger *logger.Logger) (*BootstrapCheckpointResult, error) {
	result := &BootstrapCheckpointResult{}
	offset := 0

	// The file appears to start with some header information
	// First 8 bytes might be a header offset or version
	if len(data) < 16 {
		return nil, fmt.Errorf("insufficient data for bootstrap checkpoint header")
	}

	headerOffset := int64(binary.BigEndian.Uint64(data[0:8]))
	logger.Debugf("Header offset or version: %d", headerOffset)

	// Next appears to be entry count or similar
	entryCount := int32(binary.BigEndian.Uint32(data[8:12]))
	logger.Debugf("Potential entry count: %d", entryCount)

	offset = 16 // Skip initial header

	// Try to parse entries - this is somewhat speculative based on the hex dump
	for offset < len(data)-8 {
		entry := BootstrapCheckpointEntry{}

		// Each entry seems to have a similar structure
		if offset+20 > len(data) {
			break // Not enough data for a complete entry
		}

		// Try to extract what looks like an offset (8 bytes)
		if offset+8 <= len(data) {
			entry.Offset = int64(binary.BigEndian.Uint64(data[offset:offset+8]))
			offset += 8
		}

		// CRC or similar (4 bytes)
		if offset+4 <= len(data) {
			entry.CRC = int32(binary.BigEndian.Uint32(data[offset:offset+4]))
			offset += 4
		}

		// Length field (4 bytes)
		if offset+4 <= len(data) {
			entry.Length = int32(binary.BigEndian.Uint32(data[offset:offset+4]))
			offset += 4

			// Validate length is reasonable
			if entry.Length < 0 || entry.Length > 10000 || offset+int(entry.Length) > len(data) {
				logger.Debugf("Stopping parsing at offset %d due to invalid length %d", offset-4, entry.Length)
				break
			}
		}

		// Timestamp (8 bytes)
		if offset+8 <= len(data) {
			entry.Timestamp = int64(binary.BigEndian.Uint64(data[offset:offset+8]))
			offset += 8
		}

		// Skip some bytes that might be padding or additional fields
		skipBytes := 8
		if offset+skipBytes <= len(data) {
			offset += skipBytes
		}

		// Extract data if length makes sense
		if entry.Length > 0 && entry.Length <= 1000 && offset+int(entry.Length) <= len(data) {
			entry.Data = make([]byte, entry.Length)
			copy(entry.Data, data[offset:offset+int(entry.Length)])
			offset += int(entry.Length)
		} else {
			// Try to find next potential record boundary
			// Look for patterns in the data
			nextOffset := findNextRecordBoundary(data, offset)
			if nextOffset > offset && nextOffset < len(data) {
				remainingData := nextOffset - offset
				if remainingData > 0 && remainingData < 200 {
					entry.Data = make([]byte, remainingData)
					copy(entry.Data, data[offset:nextOffset])
					offset = nextOffset
				} else {
					offset += 32 // Skip ahead a bit
				}
			} else {
				offset += 32 // Skip ahead if we can't find a pattern
			}
		}

		result.Entries = append(result.Entries, entry)
		logger.Debugf("Parsed entry: offset=%d, crc=0x%x, length=%d, timestamp=%d", 
			entry.Offset, uint32(entry.CRC), entry.Length, entry.Timestamp)

		// Prevent infinite loops
		if len(result.Entries) > 50 {
			logger.Debugf("Stopping after parsing %d entries to prevent infinite loop", len(result.Entries))
			break
		}
	}

	logger.Debugf("Successfully parsed %d bootstrap checkpoint entries", len(result.Entries))
	return result, nil
}

// findNextRecordBoundary tries to find the start of the next record
func findNextRecordBoundary(data []byte, start int) int {
	// Look for potential patterns that might indicate record boundaries
	// This is heuristic-based since we don't have the exact format specification
	
	for i := start; i < len(data)-16; i += 4 { // Align to 4-byte boundaries
		// Look for what might be a timestamp (reasonable range)
		if i+8 <= len(data) {
			potentialTimestamp := int64(binary.BigEndian.Uint64(data[i:i+8]))
			// Check if this looks like a recent timestamp (around 2025)
			if potentialTimestamp > 1700000000000 && potentialTimestamp < 1800000000000 {
				return i - 16 // Back up to account for other fields before timestamp
			}
		}
	}
	
	return start + 64 // Default fallback
}

// PrintBootstrapCheckpointSummary prints a human-readable summary
func (result *BootstrapCheckpointResult) PrintBootstrapCheckpointSummary(logger *logger.Logger) {
	logger.Infof("=== Bootstrap Checkpoint Summary ===")
	logger.Infof("Total Entries: %d", len(result.Entries))
	
	for i, entry := range result.Entries {
		timeStr := ""
		if entry.Timestamp > 1000000000000 { // Looks like milliseconds since epoch
			timeStr = FormatTimestamp(entry.Timestamp)
		}
		
		logger.Infof("Entry[%d]:", i)
		logger.Infof("  Offset: %d", entry.Offset)
		logger.Infof("  CRC: 0x%08x", uint32(entry.CRC))
		logger.Infof("  Length: %d", entry.Length)
		logger.Infof("  Timestamp: %d (%s)", entry.Timestamp, timeStr)
		
		if len(entry.Data) > 0 {
			logger.Infof("  Data: %d bytes", len(entry.Data))
			
			// Show hex dump of first 32 bytes
			previewLen := 32
			if len(entry.Data) < previewLen {
				previewLen = len(entry.Data)
			}
			
			hexStr := ""
			asciiStr := ""
			for j := 0; j < previewLen; j++ {
				hexStr += fmt.Sprintf("%02x ", entry.Data[j])
				if entry.Data[j] >= 32 && entry.Data[j] <= 126 {
					asciiStr += string(entry.Data[j])
				} else {
					asciiStr += "."
				}
			}
			
			logger.Infof("    Preview: %s |%s|", hexStr, asciiStr)
			if len(entry.Data) > previewLen {
				logger.Infof("    ... (%d more bytes)", len(entry.Data)-previewLen)
			}
		}
	}
}