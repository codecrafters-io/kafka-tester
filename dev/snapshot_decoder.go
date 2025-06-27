package dev

import (
	"encoding/binary"
	"fmt"
	"os"

	"github.com/codecrafters-io/tester-utils/logger"
)

type SnapshotHeader struct {
	Version    int16 // Snapshot format version
	CRC        int32 // CRC32 checksum
	LastOffset int64 // Last included offset
}

type SnapshotResult struct {
	Header   SnapshotHeader
	Data     []byte // Raw snapshot data after header
	Filename string // Original filename for context
}

func DecodeSnapshotFile(filePath string, logger *logger.Logger) (*SnapshotResult, error) {
	data, err := os.ReadFile(filePath)
	if err != nil {
		return nil, fmt.Errorf("error reading snapshot file %s: %w", filePath, err)
	}

	if len(data) < 10 {
		return nil, fmt.Errorf("snapshot file too small: %d bytes (minimum 10 bytes for header)", len(data))
	}

	return DecodeSnapshot(data, filePath, logger)
}

func DecodeSnapshot(data []byte, filename string, logger *logger.Logger) (*SnapshotResult, error) {
	result := &SnapshotResult{
		Filename: filename,
	}

	offset := 0
	
	result.Header.Version = int16(binary.BigEndian.Uint16(data[offset:offset+2]))
	offset += 2
	logger.Debugf("Snapshot version: %d", result.Header.Version)

	result.Header.CRC = int32(binary.BigEndian.Uint32(data[offset:offset+4]))
	offset += 4
	logger.Debugf("Snapshot CRC: 0x%08x", uint32(result.Header.CRC))

	if result.Header.Version >= 1 { // For version 1+, last offset might be 8 bytes
		if len(data) >= 14 {
			result.Header.LastOffset = int64(binary.BigEndian.Uint64(data[offset:offset+8]))
			offset += 8
		} else {
			result.Header.LastOffset = int64(binary.BigEndian.Uint32(data[offset:offset+4]))
			offset += 4
		}
	} else {
		result.Header.LastOffset = int64(binary.BigEndian.Uint32(data[offset:offset+4]))
		offset += 4
	}
	logger.Debugf("Last offset: %d", result.Header.LastOffset)

	if offset < len(data) {
		result.Data = data[offset:]
		logger.Debugf("Snapshot data size: %d bytes", len(result.Data))
	} else {
		result.Data = []byte{}
		logger.Debugf("No snapshot data beyond header")
	}

	return result, nil
}

func (result *SnapshotResult) PrintSnapshotSummary(logger *logger.Logger) {
	logger.Infof("=== Snapshot Summary ===")
	logger.Infof("File: %s", result.Filename)
	logger.Infof("Version: %d", result.Header.Version)
	logger.Infof("CRC: 0x%08x", uint32(result.Header.CRC))
	logger.Infof("Last Offset: %d", result.Header.LastOffset)
	logger.Infof("Data Size: %d bytes", len(result.Data))
	
	if len(result.Data) > 0 {
		logger.Infof("Data Preview (first 32 bytes):")
		previewLen := 32
		if len(result.Data) < previewLen {
			previewLen = len(result.Data)
		}
		
		for i := 0; i < previewLen; i += 16 {
			end := i + 16
			if end > previewLen {
				end = previewLen
			}
			
			hexStr := ""
			asciiStr := ""
			for j := i; j < end; j++ {
				hexStr += fmt.Sprintf("%02x ", result.Data[j])
				if result.Data[j] >= 32 && result.Data[j] <= 126 {
					asciiStr += string(result.Data[j])
				} else {
					asciiStr += "."
				}
			}
			
			logger.Infof("  %04x: %-48s |%s|", i, hexStr, asciiStr)
		}
		
		if len(result.Data) > previewLen {
			logger.Infof("  ... (%d more bytes)", len(result.Data)-previewLen)
		}
	}
}

func ExtractOffsetFromFilename(filename string) (int64, error) {
	// Find the last part of the path
	lastSlash := -1
	for i := len(filename) - 1; i >= 0; i-- {
		if filename[i] == '/' || filename[i] == '\\' {
			lastSlash = i
			break
		}
	}
	
	basename := filename
	if lastSlash >= 0 {
		basename = filename[lastSlash+1:]
	}
	
	if len(basename) > 9 && basename[len(basename)-9:] == ".snapshot" {
		basename = basename[:len(basename)-9]
	}
	
	var offset int64
	_, err := fmt.Sscanf(basename, "%020d", &offset)
	if err != nil {
		return 0, fmt.Errorf("failed to parse offset from filename %s: %w", filename, err)
	}
	
	return offset, nil
}

func (result *SnapshotResult) IsKRaftMetadataSnapshot() bool {
	return result.Header.Version >= 1
}

func (result *SnapshotResult) FormatSnapshotInfo() string {
	offsetFromFile, _ := ExtractOffsetFromFilename(result.Filename)
	return fmt.Sprintf("Snapshot at offset %d (last_offset=%d, version=%d, %d bytes data)", 
		offsetFromFile, result.Header.LastOffset, result.Header.Version, len(result.Data))
}