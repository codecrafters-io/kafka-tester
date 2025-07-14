package dev

import (
	"encoding/binary"
	"fmt"
	"os"
	"time"

	"github.com/codecrafters-io/tester-utils/logger"
)

type TimeIndexEntry struct {
	Timestamp      int64 // Timestamp in milliseconds since Unix epoch
	RelativeOffset int32 // Offset relative to the base offset of the log segment
}

type TimeIndexResult struct {
	Entries []TimeIndexEntry
}

func DecodeTimeIndexFile(filePath string, logger *logger.Logger) (*TimeIndexResult, error) {
	data, err := os.ReadFile(filePath)
	if err != nil {
		return nil, fmt.Errorf("error reading time index file %s: %w", filePath, err)
	}

	if len(data) == 0 {
		logger.Debugf("Time index file %s is empty", filePath)
		return &TimeIndexResult{Entries: []TimeIndexEntry{}}, nil
	}

	return DecodeTimeIndex(data, logger)
}

func DecodeTimeIndex(data []byte, logger *logger.Logger) (*TimeIndexResult, error) {
	entrySize := 12 // 8 bytes timestamp + 4 bytes relative offset
	
	if len(data)%entrySize != 0 {
		return nil, fmt.Errorf("invalid time index file size: %d bytes (must be multiple of %d)", len(data), entrySize)
	}

	numEntries := len(data) / entrySize
	result := &TimeIndexResult{
		Entries: make([]TimeIndexEntry, numEntries),
	}

	offset := 0
	for i := range numEntries {
		timestamp := int64(binary.BigEndian.Uint64(data[offset:offset+8]))
		relativeOffset := int32(binary.BigEndian.Uint32(data[offset+8:offset+12]))
		
		result.Entries[i] = TimeIndexEntry{
			Timestamp:      timestamp,
			RelativeOffset: relativeOffset,
		}
		
		logger.Debugf("Time index entry %d: timestamp=%d (%s), relative_offset=%d", 
			i, timestamp, time.UnixMilli(timestamp).Format(time.RFC3339), relativeOffset)
		offset += entrySize
	}

	logger.Debugf("Successfully decoded %d time index entries", numEntries)
	return result, nil
}

func (result *TimeIndexResult) PrintTimeIndexSummary(logger *logger.Logger) {
	logger.Infof("=== Time Index Summary ===")
	logger.Infof("Total Time Index Entries: %d", len(result.Entries))
	
	if len(result.Entries) == 0 {
		logger.Infof("Time index is empty (sparse indexing - no entries yet)")
		return
	}
	
	for i, entry := range result.Entries {
		timeStr := time.UnixMilli(entry.Timestamp).Format(time.RFC3339)
		logger.Infof("Entry[%d]: timestamp=%d (%s), relative_offset=%d", 
			i, entry.Timestamp, timeStr, entry.RelativeOffset)
	}
}

func (result *TimeIndexResult) FindOffsetByTime(targetTimestamp int64) int32 {
	if len(result.Entries) == 0 {
		return -1
	}
	
	// Binary search to find the largest timestamp <= target
	left, right := 0, len(result.Entries)-1
	relativeOffset := int32(-1)
	
	for left <= right {
		mid := (left + right) / 2
		
		if result.Entries[mid].Timestamp <= targetTimestamp {
			relativeOffset = result.Entries[mid].RelativeOffset
			left = mid + 1
		} else {
			right = mid - 1
		}
	}
	
	return relativeOffset
}

func (result *TimeIndexResult) GetTimeIndexRange() (minTime, maxTime int64, valid bool) {
	if len(result.Entries) == 0 {
		return 0, 0, false
	}
	
	return result.Entries[0].Timestamp, 
		   result.Entries[len(result.Entries)-1].Timestamp, 
		   true
}

func FormatTimestamp(timestamp int64) string {
	return time.UnixMilli(timestamp).Format("2006-01-02 15:04:05.000 MST")
}