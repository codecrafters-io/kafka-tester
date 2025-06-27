package dev

import (
	"encoding/binary"
	"fmt"
	"os"

	"github.com/codecrafters-io/tester-utils/logger"
)

type IndexEntry struct {
	RelativeOffset int32 // Offset relative to the base offset of the log segment
	Position       int32 // Physical position in the log file
}

type IndexResult struct {
	Entries []IndexEntry
}

func DecodeIndexFile(filePath string, logger *logger.Logger) (*IndexResult, error) {
	data, err := os.ReadFile(filePath)
	if err != nil {
		return nil, fmt.Errorf("error reading index file %s: %w", filePath, err)
	}

	if len(data) == 0 {
		logger.Debugf("Index file %s is empty", filePath)
		return &IndexResult{Entries: []IndexEntry{}}, nil
	}

	return DecodeIndex(data, logger)
}

func DecodeIndex(data []byte, logger *logger.Logger) (*IndexResult, error) {
	entrySize := 8 // 4 bytes relative offset + 4 bytes position

	if len(data)%entrySize != 0 {
		return nil, fmt.Errorf("invalid index file size: %d bytes (must be multiple of %d)", len(data), entrySize)
	}

	numEntries := len(data) / entrySize
	result := &IndexResult{
		Entries: make([]IndexEntry, numEntries),
	}

	offset := 0
	for i := range numEntries {
		relativeOffset := int32(binary.BigEndian.Uint32(data[offset : offset+4]))
		position := int32(binary.BigEndian.Uint32(data[offset+4 : offset+8]))

		result.Entries[i] = IndexEntry{
			RelativeOffset: relativeOffset,
			Position:       position,
		}

		logger.Debugf("Index entry %d: relative_offset=%d, position=%d", i, relativeOffset, position)
		offset += entrySize
	}

	logger.Debugf("Successfully decoded %d index entries", numEntries)
	return result, nil
}

func (result *IndexResult) PrintSummary(logger *logger.Logger) {
	logger.Infof("=== Index Summary ===")
	logger.Infof("Total Index Entries: %d", len(result.Entries))

	if len(result.Entries) == 0 {
		logger.Infof("Index is empty (sparse indexing - no entries yet)")
		return
	}

	for i, entry := range result.Entries {
		logger.Infof("Entry[%d]: relative_offset=%d, position=%d",
			i, entry.RelativeOffset, entry.Position)
	}
}

func (result *IndexResult) FindPosition(targetRelativeOffset int32) int32 {
	if len(result.Entries) == 0 {
		return -1
	}

	// Binary search to find the largest offset <= target
	left, right := 0, len(result.Entries)-1
	position := int32(-1)

	for left <= right {
		mid := (left + right) / 2

		if result.Entries[mid].RelativeOffset <= targetRelativeOffset {
			position = result.Entries[mid].Position
			left = mid + 1
		} else {
			right = mid - 1
		}
	}

	return position
}

func (result *IndexResult) GetIndexRange() (minOffset, maxOffset int32, valid bool) {
	if len(result.Entries) == 0 {
		return 0, 0, false
	}

	return result.Entries[0].RelativeOffset,
		result.Entries[len(result.Entries)-1].RelativeOffset,
		true
}
