package main

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/codecrafters-io/kafka-tester/dev"
	"github.com/codecrafters-io/tester-utils/logger"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "Usage: %s <kafka-logs-directory>\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "Example: %s /tmp/kraft-combined-logs\n", os.Args[0])
		os.Exit(1)
	}

	logsDir := os.Args[1]
	
	// Create a quiet logger
	log := logger.GetQuietLogger("")
	
	fmt.Printf("=== Kafka Logs Explorer ===\n")
	fmt.Printf("Scanning directory: %s\n\n", logsDir)
	
	// Walk the directory tree
	err := filepath.Walk(logsDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			fmt.Printf("Error accessing %s: %v\n", path, err)
			return nil // Continue walking
		}
		
		if info.IsDir() {
			return nil // Continue into subdirectories
		}
		
		// Determine file type and parse accordingly
		filename := info.Name()
		relativePath := getRelativePath(logsDir, path)
		
		fmt.Printf("📁 %s\n", relativePath)
		fmt.Printf("   Size: %d bytes\n", info.Size())
		
		switch {
		case strings.HasSuffix(filename, ".log"):
			parseLogFile(path, log)
		case strings.HasSuffix(filename, ".index"):
			parseIndexFile(path, log)
		case strings.HasSuffix(filename, ".timeindex"):
			parseTimeIndexFile(path, log)
		case strings.HasSuffix(filename, ".snapshot"):
			fmt.Printf("   Type: Snapshot file (skipped)\n")
		case filename == "partition.metadata":
			parsePartitionMetadata(path)
		case filename == "leader-epoch-checkpoint":
			parseLeaderEpochCheckpoint(path)
		case filename == "meta.properties":
			parseMetaProperties(path)
		case filename == "bootstrap.checkpoint":
			parseBootstrapCheckpoint(path)
		case strings.Contains(filename, "checkpoint"):
			parseCheckpointFile(path)
		default:
			fmt.Printf("   Type: Unknown file type\n")
		}
		
		fmt.Println()
		return nil
	})
	
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error walking directory: %v\n", err)
		os.Exit(1)
	}
}

func getRelativePath(base, full string) string {
	rel, err := filepath.Rel(base, full)
	if err != nil {
		return full
	}
	return rel
}

func parseLogFile(path string, log *logger.Logger) {
	fmt.Printf("   Type: Log file\n")
	
	result, err := dev.DecodeTopicDataFile(path, log)
	if err != nil {
		fmt.Printf("   Error: %v\n", err)
		return
	}
	
	fmt.Printf("   Record Batches: %d\n", len(result.RecordBatches))
	fmt.Printf("   Messages: %d\n", len(result.Messages))
	
	if len(result.Messages) > 0 {
		fmt.Printf("   Sample Messages:\n")
		for i, msg := range result.Messages {
			if i >= 3 { // Show max 3 messages
				fmt.Printf("     ... (%d more)\n", len(result.Messages)-i)
				break
			}
			// Truncate long messages
			displayMsg := msg
			if len(displayMsg) > 50 {
				displayMsg = displayMsg[:47] + "..."
			}
			fmt.Printf("     [%d]: %q\n", i, displayMsg)
		}
	}
}

func parseIndexFile(path string, log *logger.Logger) {
	fmt.Printf("   Type: Offset index\n")
	
	result, err := dev.DecodeIndexFile(path, log)
	if err != nil {
		fmt.Printf("   Error: %v\n", err)
		return
	}
	
	fmt.Printf("   Index Entries: %d\n", len(result.Entries))
	
	if len(result.Entries) > 0 {
		minOffset, maxOffset, valid := result.GetIndexRange()
		if valid {
			fmt.Printf("   Offset Range: %d to %d\n", minOffset, maxOffset)
		}
		
		// Show first few entries
		fmt.Printf("   Sample Entries:\n")
		for i, entry := range result.Entries {
			if i >= 3 {
				fmt.Printf("     ... (%d more)\n", len(result.Entries)-i)
				break
			}
			fmt.Printf("     [%d]: offset=%d -> position=%d\n", i, entry.RelativeOffset, entry.Position)
		}
	} else {
		fmt.Printf("   Status: Empty (sparse indexing)\n")
	}
}

func parseTimeIndexFile(path string, log *logger.Logger) {
	fmt.Printf("   Type: Time index\n")
	
	result, err := dev.DecodeTimeIndexFile(path, log)
	if err != nil {
		fmt.Printf("   Error: %v\n", err)
		return
	}
	
	fmt.Printf("   Time Index Entries: %d\n", len(result.Entries))
	
	if len(result.Entries) > 0 {
		minTime, maxTime, valid := result.GetTimeIndexRange()
		if valid {
			fmt.Printf("   Time Range: %s to %s\n", 
				dev.FormatTimestamp(minTime), dev.FormatTimestamp(maxTime))
			
			if len(result.Entries) > 1 {
				duration := (maxTime - minTime) / 1000 // Convert to seconds
				fmt.Printf("   Duration: %d seconds\n", duration)
			}
		}
		
		// Show first few entries
		fmt.Printf("   Sample Entries:\n")
		for i, entry := range result.Entries {
			if i >= 3 {
				fmt.Printf("     ... (%d more)\n", len(result.Entries)-i)
				break
			}
			timeStr := dev.FormatTimestamp(entry.Timestamp)
			fmt.Printf("     [%d]: %s -> offset=%d\n", i, timeStr, entry.RelativeOffset)
		}
	} else {
		fmt.Printf("   Status: Empty\n")
	}
}

func parsePartitionMetadata(path string) {
	fmt.Printf("   Type: Partition metadata\n")
	
	data, err := os.ReadFile(path)
	if err != nil {
		fmt.Printf("   Error: %v\n", err)
		return
	}
	
	lines := strings.Split(string(data), "\n")
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line != "" {
			fmt.Printf("   %s\n", line)
		}
	}
}

func parseLeaderEpochCheckpoint(path string) {
	fmt.Printf("   Type: Leader epoch checkpoint\n")
	
	data, err := os.ReadFile(path)
	if err != nil {
		fmt.Printf("   Error: %v\n", err)
		return
	}
	
	content := strings.TrimSpace(string(data))
	if content != "" {
		lines := strings.Split(content, "\n")
		fmt.Printf("   Epochs: %d\n", len(lines))
		
		// Show first few epochs
		for i, line := range lines {
			if i >= 3 {
				fmt.Printf("   ... (%d more)\n", len(lines)-i)
				break
			}
			fmt.Printf("   %s\n", strings.TrimSpace(line))
		}
	} else {
		fmt.Printf("   Status: Empty\n")
	}
}

func parseMetaProperties(path string) {
	fmt.Printf("   Type: Meta properties\n")
	
	data, err := os.ReadFile(path)
	if err != nil {
		fmt.Printf("   Error: %v\n", err)
		return
	}
	
	lines := strings.Split(string(data), "\n")
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line != "" && !strings.HasPrefix(line, "#") {
			fmt.Printf("   %s\n", line)
		}
	}
}

func parseCheckpointFile(path string) {
	fmt.Printf("   Type: Checkpoint file\n")
	
	data, err := os.ReadFile(path)
	if err != nil {
		fmt.Printf("   Error: %v\n", err)
		return
	}
	
	content := strings.TrimSpace(string(data))
	if content != "" {
		lines := strings.Split(content, "\n")
		fmt.Printf("   Lines: %d\n", len(lines))
		
		// Show first few lines
		for i, line := range lines {
			if i >= 5 {
				fmt.Printf("   ... (%d more)\n", len(lines)-i)
				break
			}
			fmt.Printf("   %s\n", strings.TrimSpace(line))
		}
	} else {
		fmt.Printf("   Status: Empty\n")
	}
}

func parseBootstrapCheckpoint(path string) {
	fmt.Printf("   Type: Bootstrap checkpoint (binary)\n")
	
	data, err := os.ReadFile(path)
	if err != nil {
		fmt.Printf("   Error: %v\n", err)
		return
	}
	
	fmt.Printf("   Binary size: %d bytes\n", len(data))
	
	// Look for readable strings that might indicate configuration keys
	readableStrings := extractReadableStrings(data)
	if len(readableStrings) > 0 {
		fmt.Printf("   Configuration keys found:\n")
		for i, str := range readableStrings {
			if i >= 5 { // Show max 5 strings
				fmt.Printf("     ... (%d more)\n", len(readableStrings)-i)
				break
			}
			fmt.Printf("     %s\n", str)
		}
	}
	
	// Look for timestamps
	timestamps := extractTimestamps(data)
	if len(timestamps) > 0 {
		fmt.Printf("   Timestamps found (%d):\n", len(timestamps))
		for i, ts := range timestamps {
			if i >= 3 {
				fmt.Printf("     ... (%d more)\n", len(timestamps)-i)
				break
			}
			fmt.Printf("     %s\n", dev.FormatTimestamp(ts))
		}
	}
}

func extractReadableStrings(data []byte) []string {
	var result []string
	var current []byte
	
	for _, b := range data {
		if b >= 32 && b <= 126 { // Printable ASCII
			current = append(current, b)
		} else {
			if len(current) >= 4 { // Minimum string length
				str := string(current)
				// Filter for config-like strings
				if strings.Contains(str, ".") || strings.Contains(str, "version") || 
				   strings.Contains(str, "group") || strings.Contains(str, "metadata") {
					result = append(result, str)
				}
			}
			current = nil
		}
	}
	
	// Check final string
	if len(current) >= 4 {
		str := string(current)
		if strings.Contains(str, ".") || strings.Contains(str, "version") {
			result = append(result, str)
		}
	}
	
	return result
}

func extractTimestamps(data []byte) []int64 {
	var timestamps []int64
	
	// Look for 8-byte sequences that could be timestamps
	for i := 0; i <= len(data)-8; i += 4 {
		timestamp := int64(binary.BigEndian.Uint64(data[i:i+8]))
		
		// Check if this looks like a timestamp (around 2025, in milliseconds)
		if timestamp > 1700000000000 && timestamp < 1800000000000 {
			// Avoid duplicates
			found := false
			for _, existing := range timestamps {
				if existing == timestamp {
					found = true
					break
				}
			}
			if !found {
				timestamps = append(timestamps, timestamp)
			}
		}
	}
	
	return timestamps
}