package main

import (
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/codecrafters-io/kafka-tester/dev"
	"github.com/codecrafters-io/tester-utils/logger"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "Usage: %s <timeindex-file> [timestamp-ms]\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "Example: %s /tmp/kraft-combined-logs/quickstart-events-0/00000000000000000000.timeindex\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "Example: %s /tmp/kraft-combined-logs/quickstart-events-0/00000000000000000000.timeindex 1672531200000\n", os.Args[0])
		os.Exit(1)
	}

	filePath := os.Args[1]
	
	// Create a logger
	log := logger.GetQuietLogger("")
	
	fmt.Printf("Decoding time index file: %s\n\n", filePath)
	
	// Decode the file
	result, err := dev.DecodeTimeIndexFile(filePath, log)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error decoding time index file: %v\n", err)
		os.Exit(1)
	}
	
	// Print summary
	result.PrintTimeIndexSummary(log)
	
	// Show detailed breakdown
	fmt.Printf("\n=== Raw Time Index Entries ===\n")
	for i, entry := range result.Entries {
		timeStr := dev.FormatTimestamp(entry.Timestamp)
		fmt.Printf("Entry[%d]: timestamp=%d (0x%x) [%s], relative_offset=%d (0x%x)\n", 
			i, entry.Timestamp, entry.Timestamp, timeStr, 
			entry.RelativeOffset, entry.RelativeOffset)
	}
	
	// If a timestamp was provided, look it up
	if len(os.Args) >= 3 {
		targetTimestamp, err := strconv.ParseInt(os.Args[2], 10, 64)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Invalid timestamp: %s\n", os.Args[2])
			os.Exit(1)
		}
		
		offset := result.FindOffsetByTime(targetTimestamp)
		fmt.Printf("\n=== Timestamp Lookup ===\n")
		if offset >= 0 {
			fmt.Printf("Timestamp %d (%s) maps to relative offset %d\n", 
				targetTimestamp, dev.FormatTimestamp(targetTimestamp), offset)
		} else {
			fmt.Printf("Timestamp %d (%s) not found in index\n", 
				targetTimestamp, dev.FormatTimestamp(targetTimestamp))
		}
	}
	
	// Print range info
	if len(result.Entries) > 0 {
		minTime, maxTime, valid := result.GetTimeIndexRange()
		if valid {
			fmt.Printf("\n=== Time Index Range ===\n")
			fmt.Printf("Covers timestamps %d to %d\n", minTime, maxTime)
			fmt.Printf("Time range: %s to %s\n", 
				dev.FormatTimestamp(minTime), dev.FormatTimestamp(maxTime))
			
			duration := time.Duration(maxTime-minTime) * time.Millisecond
			fmt.Printf("Duration: %v\n", duration)
		}
	}
}