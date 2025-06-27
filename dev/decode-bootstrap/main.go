package main

import (
	"fmt"
	"os"

	"github.com/codecrafters-io/kafka-tester/dev"
	"github.com/codecrafters-io/tester-utils/logger"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "Usage: %s <bootstrap-checkpoint-file>\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "Example: %s /tmp/kraft-combined-logs/bootstrap.checkpoint\n", os.Args[0])
		os.Exit(1)
	}

	filePath := os.Args[1]

	log := logger.GetQuietLogger("")

	fmt.Printf("Decoding bootstrap checkpoint file: %s\n\n", filePath)

	result, err := dev.DecodeBootstrapCheckpointFile(filePath, log)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error decoding bootstrap checkpoint file: %v\n", err)
		os.Exit(1)
	}

	result.PrintBootstrapCheckpointSummary(log)

	fileInfo, err := os.Stat(filePath)
	if err == nil {
		fmt.Printf("\n=== File Info ===\n")
		fmt.Printf("File size: %d bytes\n", fileInfo.Size())
		fmt.Printf("Entries parsed: %d\n", len(result.Entries))

		if len(result.Entries) > 0 {
			// Show timestamp range
			minTime := result.Entries[0].Timestamp
			maxTime := result.Entries[0].Timestamp

			for _, entry := range result.Entries {
				if entry.Timestamp > 1000000000000 { // Valid timestamp
					if entry.Timestamp < minTime || minTime < 1000000000000 {
						minTime = entry.Timestamp
					}
					if entry.Timestamp > maxTime {
						maxTime = entry.Timestamp
					}
				}
			}

			if minTime > 1000000000000 && maxTime > 1000000000000 {
				fmt.Printf("Time range: %s to %s\n",
					dev.FormatTimestamp(minTime), dev.FormatTimestamp(maxTime))
			}
		}
	}
}
