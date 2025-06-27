package main

import (
	"fmt"
	"os"
	"strconv"

	"github.com/codecrafters-io/kafka-tester/dev"
	"github.com/codecrafters-io/tester-utils/logger"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "Usage: %s <index-file> [relative-offset]\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "Example: %s /tmp/kraft-combined-logs/quickstart-events-0/00000000000000000000.index\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "Example: %s /tmp/kraft-combined-logs/quickstart-events-0/00000000000000000000.index 5\n", os.Args[0])
		os.Exit(1)
	}

	filePath := os.Args[1]

	log := logger.GetQuietLogger("")

	fmt.Printf("Decoding index file: %s\n\n", filePath)

	result, err := dev.DecodeIndexFile(filePath, log)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error decoding index file: %v\n", err)
		os.Exit(1)
	}

	result.PrintSummary(log)

	fmt.Printf("\n=== Raw Index Entries ===\n")
	for i, entry := range result.Entries {
		fmt.Printf("Entry[%d]: relative_offset=%d (0x%x), position=%d (0x%x)\n",
			i, entry.RelativeOffset, entry.RelativeOffset, entry.Position, entry.Position)
	}

	if len(os.Args) >= 3 {
		targetOffset, err := strconv.ParseInt(os.Args[2], 10, 32)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Invalid relative offset: %s\n", os.Args[2])
			os.Exit(1)
		}

		position := result.FindPosition(int32(targetOffset))
		fmt.Printf("\n=== Offset Lookup ===\n")
		if position >= 0 {
			fmt.Printf("Relative offset %d maps to file position %d\n", targetOffset, position)
		} else {
			fmt.Printf("Relative offset %d not found in index\n", targetOffset)
		}
	}

	if len(result.Entries) > 0 {
		minOffset, maxOffset, valid := result.GetIndexRange()
		if valid {
			fmt.Printf("\n=== Index Range ===\n")
			fmt.Printf("Covers relative offsets %d to %d\n", minOffset, maxOffset)
		}
	}
}
