package main

import (
	"fmt"
	"os"

	"github.com/codecrafters-io/kafka-tester/dev"
	"github.com/codecrafters-io/tester-utils/logger"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "Usage: %s <topic-data-file>\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "Example: %s /tmp/kraft-combined-logs/quickstart-events-0/00000000000000000000.log\n", os.Args[0])
		os.Exit(1)
	}

	filePath := os.Args[1]
	
	log := logger.GetQuietLogger("")
	
	fmt.Printf("Decoding topic data file: %s\n\n", filePath)
	
	result, err := dev.DecodeTopicDataFile(filePath, log)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error decoding file: %v\n", err)
		os.Exit(1)
	}
	
	result.PrintSummary(log)
	
	fmt.Printf("\n=== Extracted Messages ===\n")
	for i, msg := range result.Messages {
		fmt.Printf("%d: %s\n", i, msg)
	}
}