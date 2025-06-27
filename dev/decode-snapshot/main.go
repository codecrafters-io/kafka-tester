package main

import (
	"fmt"
	"os"

	"github.com/codecrafters-io/kafka-tester/dev"
	"github.com/codecrafters-io/tester-utils/logger"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "Usage: %s <snapshot-file>\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "Example: %s /tmp/kraft-combined-logs/__cluster_metadata-0/00000000000000000186.snapshot\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "Example: %s /tmp/kraft-combined-logs/quickstart-events-0/00000000000000000001.snapshot\n", os.Args[0])
		os.Exit(1)
	}

	filePath := os.Args[1]
	
	// Create a logger
	log := logger.GetQuietLogger("")
	
	fmt.Printf("Decoding snapshot file: %s\n\n", filePath)
	
	// Decode the file
	result, err := dev.DecodeSnapshotFile(filePath, log)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error decoding snapshot file: %v\n", err)
		os.Exit(1)
	}
	
	// Print summary
	result.PrintSnapshotSummary(log)
	
	// Show detailed breakdown
	fmt.Printf("\n=== Raw Header ===\n")
	fmt.Printf("Version: %d (0x%04x)\n", result.Header.Version, uint16(result.Header.Version))
	fmt.Printf("CRC: %d (0x%08x)\n", result.Header.CRC, uint32(result.Header.CRC))
	fmt.Printf("Last Offset: %d (0x%016x)\n", result.Header.LastOffset, uint64(result.Header.LastOffset))
	
	// Extract and show offset from filename
	offsetFromFile, err := dev.ExtractOffsetFromFilename(filePath)
	if err == nil {
		fmt.Printf("\n=== Filename Analysis ===\n")
		fmt.Printf("Snapshot Offset (from filename): %d\n", offsetFromFile)
		if offsetFromFile != result.Header.LastOffset {
			fmt.Printf("Note: Filename offset (%d) differs from header last offset (%d)\n", 
				offsetFromFile, result.Header.LastOffset)
		}
	}
	
	// Show snapshot type analysis
	fmt.Printf("\n=== Analysis ===\n")
	if result.IsKRaftMetadataSnapshot() {
		fmt.Printf("Type: Appears to be a KRaft metadata snapshot\n")
	} else {
		fmt.Printf("Type: Unknown snapshot type (version %d)\n", result.Header.Version)
	}
	
	fmt.Printf("Summary: %s\n", result.FormatSnapshotInfo())
	
	// Show full hex dump for small files
	if len(result.Data) <= 64 {
		fmt.Printf("\n=== Complete Data Hex Dump ===\n")
		for i := 0; i < len(result.Data); i += 16 {
			end := i + 16
			if end > len(result.Data) {
				end = len(result.Data)
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
			
			// Pad hex string to consistent width
			for len(hexStr) < 48 {
				hexStr += " "
			}
			
			fmt.Printf("%04x: %s |%s|\n", i, hexStr, asciiStr)
		}
	}
}