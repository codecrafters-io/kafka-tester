package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

const (
	LOGDIR_PATH   = "/tmp/kraft-combined-logs"
	LOG_FILE_NAME = "00000000000000000000.log"
)

// FindAndReadLogFile finds the topic directory and reads the log file, returns decoded batches
func FindAndReadLogFile(topicUUID []byte, partitionID uint32) ([]RecordBatch, error) {
	// Find the topic directory
	topicDir, err := findTopicDirectory()
	if err != nil {
		return nil, err
	}

	// Construct log file path
	logFilePath := filepath.Join(LOGDIR_PATH, topicDir, LOG_FILE_NAME)

	// Read log file contents
	contents, err := os.ReadFile(logFilePath)
	if err != nil {
		return nil, err
	}

	// Decode record batches
	batches, err := DecodeRecordBatches(contents)
	if err != nil {
		return nil, err
	}

	return batches, nil
}

// findTopicDirectory finds the only topic directory in /tmp/kraft-combined-logs
func findTopicDirectory() (string, error) {
	entries, err := os.ReadDir(LOGDIR_PATH)
	if err != nil {
		return "", err
	}

	for _, entry := range entries {
		if entry.IsDir() {
			// Skip cluster metadata directory
			if strings.HasPrefix(entry.Name(), "__cluster_metadata") {
				continue
			}

			// Check if it matches topicname-partitionnumber format
			if strings.Contains(entry.Name(), "-") && strings.HasSuffix(entry.Name(), "-0") {
				return entry.Name(), nil
			}
		}
	}

	return "", fmt.Errorf("no topic directory found")
}
