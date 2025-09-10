package kafka_files_handler

import (
	"encoding/base64"
	"fmt"

	"github.com/codecrafters-io/kafka-tester/protocol/encoder"
	"github.com/codecrafters-io/kafka-tester/protocol/kafka_interface"
	"github.com/google/uuid"
)

const (
	LOG_FILE_NAME                = "00000000000000000000.log"
	CLUSTER_METADATA_DIRECTORY   = "__cluster_metadata-0"
	PARTITION_METADATA_FILE_NAME = "partition.metadata"
	KRAFT_LOG_DIRECTORY          = "/tmp/kraft-combined-logs"
)

// uuidToBase64 converts a UUID string to base64 encoding
func uuidToBase64(uuidStr string) (string, error) {
	// Parse the UUID
	parsedUUID, err := uuid.Parse(uuidStr)
	if err != nil {
		return "", fmt.Errorf("invalid UUID format: %w", err)
	}

	// Convert to bytes and then to base64
	uuidBytes, err := parsedUUID.MarshalBinary()
	if err != nil {
		return "", fmt.Errorf("failed to marshal UUID to binary: %w", err)
	}

	return base64.StdEncoding.EncodeToString(uuidBytes), nil
}

func GetEncodedBytes(encodableObject kafka_interface.Encodable) []byte {
	encoder := encoder.NewEncoder()
	encodableObject.Encode(encoder)
	return encoder.Bytes()
}
