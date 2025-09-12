package kafka_files_generator

import (
	"encoding/base64"
	"fmt"

	"github.com/codecrafters-io/kafka-tester/protocol/encoder"
	"github.com/codecrafters-io/kafka-tester/protocol/kafka_interface"
	"github.com/google/uuid"
)

const (
	LOG_FILE_NAME                  = "00000000000000000000.log"
	CLUSTER_METADATA_DIRECTORY     = "__cluster_metadata-0"
	PARTITION_METADATA_FILE_NAME   = "partition.metadata"
	KRAFT_LOG_DIRECTORY            = "/tmp/kraft-combined-logs"
	SERVER_PROPERTIES_FILE_PATH    = "/tmp/server.properties"
	KAFKA_CLEAN_SHUTDOWN_FILE_NAME = ".kafka_cleanshutdown"
	META_PROPERTIES_FILE_NAME      = "meta.properties"

	// TODO Future: Experiment with multiple values of these constants to see if they can be randomized
	DIRECTORY_UUID            = "10000000-0000-4000-8000-000000000001"
	CLUSTER_METADATA_TOPIC_ID = "AAAAAAAAAAAAAAAAAAAAAQ"
	CLUSTER_ID                = "IAAAAAAAQACAAAAAAAAAAQ"
	NODE_ID                   = 1
	META_VERSION              = 1
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
