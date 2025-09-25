package internal

import (
	"encoding/base64"
	"fmt"
	"math"
	"strings"

	"github.com/codecrafters-io/kafka-tester/protocol/builder"
	"github.com/codecrafters-io/kafka-tester/protocol/kafka_files_generator"
	"github.com/codecrafters-io/tester-utils/random"
	"github.com/google/uuid"
)

func getInvalidAPIVersion() int16 {
	return int16(random.RandomInt(1000, 2000))
}

func getRandomCorrelationId() int32 {
	return int32(random.RandomInt(0, math.MaxInt32-1))
}

func getRandomTopicUUID() string {
	// Generate a deterministic UUID format with randomizable trailing digits
	// Uses format: 71a59a51-8968-4f8b-937e-xxxxxxxxxxxx where x are random hex digits
	// This ensures deterministic behavior for fixtures while still allowing randomization

	baseUUID := "71a59a51-8968-4f8b-937e-"
	urlUnsafeCharacters := []string{"+", "/"}

	for {
		// Generate 12 random hex digits for the last part
		randomSuffix := ""
		for range 12 {
			randomSuffix += fmt.Sprintf("%x", random.RandomInt(0, 15))
		}

		fullUUID := baseUUID + randomSuffix

		// Check if the UUID's base64 encoding contains unsafe characters
		uuidBytes, err := uuid.Parse(fullUUID)
		if err != nil {
			continue // Invalid UUID format, try again
		}

		base64Id := base64.StdEncoding.EncodeToString(uuidBytes[:])
		isURLSafe := true

		for _, char := range urlUnsafeCharacters {
			if strings.Contains(base64Id, char) {
				isURLSafe = false
				break
			}
		}

		if isURLSafe {
			return fullUUID
		}
	}
}

func getRandomTopicNames(count int) []string {
	return random.RandomWords(count)
}

func getRandomTopicUUIDs(count int) []string {
	// Generate deterministic UUIDs with incremental suffixes to ensure uniqueness
	// Uses format: 71a59a51-8968-4f8b-937e-xxxxxxxxxxxx where x are deterministic hex digits

	baseUUID := "71a59a51-8968-4f8b-937e-"
	urlUnsafeCharacters := []string{"+", "-", "/", "_"}
	uuids := make([]string, 0, count)

	for i := range count {
		for {
			// Generate deterministic suffix based on index + some randomness
			baseSuffix := fmt.Sprintf("%012x", i*1000+random.RandomInt(0, 999))
			fullUUID := baseUUID + baseSuffix

			// Check if the UUID's base64 encoding contains unsafe characters
			uuidBytes, err := uuid.Parse(fullUUID)
			if err != nil {
				continue // Invalid UUID format, try again with different random part
			}

			base64Id := base64.StdEncoding.EncodeToString(uuidBytes[:])
			isURLSafe := true

			for _, char := range urlUnsafeCharacters {
				if strings.Contains(base64Id, char) {
					isURLSafe = false
					break
				}
			}

			if isURLSafe {
				uuids = append(uuids, fullUUID)
				break
			}
		}
	}

	return uuids
}

func getEmptyTopicUUID() string {
	return "00000000-0000-0000-0000-000000000000"
}

// generateEmptyPartitionConfigs is used to prepare partitions (directories and metadata) for a given topic
func generateEmptyPartitionConfigs(numPartitions int) []kafka_files_generator.PartitionGenerationConfig {
	partitionConfigs := make([]kafka_files_generator.PartitionGenerationConfig, numPartitions)
	for i := range numPartitions {
		partitionConfigs[i] = kafka_files_generator.PartitionGenerationConfig{
			PartitionId: i,
		}
	}
	return partitionConfigs
}

// generatePartitionRequestWithRandomLogs is used to build partition information about log to be created in Produce request
func generatePartitionRequestWithRandomLogs(numPartitions int) []builder.ProduceRequestPartitionData {
	partitionCreationData := make([]builder.ProduceRequestPartitionData, numPartitions)
	for i := range numPartitions {
		partitionCreationData[i] = builder.ProduceRequestPartitionData{
			PartitionId: int32(i),
			Logs:        random.RandomWords(random.RandomInt(2, 4)),
		}
	}
	return partitionCreationData
}
