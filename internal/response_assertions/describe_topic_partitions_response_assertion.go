package response_assertions

import (
	"fmt"
	"regexp"
	"sort"

	"github.com/codecrafters-io/kafka-tester/internal/field_decoder"
	compact_array_length_assertions "github.com/codecrafters-io/kafka-tester/internal/value_assertions/compact_array_length"
	int32_assertions "github.com/codecrafters-io/kafka-tester/internal/value_assertions/int32"
	int8_assertions "github.com/codecrafters-io/kafka-tester/internal/value_assertions/int8"
	"github.com/codecrafters-io/kafka-tester/protocol/kafka_files_generator"
	"github.com/codecrafters-io/kafka-tester/protocol/kafkaapi"
	"github.com/codecrafters-io/kafka-tester/protocol/utils"
	"github.com/codecrafters-io/kafka-tester/protocol/value"
	"github.com/codecrafters-io/tester-utils/logger"
)

type ExpectedPartition struct {
	ErrorCode   int16
	PartitionId int32
}
type ExpectedTopic struct {
	Name               string
	ErrorCode          int16
	UUID               string
	ExpectedPartitions []ExpectedPartition
}

type DescribeTopicPartitionsResponseAssertion struct {
	expectedCorrelationId  int32
	expectedCursorPresence int8
	expectedTopics         []ExpectedTopic
}

func GetExpectedTopicsFromGeneratedLogDirectoryData(generatedLogDirectoryData *kafka_files_generator.GeneratedLogDirectoryData) []ExpectedTopic {
	var expectedTopics []ExpectedTopic

	for _, topicData := range generatedLogDirectoryData.GeneratedTopicsData {
		var expectedPartitions []ExpectedPartition

		for _, generatedRecordBatchesByPartition := range topicData.GeneratedRecordBatchesByPartition {
			expectedPartitions = append(expectedPartitions, ExpectedPartition{
				PartitionId: int32(generatedRecordBatchesByPartition.PartitionId),
				ErrorCode:   0,
			})
		}

		expectedTopics = append(expectedTopics, ExpectedTopic{
			Name:               topicData.Name,
			UUID:               topicData.UUID,
			ExpectedPartitions: expectedPartitions,
			ErrorCode:          0,
		})
	}

	// sort expected topics by in alphabetical order
	sort.Slice(expectedTopics, func(i, j int) bool {
		return expectedTopics[i].Name < expectedTopics[j].Name
	})

	return expectedTopics
}

func NewDescribeTopicPartitionsResponseAssertion() *DescribeTopicPartitionsResponseAssertion {
	return &DescribeTopicPartitionsResponseAssertion{}
}

func (a *DescribeTopicPartitionsResponseAssertion) ExpectCorrelationId(expectedCorrelationID int32) *DescribeTopicPartitionsResponseAssertion {
	a.expectedCorrelationId = expectedCorrelationID
	return a
}

func (a *DescribeTopicPartitionsResponseAssertion) ExpectTopics(expectedTopics []ExpectedTopic) *DescribeTopicPartitionsResponseAssertion {
	a.expectedTopics = expectedTopics
	return a
}

func (a *DescribeTopicPartitionsResponseAssertion) ExpectCursorAbsence() *DescribeTopicPartitionsResponseAssertion {
	a.expectedCursorPresence = -1
	return a
}

func (a *DescribeTopicPartitionsResponseAssertion) AssertSingleField(field field_decoder.DecodedField) error {
	path := field.Path.String()

	// Header fields
	if path == "DescribeTopicPartitionsResponse.Header.CorrelationID" {
		return int32_assertions.IsEqualTo(a.expectedCorrelationId, field.Value)
	}

	// Body level fields
	if path == "DescribeTopicPartitionsResponse.Body.ThrottleTimeMs" {
		return nil
	}

	if path == "DescribeTopicPartitionsResponse.Body.Topics.Length" {
		return compact_array_length_assertions.IsEqualTo(
			value.NewCompactArrayLength(a.expectedTopics),
			field.Value,
		)
	}

	// Topic level fields (using regex for array indices)
	topicErrorCodePattern := regexp.MustCompile(`^DescribeTopicPartitionsResponse\.Body\.Topics\.Topics\[\d+\]\.ErrorCode$`)
	if topicErrorCodePattern.MatchString(path) {
		return nil
	}

	topicNamePattern := regexp.MustCompile(`^DescribeTopicPartitionsResponse\.Body\.Topics\.Topics\[\d+\]\.Name$`)
	if topicNamePattern.MatchString(path) {
		return nil
	}

	topicUUIDPattern := regexp.MustCompile(`^DescribeTopicPartitionsResponse\.Body\.Topics\.Topics\[\d+\]\.UUID$`)
	if topicUUIDPattern.MatchString(path) {
		return nil
	}

	topicIsInternalPattern := regexp.MustCompile(`^DescribeTopicPartitionsResponse\.Body\.Topics\.Topics\[\d+\]\.IsInternal.*$`)
	if topicIsInternalPattern.MatchString(path) {
		return nil
	}

	topicAuthorizedOpsPattern := regexp.MustCompile(`^DescribeTopicPartitionsResponse\.Body\.Topics\.Topics\[\d+\]\.TopicAuthorizedOperations.*$`)
	if topicAuthorizedOpsPattern.MatchString(path) {
		return nil
	}

	// Partitions array length
	partitionsLengthPattern := regexp.MustCompile(`^DescribeTopicPartitionsResponse\.Body\.Topics\.Topics\[\d+\]\.Partitions\.Length$`)
	if partitionsLengthPattern.MatchString(path) {
		return nil
	}

	// Partition level fields (using regex for array indices)
	partitionErrorCodePattern := regexp.MustCompile(`^DescribeTopicPartitionsResponse\.Body\.Topics\.Topics\[\d+\]\.Partitions\.Partitions\[\d+\]\.ErrorCode$`)
	if partitionErrorCodePattern.MatchString(path) {
		return nil
	}

	partitionIndexPattern := regexp.MustCompile(`^DescribeTopicPartitionsResponse\.Body\.Topics\.Topics\[\d+\]\.Partitions\.Partitions\[\d+\]\.PartitionIndex$`)
	if partitionIndexPattern.MatchString(path) {
		return nil
	}

	partitionLeaderIdPattern := regexp.MustCompile(`^DescribeTopicPartitionsResponse\.Body\.Topics\.Topics\[\d+\]\.Partitions\.Partitions\[\d+\]\.LeaderId.*$`)
	if partitionLeaderIdPattern.MatchString(path) {
		return nil
	}

	partitionLeaderEpochPattern := regexp.MustCompile(`^DescribeTopicPartitionsResponse\.Body\.Topics\.Topics\[\d+\]\.Partitions\.Partitions\[\d+\]\.LeaderEpoch.*$`)
	if partitionLeaderEpochPattern.MatchString(path) {
		return nil
	}

	// ReplicaNodes array fields
	replicaNodesPattern := regexp.MustCompile(`^DescribeTopicPartitionsResponse\.Body\.Topics\.Topics\[\d+\]\.Partitions\.Partitions\[\d+\]\.ReplicaNodes.*$`)
	if replicaNodesPattern.MatchString(path) {
		return nil
	}

	// IsrNodes array fields
	isrNodesPattern := regexp.MustCompile(`^DescribeTopicPartitionsResponse\.Body\.Topics\.Topics\[\d+\]\.Partitions\.Partitions\[\d+\]\.IsrNodes.*$`)
	if isrNodesPattern.MatchString(path) {
		return nil
	}

	// EligibleLeaderReplicas array fields
	eligibleLeaderReplicasPattern := regexp.MustCompile(`^DescribeTopicPartitionsResponse\.Body\.Topics\.Topics\[\d+\]\.Partitions\.Partitions\[\d+\]\.EligibleLeaderReplicas.*$`)
	if eligibleLeaderReplicasPattern.MatchString(path) {
		return nil
	}

	// LastKnownELR array fields
	lastKnownELRPattern := regexp.MustCompile(`^DescribeTopicPartitionsResponse\.Body\.Topics\.Topics\[\d+\]\.Partitions\.Partitions\[\d+\]\.LastKnownELR.*$`)
	if lastKnownELRPattern.MatchString(path) {
		return nil
	}

	// OfflineReplicas array fields
	offlineReplicasPattern := regexp.MustCompile(`^DescribeTopicPartitionsResponse\.Body\.Topics\.Topics\[\d+\]\.Partitions\.Partitions\[\d+\]\.OfflineReplicas.*$`)
	if offlineReplicasPattern.MatchString(path) {
		return nil
	}

	// Cursor fields
	if path == "DescribeTopicPartitionsResponse.Body.Cursor.IsCursorPresent" {
		return int8_assertions.IsEqualTo(a.expectedCursorPresence, field.Value)
	}

	panic("CodeCrafters Internal Error: Unhandled field path: " + field.Path.String())
}

func (a *DescribeTopicPartitionsResponseAssertion) AssertAcrossFields(response kafkaapi.DescribeTopicPartitionsResponse, logger *logger.Logger) error {
	// Log success messages from single-field assertions
	logger.Successf("✓ CorrelationID: %d", a.expectedCorrelationId)
	logger.Successf("✓ Topics array length: %d", len(response.Body.Topics))
	logger.Successf("✓ Cursor.IsCursorPresent: %d", a.expectedCursorPresence)

	for i, expectedTopic := range a.expectedTopics {
		foundTopic := response.Body.Topics[i]

		// Assert topic names
		if expectedTopic.Name != foundTopic.Name.String() {
			return fmt.Errorf("Expected name of Topic[%d] to be %s, got %s", i, expectedTopic.Name, foundTopic.Name.String())
		}
		logger.Successf("✓ Topic[%d].Name: %s", i, expectedTopic.Name)

		// Assert UUIDs
		if expectedTopic.UUID != foundTopic.TopicUUID.String() {
			return fmt.Errorf("Expected UUID of Topic[%d] to be %s, got %s", i, expectedTopic.UUID, foundTopic.TopicUUID.String())
		}
		logger.Successf("✓ Topic[%d].UUID: %s", i, expectedTopic.UUID)

		// Assert error code
		if expectedTopic.ErrorCode != foundTopic.ErrorCode.Value {
			return fmt.Errorf("Expected error code of Topic[%d] to be %d, got %d", i, expectedTopic.ErrorCode, foundTopic.ErrorCode.Value)
		}
		logger.Successf("✓ Topic[%d].ErrorCode: %d", i, expectedTopic.ErrorCode)

		// Check partitions length
		if len(expectedTopic.ExpectedPartitions) != len(foundTopic.Partitions) {
			return fmt.Errorf("Expected partitions array length for Topic[%d] to be %d, got %d", i, len(expectedTopic.ExpectedPartitions), len(foundTopic.Partitions))
		}
		logger.Successf("✓ Topic[%d].Partitions.Length: %d", i, len(foundTopic.Partitions))

		// For each partition check id and error code
		for j, expectedPartition := range expectedTopic.ExpectedPartitions {
			foundPartition := foundTopic.Partitions[j]

			// Check partition ID
			if expectedPartition.PartitionId != foundPartition.PartitionIndex.Value {
				return fmt.Errorf("Expected partition[%d] id for Topic[%d] to be %d, got %d", j, i, expectedPartition.PartitionId, foundPartition.PartitionIndex.Value)
			}
			logger.Successf("✓ Topic[%d].Partition[%d].ID: %d", i, j, expectedPartition.PartitionId)

			// Check partition's error
			if expectedPartition.ErrorCode != foundPartition.ErrorCode.Value {
				return fmt.Errorf("Expected error code for partition[%d] of Topic[%d] to be %d, got %d", j, i, expectedPartition.ErrorCode, foundPartition.ErrorCode.Value)
			}
			logger.Successf("✓ Topic[%d].Partition[%d].ErrorCode: %d (%s)", i, j, expectedPartition.ErrorCode, utils.ErrorCodeToName(expectedPartition.ErrorCode))
		}
	}

	return nil
}
