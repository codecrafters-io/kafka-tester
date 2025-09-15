package response_assertions

import (
	"fmt"
	"regexp"

	"github.com/codecrafters-io/kafka-tester/internal/field_decoder"
	int32_assertions "github.com/codecrafters-io/kafka-tester/internal/value_assertions/int32"
	"github.com/codecrafters-io/kafka-tester/protocol/kafka_files_generator"
	"github.com/codecrafters-io/kafka-tester/protocol/kafkaapi"
	"github.com/codecrafters-io/kafka-tester/protocol/utils"
	"github.com/codecrafters-io/tester-utils/logger"
)

type ExpectedPartition struct {
	PartititionId int32
	ErrorCode     int16
}
type ExpectedTopic struct {
	Name               string
	ErrorCode          int16
	UUID               string
	ExpectedPartitions []ExpectedPartition
}

type DescribeTopicPartitionsResponseAssertion struct {
	expectedCorrelationID int32
	expectedErrorCode     int16
	expectedTopics        []ExpectedTopic
}

func GetExpectedTopicsFromGeneratedLogDirectoryData(generatedLogDirectoryData *kafka_files_generator.GeneratedLogDirectoryData) []ExpectedTopic {
	var expectedTopics []ExpectedTopic

	for _, topicData := range generatedLogDirectoryData.GeneratedTopicsData {
		var expectedPartitions []ExpectedPartition

		for partitionID := range topicData.GeneratedRecordBatchesByPartition {
			expectedPartitions = append(expectedPartitions, ExpectedPartition{
				PartititionId: int32(partitionID),
				ErrorCode:     0,
			})
		}

		expectedTopics = append(expectedTopics, ExpectedTopic{
			Name:               topicData.Name,
			UUID:               topicData.UUID,
			ExpectedPartitions: expectedPartitions,
			ErrorCode:          0,
		})
	}

	return expectedTopics
}

func NewDescribeTopicPartitionsResponseAssertion() *DescribeTopicPartitionsResponseAssertion {
	return &DescribeTopicPartitionsResponseAssertion{}
}

func (a *DescribeTopicPartitionsResponseAssertion) ExpectCorrelationId(expectedCorrelationID int32) *DescribeTopicPartitionsResponseAssertion {
	a.expectedCorrelationID = expectedCorrelationID
	return a
}

func (a *DescribeTopicPartitionsResponseAssertion) ExpectTopics(expectedTopics []ExpectedTopic) *DescribeTopicPartitionsResponseAssertion {
	a.expectedTopics = expectedTopics
	return a
}

func (a *DescribeTopicPartitionsResponseAssertion) AssertSingleField(field field_decoder.DecodedField) error {
	path := field.Path.String()

	// Header fields
	if path == "DescribeTopicPartitionsResponse.Header.CorrelationID" {
		return int32_assertions.IsEqualTo(a.expectedCorrelationID, field.Value)
	}

	// Body level fields
	if path == "DescribeTopicPartitionsResponse.Body.ThrottleTimeMs" {
		return nil
	}

	if path == "DescribeTopicPartitionsResponse.Body.Topics.Length" {
		return nil
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

	topicIsInternalPattern := regexp.MustCompile(`^DescribeTopicPartitionsResponse\.Body\.Topics\.Topics\[\d+\]\.IsInternal$`)
	if topicIsInternalPattern.MatchString(path) {
		return nil
	}

	topicAuthorizedOpsPattern := regexp.MustCompile(`^DescribeTopicPartitionsResponse\.Body\.Topics\.Topics\[\d+\]\.TopicAuthorizedOperations$`)
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

	partitionLeaderIdPattern := regexp.MustCompile(`^DescribeTopicPartitionsResponse\.Body\.Topics\.Topics\[\d+\]\.Partitions\.Partitions\[\d+\]\.LeaderId$`)
	if partitionLeaderIdPattern.MatchString(path) {
		return nil
	}

	partitionLeaderEpochPattern := regexp.MustCompile(`^DescribeTopicPartitionsResponse\.Body\.Topics\.Topics\[\d+\]\.Partitions\.Partitions\[\d+\]\.LeaderEpoch$`)
	if partitionLeaderEpochPattern.MatchString(path) {
		return nil
	}

	// ReplicaNodes array fields
	replicaNodesLengthPattern := regexp.MustCompile(`^DescribeTopicPartitionsResponse\.Body\.Topics\.Topics\[\d+\]\.Partitions\.Partitions\[\d+\]\.ReplicaNodes\.Length$`)
	if replicaNodesLengthPattern.MatchString(path) {
		return nil
	}

	replicaNodePattern := regexp.MustCompile(`^DescribeTopicPartitionsResponse\.Body\.Topics\.Topics\[\d+\]\.Partitions\.Partitions\[\d+\]\.ReplicaNodes\.ReplicaNodes\[\d+\]\.ReplicaNode$`)
	if replicaNodePattern.MatchString(path) {
		return nil
	}

	// IsrNodes array fields
	isrNodesLengthPattern := regexp.MustCompile(`^DescribeTopicPartitionsResponse\.Body\.Topics\.Topics\[\d+\]\.Partitions\.Partitions\[\d+\]\.IsrNodes\.Length$`)
	if isrNodesLengthPattern.MatchString(path) {
		return nil
	}

	isrNodePattern := regexp.MustCompile(`^DescribeTopicPartitionsResponse\.Body\.Topics\.Topics\[\d+\]\.Partitions\.Partitions\[\d+\]\.IsrNodes\.IsrNodes\[\d+\]\.IsrNode$`)
	if isrNodePattern.MatchString(path) {
		return nil
	}

	// EligibleLeaderReplicas array fields
	eligibleLeaderReplicasLengthPattern := regexp.MustCompile(`^DescribeTopicPartitionsResponse\.Body\.Topics\.Topics\[\d+\]\.Partitions\.Partitions\[\d+\]\.EligibleLeaderReplicas\.Length$`)
	if eligibleLeaderReplicasLengthPattern.MatchString(path) {
		return nil
	}

	eligibleLeaderReplicaPattern := regexp.MustCompile(`^DescribeTopicPartitionsResponse\.Body\.Topics\.Topics\[\d+\]\.Partitions\.Partitions\[\d+\]\.EligibleLeaderReplicas\.EligibleLeaderReplicas\[\d+\]\.EligibleLeaderReplica$`)
	if eligibleLeaderReplicaPattern.MatchString(path) {
		return nil
	}

	// LastKnownELR array fields
	lastKnownELRLengthPattern := regexp.MustCompile(`^DescribeTopicPartitionsResponse\.Body\.Topics\.Topics\[\d+\]\.Partitions\.Partitions\[\d+\]\.LastKnownELR\.Length$`)
	if lastKnownELRLengthPattern.MatchString(path) {
		return nil
	}

	lastKnownELRNodePattern := regexp.MustCompile(`^DescribeTopicPartitionsResponse\.Body\.Topics\.Topics\[\d+\]\.Partitions\.Partitions\[\d+\]\.LastKnownELR\.LastKnownELR\[\d+\]\.LastKnownELRNode$`)
	if lastKnownELRNodePattern.MatchString(path) {
		return nil
	}

	// OfflineReplicas array fields
	offlineReplicasLengthPattern := regexp.MustCompile(`^DescribeTopicPartitionsResponse\.Body\.Topics\.Topics\[\d+\]\.Partitions\.Partitions\[\d+\]\.OfflineReplicas\.Length$`)
	if offlineReplicasLengthPattern.MatchString(path) {
		return nil
	}

	offlineReplicaPattern := regexp.MustCompile(`^DescribeTopicPartitionsResponse\.Body\.Topics\.Topics\[\d+\]\.Partitions\.Partitions\[\d+\]\.OfflineReplicas\.OfflineReplicas\[\d+\]\.OfflineReplica$`)
	if offlineReplicaPattern.MatchString(path) {
		return nil
	}

	// Cursor fields
	if path == "DescribeTopicPartitionsResponse.Body.Cursor.IsCursorPresent" {
		return nil
	}

	if path == "DescribeTopicPartitionsResponse.Body.Cursor.TopicName" {
		return nil
	}

	if path == "DescribeTopicPartitionsResponse.Body.Cursor.PartitionIndex" {
		return nil
	}

	return nil
}

func (a *DescribeTopicPartitionsResponseAssertion) AssertAcrossFields(response kafkaapi.DescribeTopicPartitionsResponse, logger *logger.Logger) error {
	// Log success messages from single-field assertions
	logger.Successf("✓ CorrelationID: %d", a.expectedCorrelationID)
	logger.Successf("✓ ErrorCode: %d (%s)", a.expectedErrorCode, utils.ErrorCodeToName(a.expectedErrorCode))

	if len(a.expectedTopics) != len(response.Body.Topics) {
		return fmt.Errorf("Expected Topics array length to be %d, got %d", len(a.expectedTopics), len(response.Body.Topics))
	}

	logger.Successf("✓ Topics array length: %d", len(response.Body.Topics))

	for i, expectedTopic := range a.expectedTopics {
		foundTopic := response.Body.Topics[i]

		// Assert topic names
		if expectedTopic.Name != foundTopic.Name.String() {
			return fmt.Errorf("Expected name of Topic[%d] to be %s, got %s", i, expectedTopic.Name, foundTopic.Name.String())
		}
		logger.Successf("✓ Topic[%d] name: %s", i, expectedTopic.Name)

		// Assert UUIDs
		if expectedTopic.UUID != foundTopic.TopicUUID.String() {
			return fmt.Errorf("Expected UUID of Topic[%d] to be %s, got %s", i, expectedTopic.UUID, foundTopic.TopicUUID.String())
		}
		logger.Successf("✓ Topic[%d] UUID: %s", i, expectedTopic.UUID)

		// Assert error code
		if expectedTopic.ErrorCode != foundTopic.ErrorCode.Value {
			return fmt.Errorf("Expected error code of Topic[%d] to be %d, got %d", i, expectedTopic.ErrorCode, foundTopic.ErrorCode.Value)
		}
		logger.Successf("✓ Topic[%d] error code: %d", i, expectedTopic.ErrorCode)

		// Check partitions length
		if len(expectedTopic.ExpectedPartitions) != len(foundTopic.Partitions) {
			return fmt.Errorf("Expected partitions array length for Topic[%d] to be %d, got %d", i, len(expectedTopic.ExpectedPartitions), len(foundTopic.Partitions))
		}
		logger.Successf("✓ Topic[%d] partitions array length: %d", i, len(foundTopic.Partitions))

		// For each partition check id and error code
		for j, expectedPartition := range expectedTopic.ExpectedPartitions {
			foundPartition := foundTopic.Partitions[j]

			// Check partition ID
			if expectedPartition.PartititionId != foundPartition.PartitionIndex.Value {
				return fmt.Errorf("Expected partition[%d] ID for Topic[%d] to be %d, got %d", j, i, expectedPartition.PartititionId, foundPartition.PartitionIndex.Value)
			}
			logger.Successf("✓ Topic[%d] Partition[%d] ID: %d", i, j, expectedPartition.PartititionId)

			// Check partition's error
			if expectedPartition.ErrorCode != foundPartition.ErrorCode.Value {
				return fmt.Errorf("Expected partition[%d] error code for Topic[%d] to be %d, got %d", j, i, expectedPartition.ErrorCode, foundPartition.ErrorCode.Value)
			}
			logger.Successf("✓ Topic[%d] Partition[%d] error code: %d (%s)", i, j, expectedPartition.ErrorCode, utils.ErrorCodeToName(expectedPartition.ErrorCode))
		}
	}

	return nil
}
