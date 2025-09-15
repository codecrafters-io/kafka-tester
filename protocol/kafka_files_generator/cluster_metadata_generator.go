package kafka_files_generator

import (
	"fmt"
	"os"
	"path"

	"github.com/codecrafters-io/kafka-tester/protocol/encoder"
	"github.com/codecrafters-io/kafka-tester/protocol/kafkaapi"
	"github.com/codecrafters-io/kafka-tester/protocol/value"
)

type ClusterMetadataGenerator struct {
	generatedTopicsData []*GeneratedTopicData
}

func NewClusterMetadataGenerator(generatedTopicsData []*GeneratedTopicData) *ClusterMetadataGenerator {
	return &ClusterMetadataGenerator{
		generatedTopicsData: generatedTopicsData,
	}
}

func (g *ClusterMetadataGenerator) Generate() error {
	// Create __cluster_metadata-0 directory
	clusterMetadataDir := path.Join(KRAFT_LOG_DIRECTORY, CLUSTER_METADATA_DIRECTORY)
	if err := os.MkdirAll(clusterMetadataDir, 0755); err != nil {
		return fmt.Errorf("error creating cluster metadata directory %s: %w", clusterMetadataDir, err)
	}

	if err := g.writeLogFile(); err != nil {
		return err
	}

	return g.writePartitionMetadata()
}

func (g *ClusterMetadataGenerator) writeLogFile() error {
	encoder := encoder.NewEncoder()

	var recordBatches []kafkaapi.RecordBatch
	baseOffset := int64(0)

	featureLevelRecord := kafkaapi.ClusterMetadataPayload{
		FrameVersion: 1,
		Type:         12,
		Version:      0,
		Data: &kafkaapi.FeatureLevelRecord{
			Name:         "metadata.version",
			FeatureLevel: 20,
		},
	}

	recordBatch1 := kafkaapi.RecordBatch{
		BaseOffset:           value.Int64{Value: baseOffset},
		PartitionLeaderEpoch: value.Int32{Value: 1},
		Attributes:           value.Int16{Value: 0},
		LastOffsetDelta:      value.Int32{Value: 0}, // len(records) - 1
		FirstTimestamp:       value.Int64{Value: 1726045943832},
		MaxTimestamp:         value.Int64{Value: 1726045943832},
		ProducerId:           value.Int64{Value: -1},
		ProducerEpoch:        value.Int16{Value: -1},
		BaseSequence:         value.Int32{Value: -1},
		Records: []kafkaapi.Record{
			{
				Attributes:     value.Int8{Value: 0},
				TimestampDelta: value.Varint{Value: 0},
				Key:            nil,
				Value:          GetEncodedBytes(featureLevelRecord),
				Headers:        []kafkaapi.RecordHeader{},
			},
		},
	}
	recordBatches = append(recordBatches, recordBatch1)
	baseOffset += int64(len(recordBatch1.Records))

	// Process each topic and its partitions
	for _, topicData := range g.generatedTopicsData {
		// Create topic record
		topicRecord := kafkaapi.ClusterMetadataPayload{
			FrameVersion: 1,
			Type:         2,
			Version:      0,
			Data: &kafkaapi.TopicRecord{
				TopicName: topicData.Name,
				TopicUUID: topicData.UUID,
			},
		}

		// Collect all partition records for this topic
		var partitionRecords []kafkaapi.ClusterMetadataPayload
		for partitionID := range topicData.GeneratedRecordBatchesByPartition {
			partitionRecord := kafkaapi.ClusterMetadataPayload{
				FrameVersion: 1,
				Type:         3,
				Version:      1,
				Data: &kafkaapi.PartitionRecord{
					PartitionID:      int32(partitionID),
					TopicUUID:        topicData.UUID,
					Replicas:         []int32{1},
					ISReplicas:       []int32{1},
					RemovingReplicas: []int32{},
					AddingReplicas:   []int32{},
					Leader:           1,
					LeaderEpoch:      0,
					PartitionEpoch:   0,
					DirectoryUUIDs:   []string{DIRECTORY_UUID},
				},
			}
			partitionRecords = append(partitionRecords, partitionRecord)
		}

		// Create a record batch containing the topic record and all its partition records
		var records []kafkaapi.Record

		// Add topic record
		records = append(records, kafkaapi.Record{
			Attributes:     value.Int8{Value: 0},
			TimestampDelta: value.Varint{Value: 0},
			OffsetDelta:    value.Varint{Value: 0},
			Key:            nil,
			Value:          GetEncodedBytes(topicRecord),
			Headers:        []kafkaapi.RecordHeader{},
		})

		// Add all partition records
		for _, partitionRecord := range partitionRecords {
			records = append(records, kafkaapi.Record{
				Attributes:     value.Int8{Value: 0},
				TimestampDelta: value.Varint{Value: 0},
				OffsetDelta:    value.Varint{Value: 0},
				Key:            nil,
				Value:          GetEncodedBytes(partitionRecord),
				Headers:        []kafkaapi.RecordHeader{},
			})
		}

		recordBatch := kafkaapi.RecordBatch{
			BaseOffset:           value.Int64{Value: baseOffset},
			PartitionLeaderEpoch: value.Int32{Value: 1},
			Magic:                value.Int8{Value: 2},
			Attributes:           value.Int16{Value: 0},
			LastOffsetDelta:      value.Int32{Value: int32(len(records) - 1)},
			FirstTimestamp:       value.Int64{Value: 1726045957397},
			MaxTimestamp:         value.Int64{Value: 1726045957397},
			ProducerId:           value.Int64{Value: -1},
			ProducerEpoch:        value.Int16{Value: -1},
			BaseSequence:         value.Int32{Value: -1},
			Records:              records,
		}

		recordBatch.SetCRC()
		recordBatches = append(recordBatches, recordBatch)
		baseOffset += int64(len(records))
	}

	// Encode all record batches
	for _, recordBatch := range recordBatches {
		recordBatch.Encode(encoder)
	}

	encodedBytes := encoder.Bytes()

	// Write to __cluster_metadata-0/00000000000000000000.log
	logFilePath := path.Join(KRAFT_LOG_DIRECTORY, CLUSTER_METADATA_DIRECTORY, LOG_FILE_NAME)

	if err := os.WriteFile(logFilePath, encodedBytes, 0644); err != nil {
		return fmt.Errorf("error writing cluster metadata log file to %s: %w", logFilePath, err)
	}

	return nil
}

func (g *ClusterMetadataGenerator) writePartitionMetadata() error {
	content := fmt.Sprintf("version: %d\ntopic_id: %s", 0, CLUSTER_METADATA_TOPIC_ID)
	metadataFilePath := path.Join(KRAFT_LOG_DIRECTORY, CLUSTER_METADATA_DIRECTORY, PARTITION_METADATA_FILE_NAME)
	err := os.WriteFile(metadataFilePath, []byte(content), 0644)

	if err != nil {
		return fmt.Errorf("error writing cluster metadata partition metadata file: %w", err)
	}

	return nil
}
