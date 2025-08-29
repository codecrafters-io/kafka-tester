package serializer_legacy

import (
	"fmt"
	"os"

	"github.com/codecrafters-io/kafka-tester/protocol/common"
	"github.com/codecrafters-io/kafka-tester/protocol/encoder_legacy"
	"github.com/codecrafters-io/kafka-tester/protocol/kafkaapi_legacy"
	"github.com/codecrafters-io/tester-utils/logger"
)

func writeClusterMetadataBinSpec(path string, directoryUUID string, logger *logger.Logger) error {
	encoder := encoder_legacy.Encoder{}
	encoder.Init(make([]byte, 40960))

	topic3Name := "saz"
	topic3UUID := "00000000-0000-4000-8000-000000000091"

	featureLevelRecord := kafkaapi_legacy.ClusterMetadataPayload{
		FrameVersion: 1,
		Type:         12,
		Version:      0,
		Data: &kafkaapi_legacy.FeatureLevelRecord{
			Name:         "metadata.version",
			FeatureLevel: 20,
		},
	}

	topicRecord3 := kafkaapi_legacy.ClusterMetadataPayload{
		FrameVersion: 1,
		Type:         2,
		Version:      0,
		Data: &kafkaapi_legacy.TopicRecord{
			TopicName: topic3Name,
			TopicUUID: topic3UUID,
		},
	}

	partitionRecord3 := kafkaapi_legacy.ClusterMetadataPayload{
		FrameVersion: 1,
		Type:         3,
		Version:      1,
		Data: &kafkaapi_legacy.PartitionRecord{
			PartitionID:      0,
			TopicUUID:        topic3UUID,
			Replicas:         []int32{1},
			ISReplicas:       []int32{1},
			RemovingReplicas: []int32{},
			AddingReplicas:   []int32{},
			Leader:           1,
			LeaderEpoch:      0,
			PartitionEpoch:   0,
			Directories:      []string{directoryUUID},
		},
	}

	partitionRecord4 := kafkaapi_legacy.ClusterMetadataPayload{
		FrameVersion: 1,
		Type:         3,
		Version:      1,
		Data: &kafkaapi_legacy.PartitionRecord{
			PartitionID:      1,
			TopicUUID:        topic3UUID,
			Replicas:         []int32{1},
			ISReplicas:       []int32{1},
			RemovingReplicas: []int32{},
			AddingReplicas:   []int32{},
			Leader:           1,
			LeaderEpoch:      0,
			PartitionEpoch:   0,
			Directories:      []string{directoryUUID},
		},
	}

	recordBatch1 := kafkaapi_legacy.RecordBatch{
		BaseOffset:           0,
		PartitionLeaderEpoch: 1,
		Attributes:           0,
		LastOffsetDelta:      0, // len(records) - 1
		FirstTimestamp:       1726045943832,
		MaxTimestamp:         1726045943832,
		ProducerId:           -1,
		ProducerEpoch:        -1,
		BaseSequence:         -1,
		Records: []kafkaapi_legacy.Record{
			{
				Attributes:     0,
				TimestampDelta: 0,
				Key:            nil,
				Value:          GetEncodedBytes(featureLevelRecord),
				Headers:        []kafkaapi_legacy.RecordHeader{},
			},
		},
	}

	recordBatch4 := kafkaapi_legacy.RecordBatch{
		BaseOffset:           1,
		PartitionLeaderEpoch: 1,
		Attributes:           0,
		LastOffsetDelta:      2, // len(records) - 1
		FirstTimestamp:       1726045957397,
		MaxTimestamp:         1726045957397,
		ProducerId:           -1,
		ProducerEpoch:        -1,
		BaseSequence:         -1,
		Records: []kafkaapi_legacy.Record{
			{
				Attributes:     0,
				TimestampDelta: 0,
				Key:            nil,
				Value:          GetEncodedBytes(topicRecord3),
				Headers:        []kafkaapi_legacy.RecordHeader{},
			},
			{
				Attributes:     0,
				TimestampDelta: 0,
				Key:            nil,
				Value:          GetEncodedBytes(partitionRecord3),
				Headers:        []kafkaapi_legacy.RecordHeader{},
			},
			{
				Attributes:     0,
				TimestampDelta: 0,
				Key:            nil,
				Value:          GetEncodedBytes(partitionRecord4),
				Headers:        []kafkaapi_legacy.RecordHeader{},
			},
		},
	}

	recordBatch1.Encode(&encoder)
	recordBatch4.Encode(&encoder)
	encodedBytes := encoder.Bytes()[:encoder.Offset()]

	err := os.WriteFile(path, encodedBytes, 0644)
	if err != nil {
		return fmt.Errorf("error writing file to %s: %w", path, err)
	}

	logger.Debugf("  - Wrote file to: %s", path)
	return nil
}

func GenerateClusterMetadataBinSpec(logger *logger.Logger) error {
	directoryUUID := common.DIRECTORY_UUID

	basePath := common.LOG_DIR

	err := os.RemoveAll(basePath)
	if err != nil {
		return fmt.Errorf("could not remove log directory at %s: %w", basePath, err)
	}

	clusterMetadataDirectory := fmt.Sprintf("%s/__cluster_metadata-0", basePath)
	clusterMetadataDataFilePath := fmt.Sprintf("%s/00000000000000000000.log", clusterMetadataDirectory)

	err = generateDirectories([]string{clusterMetadataDirectory})
	if err != nil {
		return fmt.Errorf("could not generate directories: %w", err)
	}

	logger.UpdateLastSecondaryPrefix("Serializer")
	logger.Debugf("Writing log files to: %s", basePath)

	err = writeClusterMetadataBinSpec(clusterMetadataDataFilePath, directoryUUID, logger)
	if err != nil {
		return err
	}

	logger.Infof("Finished writing log files to: %s", basePath)
	logger.ResetSecondaryPrefixes()

	return nil
}
