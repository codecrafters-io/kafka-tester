package serializer

import (
	_ "embed"
	"fmt"
	"os"

	"github.com/codecrafters-io/kafka-tester/protocol/common"
	"github.com/codecrafters-io/tester-utils/logger"
)

// GenerateLogDirs generates the log directories and files for the test cases.
// If onlyClusterMetadata is true, only the cluster metadata will be generated.
// .log files & partition metadata files inside the topic directories will not be created.
func GenerateLogDirs(logger *logger.Logger, onlyClusterMetadata bool) error {
	// Topic1 -> Message1 (Partitions=[0])
	// Topic2 -> None (Partitions=[0])
	// Topic3 -> Message2, Message3 (Partitions=[0, 1])
	// Topic4 -> None (Partitions=[0, 1, 2])

	// meta
	nodeID := common.NODE_ID
	version := common.VERSION
	directoryUUID := common.DIRECTORY_UUID
	directoryID, _ := uuidToBase64(common.DIRECTORY_UUID)

	// cluster
	clusterID := common.CLUSTER_ID
	clusterMetadataTopicID := common.CLUSTER_METADATA_TOPIC_ID

	// topics
	topic1Name := common.TOPIC1_NAME
	topic1Partition := 0
	topic1ID, _ := uuidToBase64(common.TOPIC1_UUID)
	topic1UUID := common.TOPIC1_UUID
	topic2Name := common.TOPIC2_NAME
	topic2Partition := 0
	topic2ID, _ := uuidToBase64(common.TOPIC2_UUID)
	topic2UUID := common.TOPIC2_UUID
	topic3Name := common.TOPIC3_NAME
	topic3Partition1 := 0
	topic3Partition2 := 1
	topic3ID, _ := uuidToBase64(common.TOPIC3_UUID)
	topic3UUID := common.TOPIC3_UUID
	topic4Name := common.TOPIC4_NAME
	topic4Partition1 := 0
	topic4Partition2 := 1
	topic4Partition3 := 2
	topic4ID, _ := uuidToBase64(common.TOPIC4_UUID)
	topic4UUID := common.TOPIC4_UUID

	basePath := common.LOG_DIR

	err := os.RemoveAll(basePath)
	if err != nil {
		return fmt.Errorf("could not remove log directory at %s: %w", basePath, err)
	}

	topic1MetadataDirectory := fmt.Sprintf("%s/%s-%d", basePath, topic1Name, topic1Partition)
	topic2MetadataDirectory := fmt.Sprintf("%s/%s-%d", basePath, topic2Name, topic2Partition)
	topic3Partition1MetadataDirectory := fmt.Sprintf("%s/%s-%d", basePath, topic3Name, topic3Partition1)
	topic3Partition2MetadataDirectory := fmt.Sprintf("%s/%s-%d", basePath, topic3Name, topic3Partition2)
	topic4Partition1MetadataDirectory := fmt.Sprintf("%s/%s-%d", basePath, topic4Name, topic4Partition1)
	topic4Partition2MetadataDirectory := fmt.Sprintf("%s/%s-%d", basePath, topic4Name, topic4Partition2)
	topic4Partition3MetadataDirectory := fmt.Sprintf("%s/%s-%d", basePath, topic4Name, topic4Partition3)
	clusterMetadataDirectory := fmt.Sprintf("%s/__cluster_metadata-0", basePath)

	kraftServerPropertiesPath := fmt.Sprintf(common.SERVER_PROPERTIES_FILE_PATH)
	kafkaCleanShutdownPath := fmt.Sprintf("%s/.kafka_cleanshutdown", basePath)
	metaPropertiesPath := fmt.Sprintf("%s/meta.properties", basePath)
	topic1MetadataPath := fmt.Sprintf("%s/partition.metadata", topic1MetadataDirectory)
	topic2MetadataPath := fmt.Sprintf("%s/partition.metadata", topic2MetadataDirectory)
	topic3Partition1MetadataPath := fmt.Sprintf("%s/partition.metadata", topic3Partition1MetadataDirectory)
	topic3Partition2MetadataPath := fmt.Sprintf("%s/partition.metadata", topic3Partition2MetadataDirectory)
	topic4Partition1MetadataPath := fmt.Sprintf("%s/partition.metadata", topic4Partition1MetadataDirectory)
	topic4Partition2MetadataPath := fmt.Sprintf("%s/partition.metadata", topic4Partition2MetadataDirectory)
	topic4Partition3MetadataPath := fmt.Sprintf("%s/partition.metadata", topic4Partition3MetadataDirectory)
	clusterMetadataMetadataPath := fmt.Sprintf("%s/partition.metadata", clusterMetadataDirectory)
	topic1DataFilePath := fmt.Sprintf("%s/00000000000000000000.log", topic1MetadataDirectory)
	topic2DataFilePath := fmt.Sprintf("%s/00000000000000000000.log", topic2MetadataDirectory)
	topic3Partition1DataFilePath := fmt.Sprintf("%s/00000000000000000000.log", topic3Partition1MetadataDirectory)
	topic3Partition2DataFilePath := fmt.Sprintf("%s/00000000000000000000.log", topic3Partition2MetadataDirectory)
	topic4Partition1DataFilePath := fmt.Sprintf("%s/00000000000000000000.log", topic4Partition1MetadataDirectory)
	topic4Partition2DataFilePath := fmt.Sprintf("%s/00000000000000000000.log", topic4Partition2MetadataDirectory)
	topic4Partition3DataFilePath := fmt.Sprintf("%s/00000000000000000000.log", topic4Partition3MetadataDirectory)
	clusterMetadataDataFilePath := fmt.Sprintf("%s/00000000000000000000.log", clusterMetadataDirectory)

	err = generateDirectories([]string{topic1MetadataDirectory, topic2MetadataDirectory, topic3Partition1MetadataDirectory, topic3Partition2MetadataDirectory, topic4Partition1MetadataDirectory, topic4Partition2MetadataDirectory, topic4Partition3MetadataDirectory, clusterMetadataDirectory})
	if err != nil {
		return fmt.Errorf("could not generate directories: %w", err)
	}

	logger.UpdateLastSecondaryPrefix("Serializer")
	logger.Debugf("Writing log files to: %s", basePath)

	err = writeKraftServerProperties(kraftServerPropertiesPath, logger)
	if err != nil {
		return err
	}

	err = writeMetaProperties(metaPropertiesPath, clusterID, directoryID, nodeID, version, logger)
	if err != nil {
		return err
	}

	err = writeKafkaCleanShutdown(kafkaCleanShutdownPath, logger)
	if err != nil {
		return err
	}

	if !onlyClusterMetadata {
		err = writePartitionMetadata(topic1MetadataPath, 0, topic1ID, logger)
		if err != nil {
			return err
		}

		err = writePartitionMetadata(topic2MetadataPath, 0, topic2ID, logger)
		if err != nil {
			return err
		}

		err = writePartitionMetadata(topic3Partition1MetadataPath, 0, topic3ID, logger)
		if err != nil {
			return err
		}

		err = writePartitionMetadata(topic3Partition2MetadataPath, 0, topic3ID, logger)
		if err != nil {
			return err
		}

		err = writePartitionMetadata(topic4Partition1MetadataPath, 0, topic4ID, logger)
		if err != nil {
			return err
		}

		err = writePartitionMetadata(topic4Partition2MetadataPath, 0, topic4ID, logger)
		if err != nil {
			return err
		}

		err = writePartitionMetadata(topic4Partition3MetadataPath, 0, topic4ID, logger)
		if err != nil {
			return err
		}
	}

	err = writePartitionMetadata(clusterMetadataMetadataPath, 0, clusterMetadataTopicID, logger)
	if err != nil {
		return err
	}

	if !onlyClusterMetadata {
		err = writeTopicData(topic1DataFilePath, []string{common.MESSAGE1}, logger)
		if err != nil {
			return err
		}

		err = writeTopicData(topic2DataFilePath, []string{}, logger)
		if err != nil {
			return err
		}

		err = writeTopicData(topic3Partition1DataFilePath, []string{common.MESSAGE2, common.MESSAGE3}, logger)
		if err != nil {
			return err
		}

		err = writeTopicData(topic3Partition2DataFilePath, []string{}, logger)
		if err != nil {
			return err
		}

		err = writeTopicData(topic4Partition1DataFilePath, []string{}, logger)
		if err != nil {
			return err
		}

		err = writeTopicData(topic4Partition2DataFilePath, []string{}, logger)
		if err != nil {
			return err
		}

		err = writeTopicData(topic4Partition3DataFilePath, []string{}, logger)
		if err != nil {
			return err
		}
	}

	err = writeClusterMetadata(clusterMetadataDataFilePath, topic1Name, topic1UUID, topic2Name, topic2UUID, topic3Name, topic3UUID, topic4Name, topic4UUID, directoryUUID, logger)
	if err != nil {
		return err
	}

	logger.Infof("Finished writing log files to: %s", basePath)
	logger.ResetSecondaryPrefixes()

	return nil
}
