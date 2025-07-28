package serializer

import (
	_ "embed"
	"fmt"
	"os"

	"github.com/codecrafters-io/kafka-tester/protocol/common"
	"github.com/codecrafters-io/tester-utils/logger"
)

func GenerateLogDirs(logger *logger.Logger, topics []string) error {
	topicData := []common.TopicConfig{}
	for _, topicName := range topics {
		topicData = append(topicData, common.TOPICS[topicName])
	}

	return generateLogDirs(logger, len(topics) == 0, topicData)
}

// GenerateLogDirs generates the log directories and files for the test cases.
// If onlyClusterMetadata is true, only the cluster metadata will be generated.
// .log files & partition metadata files inside the topic directories will not be created.
func generateLogDirs(logger *logger.Logger, onlyClusterMetadata bool, topics []common.TopicConfig) error {
	// Topic1 -> Message1 (Partitions=[0])
	// Topic2 -> None (Partitions=[0])
	// Topic3 -> Message2, Message3 (Partitions=[0, 1])
	// Topic4 -> None (Partitions=[0, 1, 2])

	// meta
	nodeID := common.NODE_ID
	version := common.VERSION
	directoryID, _ := uuidToBase64(common.DIRECTORY_UUID)

	// cluster
	clusterID := common.CLUSTER_ID
	clusterMetadataTopicID := common.CLUSTER_METADATA_TOPIC_ID

	basePath := common.LOG_DIR

	err := os.RemoveAll(basePath)
	if err != nil {
		return fmt.Errorf("Could not remove log directory at %s: %w", basePath, err)
	}

	var topicMetadataDirectories []string
	for _, topic := range topics {
		for _, partition := range topic.Partitions {
			topicMetadataDirectories = append(topicMetadataDirectories, fmt.Sprintf("%s/%s-%d", basePath, topic.Name, partition.ID))
		}
	}
	clusterMetadataDirectory := fmt.Sprintf("%s/__cluster_metadata-0", basePath)

	kraftServerPropertiesPath := fmt.Sprintf(common.SERVER_PROPERTIES_FILE_PATH)
	kafkaCleanShutdownPath := fmt.Sprintf("%s/.kafka_cleanshutdown", basePath)
	metaPropertiesPath := fmt.Sprintf("%s/meta.properties", basePath)
	clusterPartitionMetadataPath := fmt.Sprintf("%s/partition.metadata", clusterMetadataDirectory)
	clusterMetadataDataFilePath := fmt.Sprintf("%s/00000000000000000000.log", clusterMetadataDirectory)

	err = generateDirectories(append(topicMetadataDirectories, clusterMetadataDirectory))
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

	err = writePartitionMetadata(clusterPartitionMetadataPath, 0, clusterMetadataTopicID, logger)
	if err != nil {
		return err
	}

	if !onlyClusterMetadata {
		err = GeneratePartitionMetadata(basePath, topics, logger)
		if err != nil {
			return err
		}
	}

	err = writeClusterMetadata(clusterMetadataDataFilePath, topics, logger)
	if err != nil {
		return err
	}

	if !onlyClusterMetadata {
		err = GenerateTopicLogData(basePath, topics, logger)
		if err != nil {
			return err
		}
	}

	logger.Infof("Finished writing log files to: %s", basePath)
	logger.ResetSecondaryPrefixes()

	return nil
}

func GenerateTopicLogData(basePath string, topics []common.TopicConfig, logger *logger.Logger) error {
	for _, topic := range topics {
		err := generateTopicLogData(basePath, topic, logger)
		if err != nil {
			return err
		}
	}
	return nil
}

func generateTopicLogData(basePath string, topic common.TopicConfig, logger *logger.Logger) error {
	for _, partition := range topic.Partitions {
		logFilePath := fmt.Sprintf("%s/%s-%d/00000000000000000000.log", basePath, topic.Name, partition.ID)
		err := writeTopicData(logFilePath, partition.Messages, logger)
		if err != nil {
			return err
		}
	}
	return nil
}

func GeneratePartitionMetadata(basePath string, topics []common.TopicConfig, logger *logger.Logger) error {
	for _, topic := range topics {
		err := generatePartitionMetadata(basePath, topic, logger)
		if err != nil {
			return err
		}
	}
	return nil
}

func generatePartitionMetadata(basePath string, topic common.TopicConfig, logger *logger.Logger) error {
	topicID, err := uuidToBase64(topic.UUID)
	if err != nil {
		panic("CodeCrafters Internal Error: Could not convert topic UUID: " + topic.UUID + " to base64: " + err.Error())
	}
	for _, partition := range topic.Partitions {
		topicMetadataPath := fmt.Sprintf("%s/%s-%d/partition.metadata", basePath, topic.Name, partition.ID)
		err := writePartitionMetadata(topicMetadataPath, 0, topicID, logger)
		if err != nil {
			return err
		}
	}
	return nil
}
