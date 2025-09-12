package kafka_files_generator

import (
	"fmt"
	"os"
	"path"

	"github.com/codecrafters-io/kafka-tester/protocol/encoder"
	"github.com/codecrafters-io/kafka-tester/protocol/kafkaapi"
	"github.com/codecrafters-io/kafka-tester/protocol/value"
	"github.com/codecrafters-io/tester-utils/logger"
)

type PartitionMetadata struct {
	Version   int
	TopicName string
	TopicUUID string
}

type PartitionGenerationConfig struct {
	PartitionID int
	Logs        []string
}

func (c *PartitionGenerationConfig) Generate(metadata PartitionMetadata, logger *logger.Logger) (kafkaapi.RecordBatches, error) {
	// Create directory first
	partitionDirPath := path.Join(
		KRAFT_LOG_DIRECTORY,
		fmt.Sprintf("%s-%d", metadata.TopicName, c.PartitionID),
	)

	if err := os.MkdirAll(partitionDirPath, 0755); err != nil {
		return nil, fmt.Errorf("error creating partition directory %s: %w", partitionDirPath, err)
	}

	// Write partition metadata file
	if err := c.writePartitionMetadata(metadata, logger); err != nil {
		return nil, err
	}

	// Write actual log file which contains messages
	recordBatches, err := c.writeLogFile(metadata, logger)

	if err != nil {
		return nil, err
	}

	return recordBatches, nil
}

func (c *PartitionGenerationConfig) writeLogFile(metadata PartitionMetadata, logger *logger.Logger) (kafkaapi.RecordBatches, error) {
	recordBatches := c.generateRecordBatchesFromLogs(c.Logs)

	logFilePath := path.Join(
		KRAFT_LOG_DIRECTORY,
		fmt.Sprintf("%s-%d", metadata.TopicName, c.PartitionID),
		LOG_FILE_NAME,
	)

	encoder := encoder.NewEncoder()
	recordBatches.Encode(encoder)

	if err := os.WriteFile(logFilePath, encoder.Bytes(), 0644); err != nil {
		return nil, fmt.Errorf("error writing file to %s: %w", logFilePath, err)
	}

	logger.Debugf("Wrote metadata for partition %d of topic %s at %s", c.PartitionID, metadata.TopicName, logFilePath)
	return recordBatches, nil
}

func (c *PartitionGenerationConfig) writePartitionMetadata(metadata PartitionMetadata, logger *logger.Logger) error {
	topicIDBase64, err := uuidToBase64(metadata.TopicUUID)
	if err != nil {
		return err
	}
	content := fmt.Sprintf("version: %d\ntopic_id: %s", metadata.Version, topicIDBase64)

	filePath := path.Join(
		KRAFT_LOG_DIRECTORY,
		fmt.Sprintf("%s-%d", metadata.TopicName, c.PartitionID),
		"partition.metadata",
	)

	err = os.WriteFile(filePath, []byte(content), 0644)

	if err != nil {
		return fmt.Errorf("error writing partition metadata file: %w", err)
	}

	logger.Debugf("Wrote metadata for partition %d of topic %s at %s", c.PartitionID, metadata.TopicName, filePath)
	return nil
}

func (c *PartitionGenerationConfig) generateRecordBatchesFromLogs(logs []string) kafkaapi.RecordBatches {
	recordBatches := kafkaapi.RecordBatches{}

	for i, message := range logs {
		recordBatches = append(recordBatches, kafkaapi.RecordBatch{
			BaseOffset:           value.Int64{Value: int64(i)},
			PartitionLeaderEpoch: value.Int32{Value: 0},
			Attributes:           value.Int16{Value: 0},
			LastOffsetDelta:      value.Int32{Value: 0},
			FirstTimestamp:       value.Int64{Value: 1726045973899},
			MaxTimestamp:         value.Int64{Value: 1726045973899},
			ProducerId:           value.Int64{Value: 0},
			ProducerEpoch:        value.Int16{Value: 0},
			BaseSequence:         value.Int32{Value: 0},
			Records: []kafkaapi.Record{
				{
					Attributes:     value.Int8{Value: 0},
					TimestampDelta: value.Int64{Value: 0},
					Key:            value.RawBytes{},
					Value:          value.RawBytes{Value: []byte(message)},
					Headers:        []kafkaapi.RecordHeader{},
				},
			},
		})
	}

	return recordBatches
}
