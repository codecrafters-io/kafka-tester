package kafka_files_handler

import (
	"fmt"
	"os"
	"path"

	"github.com/codecrafters-io/kafka-tester/protocol/common"
	"github.com/codecrafters-io/kafka-tester/protocol/encoder"
	"github.com/codecrafters-io/kafka-tester/protocol/kafkaapi"
)

type PartitionMetadata struct {
	Version   int
	TopicName string
	TopicID   string
}

type PartitionGenerationConfig struct {
	PartitionID int
	Logs        []string
}

func (c *PartitionGenerationConfig) Generate(metadata PartitionMetadata) (kafkaapi.RecordBatches, error) {
	// Create directory first
	partitionDirPath := path.Join(
		common.LOG_DIR,
		fmt.Sprintf("%s-%d", metadata.TopicName, c.PartitionID),
	)

	if err := os.MkdirAll(partitionDirPath, 0755); err != nil {
		return nil, fmt.Errorf("error creating partition directory %s: %w", partitionDirPath, err)
	}

	// Write partition metadata file
	if err := c.writePartitionMetadata(metadata); err != nil {
		return nil, err
	}

	// Write actual log file which contains messages
	recordBatches, err := c.writeLogFile(metadata)

	if err != nil {
		return nil, err
	}

	return recordBatches, nil
}

func (c *PartitionGenerationConfig) writeLogFile(metadata PartitionMetadata) (kafkaapi.RecordBatches, error) {
	recordBatches := c.generateRecordBatchesFromLogs(c.Logs)

	logFilePath := path.Join(
		common.LOG_DIR,
		fmt.Sprintf("%s-%d", metadata.TopicName, c.PartitionID),
		LOG_FILE_NAME,
	)

	encoder := encoder.NewEncoder()
	recordBatches.Encode(encoder)

	if err := os.WriteFile(logFilePath, encoder.Bytes(), 0644); err != nil {
		return nil, fmt.Errorf("error writing file to %s: %w", logFilePath, err)
	}

	return recordBatches, nil
}

func (c *PartitionGenerationConfig) writePartitionMetadata(metadata PartitionMetadata) error {
	content := fmt.Sprintf("version: %d\ntopic_id: %s", metadata.Version, metadata.TopicID)

	filePath := path.Join(
		common.LOG_DIR,
		fmt.Sprintf("%s-%d", metadata.TopicName, c.PartitionID),
		"partition.metadata",
	)

	err := os.WriteFile(filePath, []byte(content), 0644)

	if err != nil {
		return fmt.Errorf("error writing partition metadata file: %w", err)
	}

	return nil
}

func (c *PartitionGenerationConfig) generateRecordBatchesFromLogs(logs []string) kafkaapi.RecordBatches {
	var recordBatches kafkaapi.RecordBatches

	for i, message := range logs {
		recordBatches = append(recordBatches, kafkaapi.RecordBatch{
			BaseOffset:           int64(i),
			PartitionLeaderEpoch: 0,
			Attributes:           0,
			LastOffsetDelta:      0,
			FirstTimestamp:       1726045973899,
			MaxTimestamp:         1726045973899,
			ProducerId:           0,
			ProducerEpoch:        0,
			BaseSequence:         0,
			Records: []kafkaapi.Record{
				{
					Attributes:     0,
					TimestampDelta: 0,
					Key:            nil,
					Value:          []byte(message),
					Headers:        []kafkaapi.RecordHeader{},
				},
			},
		})
	}

	return recordBatches
}
