package kafka_files_generator

import (
	"github.com/codecrafters-io/kafka-tester/protocol/kafkaapi"
	"github.com/codecrafters-io/tester-utils/logger"
)

type TopicGenerationConfig struct {
	Name                         string
	UUID                         string
	PartitonGenerationConfigList []PartitionGenerationConfig
}

func (c *TopicGenerationConfig) Generate(logger *logger.Logger) (*GeneratedTopicData, error) {

	generatedTopicData := &GeneratedTopicData{
		Name:                              c.Name,
		UUID:                              c.UUID,
		GeneratedRecordBatchesByPartition: make(map[int]kafkaapi.RecordBatches),
	}

	// generate logs by partition
	for _, partitionGenerationConfig := range c.PartitonGenerationConfigList {
		partitionID := partitionGenerationConfig.PartitionID

		recordBatches, err := partitionGenerationConfig.Generate(PartitionMetadata{
			Version:   0,
			TopicName: c.Name,
			TopicUUID: c.UUID,
		}, logger)

		if err != nil {
			return nil, err
		}

		generatedTopicData.GeneratedRecordBatchesByPartition[partitionID] = recordBatches
	}

	return generatedTopicData, nil
}
