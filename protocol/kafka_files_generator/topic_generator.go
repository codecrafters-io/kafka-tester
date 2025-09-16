package kafka_files_generator

import (
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
		GeneratedRecordBatchesByPartition: []GeneratedRecordBatchesByPartition{},
	}

	// generate logs by partition
	for _, partitionGenerationConfig := range c.PartitonGenerationConfigList {
		partitionId := partitionGenerationConfig.PartitionId

		recordBatches, err := partitionGenerationConfig.Generate(PartitionMetadata{
			Version:   0,
			TopicName: c.Name,
			TopicUUID: c.UUID,
		}, logger)

		if err != nil {
			return nil, err
		}

		generatedTopicData.GeneratedRecordBatchesByPartition = append(
			generatedTopicData.GeneratedRecordBatchesByPartition,
			GeneratedRecordBatchesByPartition{
				PartitionId:   partitionId,
				recordBatches: recordBatches,
			},
		)
	}

	return generatedTopicData, nil
}
