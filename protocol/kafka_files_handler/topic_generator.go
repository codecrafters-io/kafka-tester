package kafka_files_handler

import "github.com/codecrafters-io/kafka-tester/protocol/kafkaapi"

type TopicGenerationConfig struct {
	Name                         string
	UUID                         string
	PartitonGenerationConfigList []PartitionGenerationConfig
}

type GeneratedTopicData struct {
	Name                              string
	UUID                              string
	generatedRecordBatchesByPartition map[int]kafkaapi.RecordBatches
}

func (c *TopicGenerationConfig) Generate() (*GeneratedTopicData, error) {

	generatedTopicData := &GeneratedTopicData{
		Name:                              c.Name,
		UUID:                              c.UUID,
		generatedRecordBatchesByPartition: make(map[int]kafkaapi.RecordBatches),
	}

	// generate logs by partition
	for _, partitionGenerationConfig := range c.PartitonGenerationConfigList {
		partitionID := partitionGenerationConfig.PartitionID

		recordBatches, err := partitionGenerationConfig.Generate(PartitionMetadata{
			Version:   0,
			TopicName: c.Name,
			TopicID:   c.UUID,
		})

		if err != nil {
			return nil, err
		}

		generatedTopicData.generatedRecordBatchesByPartition[partitionID] = recordBatches
	}

	return generatedTopicData, nil
}
