package kafka_files_handler

import "github.com/codecrafters-io/tester-utils/logger"

type LogDirectoryGenerationConfig struct {
	TopicGenerationConfigList []TopicGenerationConfig
}

func (c *LogDirectoryGenerationConfig) Generate(logger *logger.Logger) (*GeneratedLogDirectoryData, error) {

	var allGeneratedTopicsData []*GeneratedTopicData

	// generate logs by topics
	for _, topicGenerationConfig := range c.TopicGenerationConfigList {
		generatedTopicData, err := topicGenerationConfig.Generate(logger)

		if err != nil {
			return nil, err
		}

		allGeneratedTopicsData = append(allGeneratedTopicsData, generatedTopicData)
	}

	// generate cluster metadata as well
	clusterMetaDataGenerator := NewClusterMetadataGenerator(allGeneratedTopicsData)
	err := clusterMetaDataGenerator.Generate()

	if err != nil {
		return nil, err
	}

	return &GeneratedLogDirectoryData{
		GeneratedTopicsData: allGeneratedTopicsData,
	}, nil
}
