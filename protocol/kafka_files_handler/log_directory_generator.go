package kafka_files_handler

type LogDirectoryGenerationConfig struct {
	TopicGenerationConfigList []TopicGenerationConfig
}

type LogDirectoryGenerationData struct {
	TopicsGenerationData []*GeneratedTopicData
}

func (c *LogDirectoryGenerationConfig) Generate() (*LogDirectoryGenerationData, error) {

	var allGeneratedTopicsData []*GeneratedTopicData

	// generate logs by topics
	for _, topicGenerationConfig := range c.TopicGenerationConfigList {
		generatedTopicData, err := topicGenerationConfig.Generate()

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

	return &LogDirectoryGenerationData{
		TopicsGenerationData: allGeneratedTopicsData,
	}, nil
}
