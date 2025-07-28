package builder

import (
	kafkaapi "github.com/codecrafters-io/kafka-tester/protocol/api"
)

type ClusterMetadataTopicRecord struct {
	topicName string
	topicUUID string
}

func NewClusterMetadataTopicRecordBuilder() *ClusterMetadataTopicRecord {
	return &ClusterMetadataTopicRecord{
		topicName: "",
		topicUUID: "",
	}
}

func (b *ClusterMetadataTopicRecord) WithTopicName(topicName string) *ClusterMetadataTopicRecord {
	b.topicName = topicName
	return b
}

func (b *ClusterMetadataTopicRecord) WithTopicUUID(topicUUID string) *ClusterMetadataTopicRecord {
	b.topicUUID = topicUUID
	return b
}

func (b *ClusterMetadataTopicRecord) Build() kafkaapi.ClusterMetadataTopicRecord {
	if b.topicName == "" {
		panic("CodeCrafters Internal Error: topicName is not set")
	}
	if b.topicUUID == "" {
		panic("CodeCrafters Internal Error: topicUUID is not set")
	}

	return kafkaapi.ClusterMetadataTopicRecord{
		TopicName: b.topicName,
		TopicUUID: b.topicUUID,
	}
}
