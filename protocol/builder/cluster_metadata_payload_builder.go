package builder

import (
	kafkaapi "github.com/codecrafters-io/kafka-tester/protocol/api"
)

type ClusterMetadataPayloadBuilder struct {
	payloadType int8
	version     int8
	data        kafkaapi.ClusterMetadataPayloadDataRecord
}

func NewClusterMetadataPayloadBuilder() *ClusterMetadataPayloadBuilder {
	return &ClusterMetadataPayloadBuilder{
		version:     -1,
		payloadType: -1,
	}
}

func (b *ClusterMetadataPayloadBuilder) WithPayloadType(payloadType int8) *ClusterMetadataPayloadBuilder {
	b.payloadType = payloadType
	return b
}

func (b *ClusterMetadataPayloadBuilder) WithVersion(version int8) *ClusterMetadataPayloadBuilder {
	b.version = version
	return b
}

func (b *ClusterMetadataPayloadBuilder) WithData(data kafkaapi.ClusterMetadataPayloadDataRecord) *ClusterMetadataPayloadBuilder {
	b.data = data
	return b
}

func (b *ClusterMetadataPayloadBuilder) WithTopicRecord(topicName string, topicUUID string) *ClusterMetadataPayloadBuilder {
	topicRecord := NewClusterMetadataTopicRecordBuilder().
		WithTopicName(topicName).
		WithTopicUUID(topicUUID).
		Build()

	return b.WithPayloadType(2).WithVersion(0).WithData(&topicRecord)
}

func (b *ClusterMetadataPayloadBuilder) WithPartitionRecord(partitionID int32, topicUUID string) *ClusterMetadataPayloadBuilder {
	partitionRecord := NewClusterMetadataPartitionRecordBuilder().
		WithPartitionID(partitionID).
		WithTopicUUID(topicUUID).
		Build()

	return b.WithPayloadType(3).WithVersion(1).WithData(&partitionRecord)
}

func (b *ClusterMetadataPayloadBuilder) WithFeatureLevelRecord(name string, featureLevel int16) *ClusterMetadataPayloadBuilder {
	featureLevelRecord := kafkaapi.FeatureLevelRecord{
		Name:         name,
		FeatureLevel: featureLevel,
	}
	return b.WithPayloadType(12).WithVersion(0).WithData(&featureLevelRecord)
}

func (b *ClusterMetadataPayloadBuilder) Build() kafkaapi.ClusterMetadataPayload {
	if b.payloadType == -1 {
		panic("CodeCrafters Internal Error: payloadType is not set")
	}
	if b.version == -1 {
		panic("CodeCrafters Internal Error: version is not set")
	}
	if b.data == nil {
		panic("CodeCrafters Internal Error: data is not set")
	}

	return kafkaapi.ClusterMetadataPayload{
		FrameVersion: 1,
		Type:         b.payloadType,
		Version:      b.version,
		Data:         b.data,
	}
}
