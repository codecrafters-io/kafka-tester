package builder

import (
	kafkaapi "github.com/codecrafters-io/kafka-tester/protocol/api"
	"github.com/codecrafters-io/kafka-tester/protocol/common"
)

type ClusterMetadataPartitionRecord struct {
	partitionID int32
	topicUUID   string
}

func NewClusterMetadataPartitionRecordBuilder() *ClusterMetadataPartitionRecord {
	return &ClusterMetadataPartitionRecord{
		partitionID: -1,
		topicUUID:   "",
	}
}

func (b *ClusterMetadataPartitionRecord) WithPartitionID(partitionID int32) *ClusterMetadataPartitionRecord {
	b.partitionID = partitionID
	return b
}

func (b *ClusterMetadataPartitionRecord) WithTopicUUID(topicUUID string) *ClusterMetadataPartitionRecord {
	b.topicUUID = topicUUID
	return b
}

func (b *ClusterMetadataPartitionRecord) Build() kafkaapi.ClusterMetadataPartitionRecord {
	directoryUUID := common.DIRECTORY_UUID
	nodeID := int32(common.NODE_ID)

	if b.partitionID == -1 {
		panic("CodeCrafters Internal Error: partitionID is not set")
	}

	if b.topicUUID == "" {
		panic("CodeCrafters Internal Error: topicUUID is not set")
	}

	return kafkaapi.ClusterMetadataPartitionRecord{
		PartitionID:      b.partitionID,
		TopicUUID:        b.topicUUID,
		Replicas:         []int32{nodeID},
		ISReplicas:       []int32{nodeID},
		RemovingReplicas: []int32{},
		AddingReplicas:   []int32{},
		Leader:           nodeID,
		LeaderEpoch:      0,
		PartitionEpoch:   0,
		Directories:      []string{directoryUUID},
	}
}
