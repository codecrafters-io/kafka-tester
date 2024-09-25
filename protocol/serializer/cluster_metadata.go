package serializer

import (
	"fmt"
	"os"

	kafkaapi "github.com/codecrafters-io/kafka-tester/protocol/api"
	kafkaencoder "github.com/codecrafters-io/kafka-tester/protocol/encoder"
)

func writeClusterMetadata(path string, topic1Name string, topic1UUID string, topic2Name string, topic2UUID string, topic3Name string, topic3UUID string, directoryUUID string) {
	encoder := kafkaencoder.RealEncoder{}
	encoder.Init(make([]byte, 40960))

	featureLevelRecord := kafkaapi.ClusterMetadataPayload{
		FrameVersion: 1,
		Type:         12,
		Version:      0,
		Data: &kafkaapi.FeatureLevelRecord{
			Name:         "metadata.version",
			FeatureLevel: 20,
		},
	}

	topicRecord1 := kafkaapi.ClusterMetadataPayload{
		FrameVersion: 1,
		Type:         2,
		Version:      0,
		Data: &kafkaapi.TopicRecord{
			TopicName: topic1Name,
			TopicUUID: topic1UUID,
		},
	}

	partitionRecord1 := kafkaapi.ClusterMetadataPayload{
		FrameVersion: 1,
		Type:         3,
		Version:      1,
		Data: &kafkaapi.PartitionRecord{
			PartitionID:      0,
			TopicUUID:        topic1UUID,
			Replicas:         []int32{1},
			ISReplicas:       []int32{1},
			RemovingReplicas: []int32{},
			AddingReplicas:   []int32{},
			Leader:           1,
			LeaderEpoch:      0,
			PartitionEpoch:   0,
			Directories:      []string{directoryUUID},
		},
	}

	topicRecord2 := kafkaapi.ClusterMetadataPayload{
		FrameVersion: 1,
		Type:         2,
		Version:      0,
		Data: &kafkaapi.TopicRecord{
			TopicName: topic2Name,
			TopicUUID: topic2UUID,
		},
	}

	partitionRecord2 := kafkaapi.ClusterMetadataPayload{
		FrameVersion: 1,
		Type:         3,
		Version:      1,
		Data: &kafkaapi.PartitionRecord{
			PartitionID:      0,
			TopicUUID:        topic2UUID,
			Replicas:         []int32{1},
			ISReplicas:       []int32{1},
			RemovingReplicas: []int32{},
			AddingReplicas:   []int32{},
			Leader:           1,
			LeaderEpoch:      0,
			PartitionEpoch:   0,
			Directories:      []string{directoryUUID},
		},
	}

	topicRecord3 := kafkaapi.ClusterMetadataPayload{
		FrameVersion: 1,
		Type:         2,
		Version:      0,
		Data: &kafkaapi.TopicRecord{
			TopicName: topic3Name,
			TopicUUID: topic3UUID,
		},
	}

	partitionRecord3 := kafkaapi.ClusterMetadataPayload{
		FrameVersion: 1,
		Type:         3,
		Version:      1,
		Data: &kafkaapi.PartitionRecord{
			PartitionID:      1,
			TopicUUID:        topic3UUID,
			Replicas:         []int32{1},
			ISReplicas:       []int32{1},
			RemovingReplicas: []int32{},
			AddingReplicas:   []int32{},
			Leader:           1,
			LeaderEpoch:      0,
			PartitionEpoch:   0,
			Directories:      []string{directoryUUID},
		},
	}

	partitionRecord4 := kafkaapi.ClusterMetadataPayload{
		FrameVersion: 1,
		Type:         3,
		Version:      1,
		Data: &kafkaapi.PartitionRecord{
			PartitionID:      0,
			TopicUUID:        topic3UUID,
			Replicas:         []int32{1},
			ISReplicas:       []int32{1},
			RemovingReplicas: []int32{},
			AddingReplicas:   []int32{},
			Leader:           1,
			LeaderEpoch:      0,
			PartitionEpoch:   0,
			Directories:      []string{directoryUUID},
		},
	}

	recordBatch1 := kafkaapi.RecordBatch{
		BaseOffset:           1,
		PartitionLeaderEpoch: 1,
		Attributes:           0,
		LastOffsetDelta:      3,
		FirstTimestamp:       1726045943832,
		MaxTimestamp:         1726045943832,
		ProducerId:           -1,
		ProducerEpoch:        -1,
		BaseSequence:         -1,
		Records: []kafkaapi.Record{
			{
				Attributes:     0,
				TimestampDelta: 0,
				Key:            nil,
				Value:          GetEncodedBytes(featureLevelRecord),
				Headers:        []kafkaapi.RecordHeader{},
			},
		},
	}
	recordBatch2 := kafkaapi.RecordBatch{
		BaseOffset:           int64(len(recordBatch1.Records) + int(recordBatch1.BaseOffset)),
		PartitionLeaderEpoch: 1,
		Attributes:           0,
		LastOffsetDelta:      1,
		FirstTimestamp:       1726045957397,
		MaxTimestamp:         1726045957397,
		ProducerId:           -1,
		ProducerEpoch:        -1,
		BaseSequence:         -1,
		Records: []kafkaapi.Record{
			{
				Attributes:     0,
				TimestampDelta: 0,
				Key:            nil,
				Value:          GetEncodedBytes(topicRecord1),
				Headers:        []kafkaapi.RecordHeader{},
			},
			{
				Attributes:     0,
				TimestampDelta: 0,
				Key:            nil,
				Value:          GetEncodedBytes(partitionRecord1),
				Headers:        []kafkaapi.RecordHeader{},
			},
		},
	}

	recordBatch3 := kafkaapi.RecordBatch{
		BaseOffset:           int64(len(recordBatch2.Records) + int(recordBatch2.BaseOffset)),
		PartitionLeaderEpoch: 1,
		Attributes:           0,
		LastOffsetDelta:      1,
		FirstTimestamp:       1726045957397,
		MaxTimestamp:         1726045957397,
		ProducerId:           -1,
		ProducerEpoch:        -1,
		BaseSequence:         -1,
		Records: []kafkaapi.Record{
			{
				Attributes:     0,
				TimestampDelta: 0,
				Key:            nil,
				Value:          GetEncodedBytes(topicRecord2),
				Headers:        []kafkaapi.RecordHeader{},
			},
			{
				Attributes:     0,
				TimestampDelta: 0,
				Key:            nil,
				Value:          GetEncodedBytes(partitionRecord2),
				Headers:        []kafkaapi.RecordHeader{},
			},
		},
	}

	recordBatch4 := kafkaapi.RecordBatch{
		BaseOffset:           int64(len(recordBatch3.Records) + int(recordBatch3.BaseOffset)),
		PartitionLeaderEpoch: 1,
		Attributes:           0,
		LastOffsetDelta:      1,
		FirstTimestamp:       1726045957397,
		MaxTimestamp:         1726045957397,
		ProducerId:           -1,
		ProducerEpoch:        -1,
		BaseSequence:         -1,
		Records: []kafkaapi.Record{
			{
				Attributes:     0,
				TimestampDelta: 0,
				Key:            nil,
				Value:          GetEncodedBytes(topicRecord3),
				Headers:        []kafkaapi.RecordHeader{},
			},
			{
				Attributes:     0,
				TimestampDelta: 0,
				Key:            nil,
				Value:          GetEncodedBytes(partitionRecord3),
				Headers:        []kafkaapi.RecordHeader{},
			},
			{
				Attributes:     0,
				TimestampDelta: 0,
				Key:            nil,
				Value:          GetEncodedBytes(partitionRecord4),
				Headers:        []kafkaapi.RecordHeader{},
			},
		},
	}

	recordBatch1.Encode(&encoder)
	recordBatch2.Encode(&encoder)
	recordBatch3.Encode(&encoder)
	recordBatch4.Encode(&encoder)
	encodedBytes := encoder.Bytes()[:encoder.Offset()]

	err := os.WriteFile(path, encodedBytes, 0644)
	if err != nil {
		fmt.Printf("Error writing file: %v\n", err)
	}
	fmt.Printf("Successfully wrote cluster metadata file to: %s\n", path)
}
