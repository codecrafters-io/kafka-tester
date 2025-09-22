package internal

import (
	"encoding/hex"
	"fmt"
	"testing"

	"github.com/codecrafters-io/kafka-tester/protocol/encoder"
	"github.com/codecrafters-io/kafka-tester/protocol/kafkaapi"
	"github.com/stretchr/testify/assert"
)

func TestEncodeFeatureLevelRecordPayload(t *testing.T) {
	featureLevelRecord := kafkaapi.ClusterMetadataPayload{
		FrameVersion: 1,
		Type:         12,
		Version:      0,
		Data: &kafkaapi.FeatureLevelRecord{
			Name:         "metadata.version",
			FeatureLevel: 20,
		},
	}

	encoder := encoder.NewEncoder()
	featureLevelRecord.Encode(encoder)

	encoderBytes := encoder.Bytes()
	fmt.Printf("%s\n", hex.Dump(encoderBytes))

	assert.Equal(t, "010c00116d657461646174612e76657273696f6e001400", hex.EncodeToString(encoderBytes))
}

func TestEncodeTopicRecordPayload(t *testing.T) {
	topicRecord := kafkaapi.ClusterMetadataPayload{
		FrameVersion: 1,
		Type:         2,
		Version:      0,
		Data: &kafkaapi.TopicRecord{
			TopicName: "foo",
			TopicUUID: "bfd99e5e-3235-4552-81f8-d4af1741970c"},
	}

	encoder := encoder.NewEncoder()
	topicRecord.Encode(encoder)

	encoderBytes := encoder.Bytes()

	fmt.Printf("%s\n", hex.Dump(encoderBytes))

	assert.Equal(t, "01020004666f6fbfd99e5e3235455281f8d4af1741970c00", hex.EncodeToString(encoderBytes))
}

func TestEncodePartitionRecordPayload(t *testing.T) {
	partitionRecord := kafkaapi.ClusterMetadataPayload{
		FrameVersion: 1,
		Type:         3,
		Version:      1,
		Data: &kafkaapi.PartitionRecord{
			PartitionId:      0,
			TopicUUID:        "bfd99e5e-3235-4552-81f8-d4af1741970c",
			Replicas:         []int32{1},
			ISReplicas:       []int32{1},
			RemovingReplicas: []int32{},
			AddingReplicas:   []int32{},
			Leader:           1,
			LeaderEpoch:      0,
			PartitionEpoch:   0,
			DirectoryUUIDs:   []string{"0224973c-badd-44cf-8744-45a99619da34"},
		},
	}

	encoder := encoder.NewEncoder()
	partitionRecord.Encode(encoder)
	encoderBytes := encoder.Bytes()

	fmt.Printf("%s\n", hex.Dump(encoderBytes))

	assert.Equal(t, "01030100000000bfd99e5e3235455281f8d4af1741970c020000000102000000010101000000010000000000000000020224973cbadd44cf874445a99619da3400", hex.EncodeToString(encoderBytes))
}

// ToDo: Add test for record
// ToDo: Add test for recordBatch
