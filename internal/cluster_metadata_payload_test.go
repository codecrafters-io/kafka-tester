package internal

import (
	"encoding/hex"
	"fmt"
	"testing"

	kafkaapi "github.com/codecrafters-io/kafka-tester/protocol/api"
	"github.com/codecrafters-io/kafka-tester/protocol/serializer"
	"github.com/stretchr/testify/assert"
)

func TestDecodeBeginTransactionRecordPayload(t *testing.T) {
	hexdump := "01170001001212426f6f747374726170207265636f726473"

	b, err := hex.DecodeString(hexdump)
	if err != nil {
		panic(err)
	}

	beginTransactionRecord := kafkaapi.ClusterMetadataPayload{}
	err = beginTransactionRecord.Decode(b)

	assert.NoError(t, err)
	assert.EqualValues(t, 1, beginTransactionRecord.FrameVersion)
	assert.EqualValues(t, 23, beginTransactionRecord.Type)
	assert.EqualValues(t, 0, beginTransactionRecord.Version)

	payload, ok := beginTransactionRecord.Data.(*kafkaapi.BeginTransactionRecord)
	assert.True(t, ok)
	assert.EqualValues(t, "Bootstrap records", payload.Name)
}

func TestDecodeFeatureLevelRecordPayload(t *testing.T) {
	hexdump := "010c00116d657461646174612e76657273696f6e001400"

	b, err := hex.DecodeString(hexdump)
	if err != nil {
		panic(err)
	}

	featureLevelRecord := kafkaapi.ClusterMetadataPayload{}
	err = featureLevelRecord.Decode(b)

	assert.NoError(t, err)
	assert.EqualValues(t, 1, featureLevelRecord.FrameVersion)
	assert.EqualValues(t, 12, featureLevelRecord.Type)
	assert.EqualValues(t, 0, featureLevelRecord.Version)

	payload, ok := featureLevelRecord.Data.(*kafkaapi.FeatureLevelRecord)
	assert.True(t, ok)
	assert.EqualValues(t, "metadata.version", payload.Name)
}

func TestDecodeZKMigrationRecordPayload(t *testing.T) {
	hexdump := "0115000000"

	b, err := hex.DecodeString(hexdump)
	if err != nil {
		panic(err)
	}

	zkMigrationRecord := kafkaapi.ClusterMetadataPayload{}
	err = zkMigrationRecord.Decode(b)

	assert.NoError(t, err)
	assert.EqualValues(t, 1, zkMigrationRecord.FrameVersion)
	assert.EqualValues(t, 21, zkMigrationRecord.Type)
	assert.EqualValues(t, 0, zkMigrationRecord.Version)

	payload, ok := zkMigrationRecord.Data.(*kafkaapi.ZKMigrationStateRecord)
	assert.True(t, ok)
	assert.EqualValues(t, 0, payload.MigrationState)
}

func TestDecodeEndTransactionRecordPayload(t *testing.T) {
	hexdump := "01180000"

	b, err := hex.DecodeString(hexdump)
	if err != nil {
		panic(err)
	}

	endTransactionRecord := kafkaapi.ClusterMetadataPayload{}
	err = endTransactionRecord.Decode(b)

	assert.NoError(t, err)
	assert.EqualValues(t, 1, endTransactionRecord.FrameVersion)
	assert.EqualValues(t, 24, endTransactionRecord.Type)
	assert.EqualValues(t, 0, endTransactionRecord.Version)

	_, ok := endTransactionRecord.Data.(*kafkaapi.EndTransactionRecord)
	assert.True(t, ok)
}

func TestDecodeTopicRecordPayload(t *testing.T) {
	hexdump := "01020004666f6fbfd99e5e3235455281f8d4af1741970c00"

	b, err := hex.DecodeString(hexdump)
	if err != nil {
		panic(err)
	}

	topicRecord := kafkaapi.ClusterMetadataPayload{}
	err = topicRecord.Decode(b)

	assert.NoError(t, err)
	assert.EqualValues(t, 1, topicRecord.FrameVersion)
	assert.EqualValues(t, 2, topicRecord.Type)
	assert.EqualValues(t, 0, topicRecord.Version)

	payload, ok := topicRecord.Data.(*kafkaapi.TopicRecord)
	assert.True(t, ok)
	assert.EqualValues(t, "foo", payload.TopicName)
	assert.EqualValues(t, "bfd99e5e-3235-4552-81f8-d4af1741970c", payload.TopicUUID)
}

func TestDecodePartitionRecordPayload(t *testing.T) {
	hexdump := "01030100000000bfd99e5e3235455281f8d4af1741970c020000000102000000010101000000010000000000000000020224973cbadd44cf874445a99619da3400"

	b, err := hex.DecodeString(hexdump)
	if err != nil {
		panic(err)
	}

	partitionRecord := kafkaapi.ClusterMetadataPayload{}
	err = partitionRecord.Decode(b)

	assert.NoError(t, err)
	assert.EqualValues(t, 1, partitionRecord.FrameVersion)
	assert.EqualValues(t, 3, partitionRecord.Type)
	assert.EqualValues(t, 1, partitionRecord.Version)

	payload, ok := partitionRecord.Data.(*kafkaapi.PartitionRecord)
	assert.True(t, ok)
	assert.EqualValues(t, 0, payload.PartitionID)
	assert.EqualValues(t, "bfd99e5e-3235-4552-81f8-d4af1741970c", payload.TopicUUID)
	assert.EqualValues(t, []int32{1}, payload.Replicas)
	assert.EqualValues(t, []int32{1}, payload.ISReplicas)
	assert.EqualValues(t, []int32{}, payload.RemovingReplicas)
	assert.EqualValues(t, []int32{}, payload.AddingReplicas)
	assert.EqualValues(t, 1, payload.Leader)
	assert.EqualValues(t, 0, payload.LeaderEpoch)
	assert.EqualValues(t, 0, payload.PartitionEpoch)
	assert.EqualValues(t, []string{"0224973c-badd-44cf-8744-45a99619da34"}, payload.Directories)
}

func TestEncodeBeginTransactionRecordPayload(t *testing.T) {
	beginTransactionRecord := kafkaapi.ClusterMetadataPayload{
		FrameVersion: 1,
		Type:         23,
		Version:      0,
		Data: &kafkaapi.BeginTransactionRecord{
			Name: "Bootstrap records",
		},
	}

	bytes := serializer.GetEncodedBytes(beginTransactionRecord)
	fmt.Printf("%s\n", hex.Dump(bytes))

	assert.Equal(t, "01170001001212426f6f747374726170207265636f726473", hex.EncodeToString(bytes))
}

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

	bytes := serializer.GetEncodedBytes(featureLevelRecord)
	fmt.Printf("%s\n", hex.Dump(bytes))

	assert.Equal(t, "010c00116d657461646174612e76657273696f6e001400", hex.EncodeToString(bytes))
}

func TestEncodeZKMigrationRecordPayload(t *testing.T) {
	zkMigrationRecord := kafkaapi.ClusterMetadataPayload{
		FrameVersion: 1,
		Type:         21,
		Version:      0,
		Data:         &kafkaapi.ZKMigrationStateRecord{MigrationState: 0},
	}
	bytes := serializer.GetEncodedBytes(zkMigrationRecord)
	fmt.Printf("%s\n", hex.Dump(bytes))

	assert.Equal(t, "0115000000", hex.EncodeToString(bytes))
}

func TestEncodeEndTransactionRecordPayload(t *testing.T) {
	endTransactionRecord := kafkaapi.ClusterMetadataPayload{
		FrameVersion: 1,
		Type:         24,
		Version:      0,
		Data:         &kafkaapi.EndTransactionRecord{},
	}

	bytes := serializer.GetEncodedBytes(endTransactionRecord)
	fmt.Printf("%s\n", hex.Dump(bytes))

	assert.Equal(t, "01180000", hex.EncodeToString(bytes))
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
	bytes := serializer.GetEncodedBytes(topicRecord)
	fmt.Printf("%s\n", hex.Dump(bytes))

	assert.Equal(t, "01020004666f6fbfd99e5e3235455281f8d4af1741970c00", hex.EncodeToString(bytes))
}

func TestEncodePartitionRecordPayload(t *testing.T) {
	partitionRecord := kafkaapi.ClusterMetadataPayload{
		FrameVersion: 1,
		Type:         3,
		Version:      1,
		Data: &kafkaapi.PartitionRecord{
			PartitionID:      0,
			TopicUUID:        "bfd99e5e-3235-4552-81f8-d4af1741970c",
			Replicas:         []int32{1},
			ISReplicas:       []int32{1},
			RemovingReplicas: []int32{},
			AddingReplicas:   []int32{},
			Leader:           1,
			LeaderEpoch:      0,
			PartitionEpoch:   0,
			Directories:      []string{"0224973c-badd-44cf-8744-45a99619da34"},
		},
	}

	bytes := serializer.GetEncodedBytes(partitionRecord)
	fmt.Printf("%s\n", hex.Dump(bytes))

	assert.Equal(t, "01030100000000bfd99e5e3235455281f8d4af1741970c020000000102000000010101000000010000000000000000020224973cbadd44cf874445a99619da3400", hex.EncodeToString(bytes))
}

// ToDo: Add test for record
// ToDo: Add test for recordBatch
