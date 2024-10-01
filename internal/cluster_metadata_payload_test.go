package internal

import (
	"encoding/hex"
	"fmt"
	"testing"

	kafkaapi "github.com/codecrafters-io/kafka-tester/protocol/api"
	"github.com/codecrafters-io/kafka-tester/protocol/decoder"
	"github.com/codecrafters-io/kafka-tester/protocol/encoder"
	"github.com/codecrafters-io/kafka-tester/protocol/serializer"
	"github.com/stretchr/testify/assert"
)

func TestBeginTxnRecord(t *testing.T) {
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

func TestFeatureLevelRecord(t *testing.T) {
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

func TestZKMigrationRecord(t *testing.T) {
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

func TestEndTxnRecord(t *testing.T) {
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

func TestTopicRecord(t *testing.T) {
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

func TestPartitionRecord(t *testing.T) {
	hexdump := "01030100000000bfd99e5e3235455281f8d4af1741970c020000000102000000010101000000010000000000000000020224973cbadd44cf874445a99619da3400"

	b, err := hex.DecodeString(hexdump)
	if err != nil {
		panic(err)
	}

	decoder := decoder.RealDecoder{}
	decoder.Init(b)

	frame, err := decoder.GetInt8()
	fmt.Printf("frame: %d\n", frame)
	assert.NoError(t, err)

	typ, err := decoder.GetInt8()
	fmt.Printf("typ: %d\n", typ)
	assert.NoError(t, err)

	version, err := decoder.GetInt8()
	fmt.Printf("version: %d\n", version)
	assert.NoError(t, err)

	partitionID, err := decoder.GetInt32()
	fmt.Printf("partitionID: %d\n", partitionID)
	assert.NoError(t, err)

	topicID, err := getUUID(&decoder)
	fmt.Printf("topicID: %s\n", topicID)
	assert.NoError(t, err)

	arrayLength, err := decoder.GetUnsignedVarint()
	fmt.Printf("arrayLength: %d\n", arrayLength)
	assert.NoError(t, err)

	var replicas []int32
	for i := 0; i < int(arrayLength-1); i++ {
		replica, err := decoder.GetInt32()
		assert.NoError(t, err)
		replicas = append(replicas, replica)
	}
	fmt.Printf("replicas: %v\n", replicas)

	arrayLength, err = decoder.GetUnsignedVarint()
	fmt.Printf("arrayLength: %d\n", arrayLength)
	assert.NoError(t, err)

	var inSyncReplicas []int32
	for i := 0; i < int(arrayLength-1); i++ {
		replica, err := decoder.GetInt32()
		assert.NoError(t, err)
		inSyncReplicas = append(inSyncReplicas, replica)
	}
	fmt.Printf("inSyncReplicas: %v\n", inSyncReplicas)

	arrayLength, err = decoder.GetUnsignedVarint()
	fmt.Printf("arrayLength: %d\n", arrayLength)
	assert.NoError(t, err)

	var removingReplicas []int32
	for i := 0; i < int(arrayLength-1); i++ {
		replica, err := decoder.GetInt32()
		assert.NoError(t, err)
		removingReplicas = append(removingReplicas, replica)
	}
	fmt.Printf("removingReplicas: %v\n", removingReplicas)

	arrayLength, err = decoder.GetUnsignedVarint()
	fmt.Printf("arrayLength: %d\n", arrayLength)
	assert.NoError(t, err)

	var addingReplicas []int32
	for i := 0; i < int(arrayLength-1); i++ {
		replica, err := decoder.GetInt32()
		assert.NoError(t, err)
		addingReplicas = append(addingReplicas, replica)
	}
	fmt.Printf("addingReplicas: %v\n", addingReplicas)

	leader, err := decoder.GetInt32()
	fmt.Printf("leader: %d\n", leader)
	assert.NoError(t, err)

	leaderEpoch, err := decoder.GetInt32()
	fmt.Printf("leaderEpoch: %d\n", leaderEpoch)
	assert.NoError(t, err)

	partitionEpoch, err := decoder.GetInt32()
	fmt.Printf("partitionEpoch: %d\n", partitionEpoch)
	assert.NoError(t, err)

	if version >= 1 {
		arrayLength, err = decoder.GetUnsignedVarint()
		fmt.Printf("arrayLength: %d\n", arrayLength)
		assert.NoError(t, err)

		var directories []string
		for i := 0; i < int(arrayLength-1); i++ {
			replica, err := getUUID(&decoder)
			assert.NoError(t, err)
			directories = append(directories, replica)
		}
		fmt.Printf("directories: %v\n", directories)
	}

	tagFieldCount, err := decoder.GetUnsignedVarint()
	fmt.Printf("tagFieldCount: %d\n", tagFieldCount)
	assert.NoError(t, err)

	assert.Equal(t, 0, int(decoder.Remaining()))
}

func getUUID(pd *decoder.RealDecoder) (string, error) {
	topicUUIDBytes, err := pd.GetRawBytes(16)
	if err != nil {
		return "", err
	}
	topicUUID, err := encoder.DecodeUUID(topicUUIDBytes)
	if err != nil {
		return "", err
	}
	return topicUUID, nil
}

func TestEncodeBeginTransactionRecord(t *testing.T) {
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

func TestEncodeFeatureLevelRecord(t *testing.T) {
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

func TestEncodeZKMigrationRecord(t *testing.T) {
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

func TestEncodeEndTransactionRecord(t *testing.T) {
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

func TestEncodeTopicRecord(t *testing.T) {
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

func TestEncodePartitionRecord(t *testing.T) {
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
