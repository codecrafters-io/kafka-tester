package serializer

import (
	"bytes"
	"encoding/base64"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/codecrafters-io/kafka-tester/internal"
	kafkaapi "github.com/codecrafters-io/kafka-tester/protocol/api"
	"github.com/codecrafters-io/kafka-tester/protocol/common"
	"github.com/codecrafters-io/kafka-tester/protocol/encoder"
	"github.com/google/uuid"
)

func generateClusterMetadata() {
	encoder := encoder.RealEncoder{}
	encoder.Init(make([]byte, 4096000))

	featureLevelRecord := kafkaapi.ClusterMetadataPayload{
		FrameVersion: 1,
		Type:         12,
		Version:      0,
		Data: &kafkaapi.FeatureLevelRecord{
			Name:         "metadata.version",
			FeatureLevel: 20,
		},
	}

	topicRecord := kafkaapi.ClusterMetadataPayload{
		FrameVersion: 1,
		Type:         2,
		Version:      0,
		Data: &kafkaapi.TopicRecord{
			TopicName: "foo",
			TopicUUID: "bfd99e5e-3235-4552-81f8-d4af1741970c"},
	}

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
				Value:          getEncodedBytes(featureLevelRecord),
				Headers:        []kafkaapi.RecordHeader{},
			},
		},
	}
	recordBatch2 := kafkaapi.RecordBatch{
		BaseOffset:           int64(len(recordBatch1.Records) + 1),
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
				Value:          getEncodedBytes(topicRecord),
				Headers:        []kafkaapi.RecordHeader{},
			},
			{
				Attributes:     0,
				TimestampDelta: 0,
				Key:            nil,
				Value:          getEncodedBytes(partitionRecord),
				Headers:        []kafkaapi.RecordHeader{},
			},
		},
	}

	recordBatch1.Encode(&encoder)
	recordBatch2.Encode(&encoder)
	encodedBytes := encoder.Bytes()[:encoder.Offset()]

	path := "/Users/ryang/Developer/work/course-testers/kafka-tester/internal/test_helpers/pass_all/kraft-generated-logs/__cluster_metadata-0/00000000000000000000.log"

	existingBytes, err := os.ReadFile(path)
	if err != nil {
		fmt.Printf("Error reading file: %v\n", err)
		return
	}
	out := internal.GetFormattedHexdump(existingBytes)
	fmt.Printf("OLD:\n%s\n\n", out)

	err = os.WriteFile(path, encodedBytes, 0644)
	if err != nil {
		fmt.Printf("Error writing file: %v\n", err)
	}

	out = internal.GetFormattedHexdump(encodedBytes)
	fmt.Printf("NEW:\n%s\n\n", out)

	if !bytes.Equal(existingBytes, encodedBytes) {
		fmt.Printf("Bytes are different\n")
	}
}

func getEncodedBytes(encodableObject interface{}) []byte {
	encoder := encoder.RealEncoder{}
	encoder.Init(make([]byte, 1024))

	switch obj := encodableObject.(type) {
	case kafkaapi.ClusterMetadataPayload:
		obj.Encode(&encoder)
	}

	encoded := encoder.Bytes()[:encoder.Offset()]

	return encoded
}

func generateTopicData() {
	encoder := encoder.RealEncoder{}
	encoder.Init(make([]byte, 4096))

	recordBatch := kafkaapi.RecordBatch{
		BaseOffset:           0,
		PartitionLeaderEpoch: 0,
		Attributes:           0,
		LastOffsetDelta:      0,
		FirstTimestamp:       1726045973899,
		MaxTimestamp:         1726045973899,
		ProducerId:           0,
		ProducerEpoch:        0,
		BaseSequence:         0,
		Records: []kafkaapi.Record{
			{
				Attributes:     0,
				TimestampDelta: 0,
				Key:            nil,
				Value:          []byte(common.TOPIC1_MESSAGE1),
				Headers:        []kafkaapi.RecordHeader{},
			},
		},
	}

	recordBatch.Encode(&encoder)
	encodedBytes := encoder.Bytes()[:encoder.Offset()]

	path := "/Users/ryang/Developer/work/course-testers/kafka-tester/internal/test_helpers/pass_all/kraft-generated-logs/foo-0/00000000000000000000.log"

	existingBytes, err := os.ReadFile(path)
	if err != nil {
		fmt.Printf("Error reading file: %v\n", err)
		return
	}
	out := internal.GetFormattedHexdump(existingBytes)
	fmt.Printf("OLD:\n%s\n\n", out)

	err = os.WriteFile(path, encodedBytes, 0644)
	if err != nil {
		fmt.Printf("Error writing file: %v\n", err)
	}

	out = internal.GetFormattedHexdump(encodedBytes)
	fmt.Printf("NEW:\n%s\n\n", out)

	if !bytes.Equal(existingBytes, encodedBytes) {
		fmt.Printf("Bytes are different\n")
	}
}

func GenerateClusterMetadata(path string, topicName string, topicUUID string, directoryUUID string) {
	encoder := encoder.RealEncoder{}
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

	topicRecord := kafkaapi.ClusterMetadataPayload{
		FrameVersion: 1,
		Type:         2,
		Version:      0,
		Data: &kafkaapi.TopicRecord{
			TopicName: topicName,
			TopicUUID: topicUUID,
		},
	}

	partitionRecord := kafkaapi.ClusterMetadataPayload{
		FrameVersion: 1,
		Type:         3,
		Version:      1,
		Data: &kafkaapi.PartitionRecord{
			PartitionID:      0,
			TopicUUID:        topicUUID,
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
				Value:          getEncodedBytes(featureLevelRecord),
				Headers:        []kafkaapi.RecordHeader{},
			},
		},
	}
	recordBatch2 := kafkaapi.RecordBatch{
		BaseOffset:           int64(len(recordBatch1.Records) + 1),
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
				Value:          getEncodedBytes(topicRecord),
				Headers:        []kafkaapi.RecordHeader{},
			},
			{
				Attributes:     0,
				TimestampDelta: 0,
				Key:            nil,
				Value:          getEncodedBytes(partitionRecord),
				Headers:        []kafkaapi.RecordHeader{},
			},
		},
	}

	recordBatch1.Encode(&encoder)
	recordBatch2.Encode(&encoder)
	encodedBytes := encoder.Bytes()[:encoder.Offset()]

	err := os.WriteFile(path, encodedBytes, 0644)
	if err != nil {
		fmt.Printf("Error writing file: %v\n", err)
	}
	fmt.Printf("Successfully wrote cluster metadata file to: %s\n", path)
}

func GenerateTopicData(path string, topicName string, message string) {
	encoder := encoder.RealEncoder{}
	encoder.Init(make([]byte, 4096))

	recordBatch := kafkaapi.RecordBatch{
		BaseOffset:           0,
		PartitionLeaderEpoch: 0,
		Attributes:           0,
		LastOffsetDelta:      0,
		FirstTimestamp:       1726045973899,
		MaxTimestamp:         1726045973899,
		ProducerId:           0,
		ProducerEpoch:        0,
		BaseSequence:         0,
		Records: []kafkaapi.Record{
			{
				Attributes:     0,
				TimestampDelta: 0,
				Key:            nil,
				Value:          []byte(message),
				Headers:        []kafkaapi.RecordHeader{},
			},
		},
	}

	recordBatch.Encode(&encoder)
	encodedBytes := encoder.Bytes()[:encoder.Offset()]

	err := os.WriteFile(path, encodedBytes, 0644)
	if err != nil {
		fmt.Printf("Error writing file: %v\n", err)
	}
	fmt.Printf("Successfully wrote topic data file to: %s\n", path)
}

func writePartitionMetadata(version int, topicID string, path string) error {
	content := fmt.Sprintf("version: %d\ntopic_id: %s", version, topicID)
	err := os.WriteFile(path, []byte(content), 0644)
	if err != nil {
		return fmt.Errorf("error writing partition metadata file: %w", err)
	}
	fmt.Printf("Successfully wrote partition.metadata file to: %s\n", path)
	return nil
}

func generatePartitionMetadataFile(path string, versionID int, topicID string) error {
	return writePartitionMetadata(versionID, topicID, path)
}

func generatePartitionMetadataFiles() {
	generatePartitionMetadataFile("/Users/ryang/Developer/work/course-testers/kafka-tester/internal/test_helpers/pass_all/kraft-generated-logs/foo-0/partition.metadata", 0, "v9meXjI1RVKB-NSvF0GXDA")

	generatePartitionMetadataFile("/Users/ryang/Developer/work/course-testers/kafka-tester/internal/test_helpers/pass_all/kraft-generated-logs/__cluster_metadata-0/partition.metadata", 0, "AAAAAAAAAAAAAAAAAAAAAQ")
}

func writeMetaProperties(clusterID, directoryID string, nodeID, version int, path string) error {
	content := fmt.Sprintf("#\n#%s\ncluster.id=%s\ndirectory.id=%s\nnode.id=%d\nversion=%d\n",
		time.Now().Format("Mon Jan 02 15:04:05 MST 2006"), clusterID, directoryID, nodeID, version)

	err := os.WriteFile(path, []byte(content), 0644)
	if err != nil {
		return fmt.Errorf("error writing meta properties file: %w", err)
	}
	fmt.Printf("Successfully wrote meta.properties file to: %s\n", path)
	return nil
}

func generateMetaPropertiesFile(path, clusterID, directoryID string, nodeID, version int) error {
	return writeMetaProperties(clusterID, directoryID, nodeID, version, path)
}

func Generate_all_required_files() {
	clusterID := common.CLUSTER_ID
	directoryID, _ := uuidToBase64(common.DIRECTORY_UUID)
	nodeID := common.NODE_ID
	version := common.VERSION
	topicName := common.TOPIC_NAME
	topicID, _ := uuidToBase64(common.TOPIC_UUID)
	topicUUID := common.TOPIC_UUID
	clusterMetadataTopicID := common.CLUSTER_METADATA_TOPIC_ID
	directoryUUID := common.DIRECTORY_UUID
	fmt.Printf("directoryUUID: %s\n", directoryUUID)
	fmt.Printf("directoryID: %s\n", directoryID)

	basePath := "/Users/ryang/Developer/work/course-testers/kafka-tester/internal/test_helpers/pass_all/kraft-genx-logs/"
	topicMetadataDirectory := fmt.Sprintf("%s/%s-0", basePath, topicName)
	clusterMetadataDirectory := fmt.Sprintf("%s/__cluster_metadata-0", basePath)

	metaPropertiesPath := fmt.Sprintf("%s/meta.properties", basePath)
	topicMetadataPath := fmt.Sprintf("%s/partition.metadata", topicMetadataDirectory)
	clusterMetadataPath := fmt.Sprintf("%s/partition.metadata", clusterMetadataDirectory)
	topicDataPath := fmt.Sprintf("%s/00000000000000000000.log", topicMetadataDirectory)
	clusterMetadataFilePath := fmt.Sprintf("%s/00000000000000000000.log", clusterMetadataDirectory)

	generateDirectory(topicMetadataDirectory)
	generateDirectory(clusterMetadataDirectory)

	generateMetaPropertiesFile(metaPropertiesPath, clusterID, directoryID, nodeID, version)
	generatePartitionMetadataFile(topicMetadataPath, 0, topicID)
	generatePartitionMetadataFile(clusterMetadataPath, 0, clusterMetadataTopicID)

	GenerateTopicData(topicDataPath, topicName, "Hello World!")

	GenerateClusterMetadata(clusterMetadataFilePath, topicName, topicUUID, directoryUUID)
}

func generateDirectory(path string) {
	err := os.MkdirAll(path, 0755)
	if err != nil {
		fmt.Printf("Error creating directory: %v\n", err)
	}
}

func base64ToUUID(base64Str string) (string, error) {
	base64Str = strings.Replace(base64Str, "-", "+", 1)
	base64Str += "=="
	decoded, err := base64.StdEncoding.DecodeString(base64Str)
	if err != nil {
		return "", fmt.Errorf("error decoding base64 string: %w", err)
	}

	if len(decoded) != 16 {
		return "", fmt.Errorf("decoded byte array is not 16 bytes long")
	}

	uuid := fmt.Sprintf("%08x-%04x-%04x-%04x-%012x",
		decoded[0:4],
		decoded[4:6],
		decoded[6:8],
		decoded[8:10],
		decoded[10:16])

	return uuid, nil
}

func uuidToBase64(uuidStr string) (string, error) {
	parsedUUID, err := uuid.Parse(uuidStr)
	if err != nil {
		return "", fmt.Errorf("error parsing UUID string: %w", err)
	}

	base64Str := base64.StdEncoding.EncodeToString(parsedUUID[:])
	// Replace '+' with '-' to match the original format
	base64Str = strings.Replace(base64Str, "+", "-", 1)
	// Remove padding
	base64Str = strings.TrimRight(base64Str, "=")
	return base64Str, nil
}
