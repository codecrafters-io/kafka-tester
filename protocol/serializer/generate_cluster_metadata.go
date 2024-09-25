package serializer

import (
	_ "embed"
	"encoding/base64"
	"fmt"
	"os"
	"strings"
	"time"

	kafkaapi "github.com/codecrafters-io/kafka-tester/protocol/api"
	"github.com/codecrafters-io/kafka-tester/protocol/common"
	kafkaencoder "github.com/codecrafters-io/kafka-tester/protocol/encoder"
	"github.com/google/uuid"
)

// writeKraftServerProperties writes the embedded kraft.server.properties content to /tmp/kraft-combined-logs/kraft.server.properties
func writeKraftServerProperties(path string) {
	kraftServerProperties := `process.roles=broker,controller
node.id=1
controller.quorum.voters=1@localhost:9093
listeners=PLAINTEXT://:9092,CONTROLLER://:9093
controller.listener.names=CONTROLLER
listener.security.protocol.map=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,SSL:SSL,SASL_PLAINTEXT:SASL_PLAINTEXT,SASL_SSL:SASL_SSL
log.dirs=/tmp/kraft-combined-logs`

	err := os.WriteFile(path, []byte(kraftServerProperties), 0644)
	if err != nil {
		// ToDo error handling
		// All generate file methods need to handle errors properly
		fmt.Printf("Failed to write to file %s: %v", path, err)
	}

	fmt.Printf("Successfully wrote embedded kraft.server.properties to %s", path)
}

func GetEncodedBytes(encodableObject interface{}) []byte {
	encoder := kafkaencoder.RealEncoder{}
	encoder.Init(make([]byte, 1024))

	switch obj := encodableObject.(type) {
	case kafkaapi.ClusterMetadataPayload:
		obj.Encode(&encoder)
	}

	encoded := encoder.Bytes()[:encoder.Offset()]

	return encoded
}

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

func serializeTopicData(messages []string) []byte {
	encoder := kafkaencoder.RealEncoder{}
	encoder.Init(make([]byte, 4096))

	for i, message := range messages {
		recordBatch := kafkaapi.RecordBatch{
			BaseOffset:           int64(i),
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
	}

	return encoder.Bytes()[:encoder.Offset()]
}

func writeTopicData(path string, messages []string) {
	encodedBytes := serializeTopicData(messages)

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

func GenerateLogDirs() {
	// Topic1 -> Message1 (Partition=1)
	// Topic2 -> None (Partition=1)
	// Topic3 -> Message2, Message3 (Partition=2)

	// meta
	nodeID := common.NODE_ID
	version := common.VERSION
	directoryUUID := common.DIRECTORY_UUID
	directoryID, _ := uuidToBase64(common.DIRECTORY_UUID)

	// cluster
	clusterID := common.CLUSTER_ID
	clusterMetadataTopicID := common.CLUSTER_METADATA_TOPIC_ID

	// topics
	topic1Name := common.TOPIC1_NAME
	topic1ID, _ := uuidToBase64(common.TOPIC1_UUID)
	topic1UUID := common.TOPIC1_UUID
	topic2Name := common.TOPIC2_NAME
	topic2ID, _ := uuidToBase64(common.TOPIC2_UUID)
	topic2UUID := common.TOPIC2_UUID
	topic3Name := common.TOPIC3_NAME
	topic3ID, _ := uuidToBase64(common.TOPIC3_UUID)
	topic3UUID := common.TOPIC3_UUID

	basePath := common.LOG_DIR

	topic1MetadataDirectory := fmt.Sprintf("%s/%s-0", basePath, topic1Name)
	topic2MetadataDirectory := fmt.Sprintf("%s/%s-0", basePath, topic2Name)
	topic3Partition1MetadataDirectory := fmt.Sprintf("%s/%s-0", basePath, topic3Name)
	topic3Partition2MetadataDirectory := fmt.Sprintf("%s/%s-1", basePath, topic3Name)
	clusterMetadataDirectory := fmt.Sprintf("%s/__cluster_metadata-0", basePath)

	kraftServerPropertiesPath := fmt.Sprintf("%s/kraft.server.properties", basePath)
	metaPropertiesPath := fmt.Sprintf("%s/meta.properties", basePath)
	topic1MetadataPath := fmt.Sprintf("%s/partition.metadata", topic1MetadataDirectory)
	topic2MetadataPath := fmt.Sprintf("%s/partition.metadata", topic2MetadataDirectory)
	topic3Partition1MetadataPath := fmt.Sprintf("%s/partition.metadata", topic3Partition1MetadataDirectory)
	topic3Partition2MetadataPath := fmt.Sprintf("%s/partition.metadata", topic3Partition2MetadataDirectory)
	clusterMetadataMetadataPath := fmt.Sprintf("%s/partition.metadata", clusterMetadataDirectory)
	topic1DataFilePath := fmt.Sprintf("%s/00000000000000000000.log", topic1MetadataDirectory)
	topic2DataFilePath := fmt.Sprintf("%s/00000000000000000000.log", topic2MetadataDirectory)
	topic3Partition1DataFilePath := fmt.Sprintf("%s/00000000000000000000.log", topic3Partition1MetadataDirectory)
	topic3Partition2DataFilePath := fmt.Sprintf("%s/00000000000000000000.log", topic3Partition2MetadataDirectory)
	clusterMetadataDataFilePath := fmt.Sprintf("%s/00000000000000000000.log", clusterMetadataDirectory)

	generateDirectories([]string{topic1MetadataDirectory, topic2MetadataDirectory, topic3Partition1MetadataDirectory, topic3Partition2MetadataDirectory, clusterMetadataDirectory})

	generateMetaPropertiesFile(metaPropertiesPath, clusterID, directoryID, nodeID, version)
	generatePartitionMetadataFile(topic1MetadataPath, 0, topic1ID)
	generatePartitionMetadataFile(topic2MetadataPath, 0, topic2ID)
	generatePartitionMetadataFile(topic3Partition1MetadataPath, 0, topic3ID)
	generatePartitionMetadataFile(topic3Partition2MetadataPath, 0, topic3ID)
	generatePartitionMetadataFile(clusterMetadataPath, 0, clusterMetadataTopicID)

	writeTopicData(topic1DataFilePath, []string{common.MESSAGE1})
	writeTopicData(topic2DataFilePath, []string{})
	writeTopicData(topic3Partition1DataFilePath, []string{common.MESSAGE2, common.MESSAGE3})
	writeTopicData(topic3Partition2DataFilePath, []string{})

	writeClusterMetadata(clusterMetadataDataFilePath, topic1Name, topic1UUID, topic2Name, topic2UUID, topic3Name, topic3UUID, directoryUUID)

	writeKraftServerProperties(kraftServerPropertiesPath)
}

func generateDirectories(paths []string) {
	// ToDo: error handling
	for _, path := range paths {
		generateDirectory(path)
	}
}

func generateDirectory(path string) {
	// ToDo: error handling
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
