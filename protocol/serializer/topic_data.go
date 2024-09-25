package serializer

import (
	"fmt"
	"os"

	kafkaapi "github.com/codecrafters-io/kafka-tester/protocol/api"
	kafkaencoder "github.com/codecrafters-io/kafka-tester/protocol/encoder"
)

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

func writePartitionMetadata(path string, version int, topicID string) error {
	content := fmt.Sprintf("version: %d\ntopic_id: %s", version, topicID)
	err := os.WriteFile(path, []byte(content), 0644)
	if err != nil {
		return fmt.Errorf("error writing partition metadata file: %w", err)
	}
	fmt.Printf("Successfully wrote partition.metadata file to: %s\n", path)
	return nil
}
