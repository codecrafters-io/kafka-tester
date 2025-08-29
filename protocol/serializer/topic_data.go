package serializer

import (
	"fmt"
	"os"

	"github.com/codecrafters-io/kafka-tester/protocol/encoder"
	kafkaapi "github.com/codecrafters-io/kafka-tester/protocol/kafkaapi_legacy"
	"github.com/codecrafters-io/tester-utils/logger"
)

func serializeTopicData(messages []string) []byte {
	// Given an array of messages, generates the RecordBatch struct first,
	// then uses the encoder.Encoder to encode them into bytes.
	encoder := encoder.Encoder{}
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

func writeTopicData(path string, messages []string, logger *logger.Logger) error {
	encodedBytes := serializeTopicData(messages)

	err := os.WriteFile(path, encodedBytes, 0644)
	if err != nil {
		return fmt.Errorf("error writing file to %s: %w", path, err)
	}

	logger.Debugf("  - Wrote file to: %s", path)
	return nil
}

func writePartitionMetadata(path string, version int, topicID string, logger *logger.Logger) error {
	content := fmt.Sprintf("version: %d\ntopic_id: %s", version, topicID)

	err := os.WriteFile(path, []byte(content), 0644)
	if err != nil {
		return fmt.Errorf("error writing partition metadata file: %w", err)
	}

	logger.Debugf("  - Wrote file to: %s", path)
	return nil
}
