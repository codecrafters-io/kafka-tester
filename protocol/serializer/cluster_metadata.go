package serializer

import (
	"fmt"
	"os"

	kafkaapi "github.com/codecrafters-io/kafka-tester/protocol/api"
	"github.com/codecrafters-io/kafka-tester/protocol/builder"
	"github.com/codecrafters-io/kafka-tester/protocol/common"
	"github.com/codecrafters-io/kafka-tester/protocol/encoder"
	"github.com/codecrafters-io/tester-utils/logger"
)

func writeClusterMetadata(path string, topics []common.TopicConfig, logger *logger.Logger) error {
	featureLevelRecord := builder.NewClusterMetadataPayloadBuilder().
		WithFeatureLevelRecord("metadata.version", 20).
		Build()

	recordBatch1 := builder.NewRecordBatchBuilder().
		AddRecord(nil, GetEncodedBytes(featureLevelRecord), []kafkaapi.RecordHeader{}).
		Build()

	allRecordBatches := kafkaapi.RecordBatches{recordBatch1}

	prevRecordBatch := recordBatch1
	for _, topic := range topics {
		topicRecord := builder.NewClusterMetadataPayloadBuilder().
			WithTopicRecord(topic.Name, topic.UUID).
			Build()

		recordBatchBuilder := builder.NewRecordBatchBuilder().
			WithBaseOffset(int64(len(prevRecordBatch.Records)+int(prevRecordBatch.BaseOffset))).
			AddRecord(nil, GetEncodedBytes(topicRecord), []kafkaapi.RecordHeader{})

		for _, partition := range topic.Partitions {
			partitionRecord := builder.NewClusterMetadataPayloadBuilder().
				WithPartitionRecord(int32(partition.ID), topic.UUID).
				Build()

			recordBatchBuilder.AddRecord(nil, GetEncodedBytes(partitionRecord), []kafkaapi.RecordHeader{})
		}

		recordBatch := recordBatchBuilder.Build()
		allRecordBatches = append(allRecordBatches, recordBatch)
		prevRecordBatch = recordBatch
	}

	encoder := encoder.Encoder{}
	encoder.Init(make([]byte, 40960))
	allRecordBatches.Encode(&encoder)
	encodedBytes := encoder.Bytes()[:encoder.Offset()]

	err := os.WriteFile(path, encodedBytes, 0644)
	if err != nil {
		return fmt.Errorf("error writing file to %s: %w", path, err)
	}

	logger.Debugf("  - Wrote file to: %s", path)
	return nil
}
