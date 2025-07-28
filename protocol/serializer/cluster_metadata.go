package serializer

import (
	"fmt"
	"os"

	kafkaapi "github.com/codecrafters-io/kafka-tester/protocol/api"
	"github.com/codecrafters-io/kafka-tester/protocol/builder"
	"github.com/codecrafters-io/kafka-tester/protocol/encoder"
	"github.com/codecrafters-io/tester-utils/logger"
)

func writeClusterMetadata(path string, topic1Name string, topic1UUID string, topic2Name string, topic2UUID string, topic3Name string, topic3UUID string, topic4Name string, topic4UUID string, directoryUUID string, logger *logger.Logger) error {
	encoder := encoder.Encoder{}
	encoder.Init(make([]byte, 40960))

	featureLevelRecord := builder.NewClusterMetadataPayloadBuilder().
		WithFeatureLevelRecord("metadata.version", 20).
		Build()

	topicRecord1 := builder.NewClusterMetadataPayloadBuilder().
		WithTopicRecord(topic1Name, topic1UUID).
		Build()

	partitionRecord1 := builder.NewClusterMetadataPayloadBuilder().
		WithPartitionRecord(0, topic1UUID).
		Build()

	topicRecord2 := builder.NewClusterMetadataPayloadBuilder().
		WithTopicRecord(topic2Name, topic2UUID).
		Build()

	partitionRecord2 := builder.NewClusterMetadataPayloadBuilder().
		WithPartitionRecord(0, topic2UUID).
		Build()

	topicRecord3 := builder.NewClusterMetadataPayloadBuilder().
		WithTopicRecord(topic3Name, topic3UUID).
		Build()

	partitionRecord3_0 := builder.NewClusterMetadataPayloadBuilder().
		WithPartitionRecord(0, topic3UUID).
		Build()

	partitionRecord3_1 := builder.NewClusterMetadataPayloadBuilder().
		WithPartitionRecord(1, topic3UUID).
		Build()

	topicRecord4 := builder.NewClusterMetadataPayloadBuilder().
		WithTopicRecord(topic4Name, topic4UUID).
		Build()

	partitionRecord4_0 := builder.NewClusterMetadataPayloadBuilder().
		WithPartitionRecord(0, topic4UUID).
		Build()

	partitionRecord4_1 := builder.NewClusterMetadataPayloadBuilder().
		WithPartitionRecord(1, topic4UUID).
		Build()

	partitionRecord4_2 := builder.NewClusterMetadataPayloadBuilder().
		WithPartitionRecord(2, topic4UUID).
		Build()

	recordBatch1 := builder.NewRecordBatchBuilder().
		AddRecord(nil, GetEncodedBytes(featureLevelRecord), []kafkaapi.RecordHeader{}).
		Build()

	recordBatch2 := builder.NewRecordBatchBuilder().
		WithBaseOffset(int64(len(recordBatch1.Records)+int(recordBatch1.BaseOffset))).
		AddRecord(nil, GetEncodedBytes(topicRecord1), []kafkaapi.RecordHeader{}).
		AddRecord(nil, GetEncodedBytes(partitionRecord1), []kafkaapi.RecordHeader{}).
		Build()

	recordBatch3 := builder.NewRecordBatchBuilder().
		WithBaseOffset(int64(len(recordBatch2.Records)+int(recordBatch2.BaseOffset))).
		AddRecord(nil, GetEncodedBytes(topicRecord2), []kafkaapi.RecordHeader{}).
		AddRecord(nil, GetEncodedBytes(partitionRecord2), []kafkaapi.RecordHeader{}).
		Build()

	recordBatch4 := builder.NewRecordBatchBuilder().
		WithBaseOffset(int64(len(recordBatch3.Records)+int(recordBatch3.BaseOffset))).
		AddRecord(nil, GetEncodedBytes(topicRecord3), []kafkaapi.RecordHeader{}).
		AddRecord(nil, GetEncodedBytes(partitionRecord3_0), []kafkaapi.RecordHeader{}).
		AddRecord(nil, GetEncodedBytes(partitionRecord3_1), []kafkaapi.RecordHeader{}).
		Build()

	recordBatch5 := builder.NewRecordBatchBuilder().
		WithBaseOffset(int64(len(recordBatch4.Records)+int(recordBatch4.BaseOffset))).
		AddRecord(nil, GetEncodedBytes(topicRecord4), []kafkaapi.RecordHeader{}).
		AddRecord(nil, GetEncodedBytes(partitionRecord4_0), []kafkaapi.RecordHeader{}).
		AddRecord(nil, GetEncodedBytes(partitionRecord4_1), []kafkaapi.RecordHeader{}).
		AddRecord(nil, GetEncodedBytes(partitionRecord4_2), []kafkaapi.RecordHeader{}).
		Build()

	recordBatch1.Encode(&encoder)
	recordBatch2.Encode(&encoder)
	recordBatch3.Encode(&encoder)
	recordBatch4.Encode(&encoder)
	recordBatch5.Encode(&encoder)
	encodedBytes := encoder.Bytes()[:encoder.Offset()]

	err := os.WriteFile(path, encodedBytes, 0644)
	if err != nil {
		return fmt.Errorf("error writing file to %s: %w", path, err)
	}

	logger.Debugf("  - Wrote file to: %s", path)
	return nil
}
