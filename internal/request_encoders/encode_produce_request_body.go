package request_encoders

import (
	"fmt"

	"github.com/codecrafters-io/kafka-tester/internal/field_encoder"
	"github.com/codecrafters-io/kafka-tester/protocol/encoder"
	"github.com/codecrafters-io/kafka-tester/protocol/kafkaapi"
	"github.com/codecrafters-io/kafka-tester/protocol/value"
)

func encodeProduceRequestBody(requestBody kafkaapi.ProduceRequestBody, encoder *field_encoder.FieldEncoder) {
	encoder.WriteCompactNullableStringField("TransactionalID", requestBody.TransactionalId)
	encoder.WriteInt16Field("Acks", requestBody.Acks)
	encoder.WriteInt32Field("TimeoutMS", requestBody.TimeoutMs)
	encodeCompactArray(requestBody.Topics, encoder, "Topics", encodeProduceRequestTopicData)
	encoder.WriteEmptyTagBuffer()
}

func encodeProduceRequestTopicData(topicData kafkaapi.ProduceRequestTopicData, encoder *field_encoder.FieldEncoder) {
	encoder.WriteCompactStringField("Name", topicData.Name)
	encodeCompactArray(topicData.Partitions, encoder, "Partitions", encodeProduceRequestPartitionData)
	encoder.WriteEmptyTagBuffer()
}

func encodeProduceRequestPartitionData(partitionData kafkaapi.ProduceRequestPartitionData, fieldEncoder *field_encoder.FieldEncoder) {
	fieldEncoder.WriteInt32Field("Id", partitionData.Id)

	encoder := encoder.NewEncoder()
	partitionData.RecordBatches.Encode(encoder)
	recordBatchesTotalSize := len(encoder.Bytes())
	fieldEncoder.WriteUvarint("RecordBatchesSize", value.UnsignedVarint{Value: uint64(recordBatchesTotalSize) + 1})

	for _, recordBatch := range partitionData.RecordBatches {
		encodeProduceRequestRecordBatch(recordBatch, fieldEncoder)
	}

	fieldEncoder.WriteEmptyTagBuffer()
}

func encodeProduceRequestRecordBatch(recordBatch kafkaapi.RecordBatch, encoder *field_encoder.FieldEncoder) {
	encoder.PushPathContext("RecordBatch")
	defer encoder.PopPathContext()

	// Initialize these values so they can be encoded properly
	recordBatch.SetCRC()
	recordBatch.SetBatchLength()

	encoder.WriteInt64Field("BaseOffset", recordBatch.BaseOffset)
	encoder.WriteInt32Field("BatchLength", recordBatch.BatchLength)
	encoder.WriteInt32Field("PartitionLeaderEpoch", recordBatch.PartitionLeaderEpoch)
	encoder.WriteInt8Field("Magic", recordBatch.Magic)
	encoder.WriteInt32Field("CRC", recordBatch.CRC)

	// Properties
	encoder.WriteInt16Field("Attributes", recordBatch.Attributes)
	encoder.WriteInt32Field("LastOffsetDelta", recordBatch.LastOffsetDelta)
	encoder.WriteInt64Field("FirstTimeStamp", recordBatch.FirstTimestamp)
	encoder.WriteInt64Field("MaxTimeStamp", recordBatch.MaxTimestamp)
	encoder.WriteInt64Field("ProducerId", recordBatch.ProducerId)
	encoder.WriteInt16Field("ProducerEpoch", recordBatch.ProducerEpoch)
	encoder.WriteInt32Field("BaseSequence", recordBatch.BaseSequence)
	encoder.WriteInt32Field("RecordsLength", value.Int32{Value: int32(len(recordBatch.Records))})

	for i, record := range recordBatch.Records {
		record.OffsetDelta = value.Varint{Value: int64(i)}
		encodeProduceRequestRecord(record, encoder)
	}
}

func encodeProduceRequestRecord(record kafkaapi.Record, encoder *field_encoder.FieldEncoder) {
	encoder.PushPathContext(fmt.Sprintf("Record[%d]", record.OffsetDelta.Value))
	defer encoder.PopPathContext()

	record.SetSize()

	encoder.WriteVarint("Size", record.Size)
	encoder.WriteInt8Field("Attributes", record.Attributes)
	encoder.WriteVarint("TimestampDelta", record.TimestampDelta)
	encoder.WriteVarint("OffsetDelta", record.OffsetDelta)

	// Special encoding that does not belong to any data type and is only present inside Records
	// similar to protobuf encoding. It is mentioned in the Kafka docs here:  https://kafka.apache.org/documentation/#recordheader
	if record.Key.Value == nil {
		encoder.WriteVarint("KeyLength", value.Varint{Value: -1})
	} else {
		encoder.WriteVarint("KeyLength", value.Varint{Value: int64(len(record.Key.Value))})
		encoder.WriteRawBytes("Key", record.Key)
	}

	if record.Value.Value == nil {
		encoder.WriteVarint("ValueLength", value.Varint{Value: -1})
	} else {
		encoder.WriteVarint("ValueLength", value.Varint{Value: int64(len(record.Value.Value))})
		encoder.WriteRawBytes("Value", record.Value)
	}

	if len(record.Headers) > 0 {
		panic("Codecrafters Internal Error - Record Headers in Produce Request is neither empty nor nil")
	}

	if record.Headers == nil {
		encoder.WriteVarint("HeadersLength", value.Varint{Value: -1})
	} else {
		encoder.WriteVarint("HeadersLength", value.Varint{Value: 0})
	}
}
