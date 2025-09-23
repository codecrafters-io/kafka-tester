package builder

import (
	"github.com/codecrafters-io/kafka-tester/protocol/kafka_files_generator"
	"github.com/codecrafters-io/kafka-tester/protocol/kafkaapi"
	"github.com/codecrafters-io/kafka-tester/protocol/value"
	"github.com/codecrafters-io/tester-utils/random"
)

// GetProduceRequestTopicData builds TopicData for Produce request based on the topics and partitions created so far
// The produce request will issue 2-3 logs per partition of each topic while building the request
func GetProduceRequestTopicData(generatedLogDirectoryData *kafka_files_generator.GeneratedLogDirectoryData) []ProduceRequestTopicData {
	// for each topic and each partition
	// generate 2-3 logs
	topicData := []ProduceRequestTopicData{}

	for _, topic := range generatedLogDirectoryData.GeneratedTopicsData {

		partitionData := []ProduceRequestPartitionData{}

		// generate partition data for each partitions inside the topic
		for _, partition := range topic.GeneratedRecordBatchesByPartition {
			partitionData = append(partitionData, ProduceRequestPartitionData{
				PartitionId: int32(partition.PartitionId),
				Logs:        random.RandomWords(random.RandomInt(2, 4)),
			})
		}

		topicData = append(topicData, ProduceRequestTopicData{
			TopicName:              topic.Name,
			PartitionsCreationData: partitionData,
		})
	}

	return topicData
}

type ProduceRequestPartitionData struct {
	PartitionId int32
	Logs        []string
}

type ProduceRequestTopicData struct {
	TopicName              string
	PartitionsCreationData []ProduceRequestPartitionData
}

type ProduceRequestBuilder struct {
	correlationId     int32
	topicCreationData []ProduceRequestTopicData
}

func NewProduceRequestBuilder() *ProduceRequestBuilder {
	return &ProduceRequestBuilder{}
}

func (b *ProduceRequestBuilder) WithCorrelationId(correlationId int32) *ProduceRequestBuilder {
	b.correlationId = correlationId
	return b
}

func (b *ProduceRequestBuilder) WithTopicRequestData(topicData []ProduceRequestTopicData) *ProduceRequestBuilder {
	b.topicCreationData = topicData
	return b
}

func (b *ProduceRequestBuilder) Build() kafkaapi.ProduceRequest {
	topicsArray := []kafkaapi.ProduceRequestTopicData{}

	// For each topic
	for _, topic := range b.topicCreationData {

		partitionArray := []kafkaapi.ProduceRequestPartitionData{}

		// For each partition of each topic
		for _, partition := range topic.PartitionsCreationData {
			records := []kafkaapi.Record{}

			// Make one record object for one log inside the partition
			for _, log := range partition.Logs {
				records = append(records, kafkaapi.Record{
					Attributes:     value.Int8{Value: 0},
					TimestampDelta: value.Varint{Value: 0},
					Key:            value.RawBytes{},
					Value:          value.RawBytes{Value: []byte(log)},
					Headers:        []kafkaapi.RecordHeader{},
				})
			}

			partitionData := kafkaapi.ProduceRequestPartitionData{
				Id: value.Int32{Value: partition.PartitionId},
				RecordBatches: []kafkaapi.RecordBatch{
					{
						BaseOffset:           value.Int64{Value: 0},
						PartitionLeaderEpoch: value.Int32{Value: 0},
						Magic:                value.Int8{Value: 2},
						Attributes:           value.Int16{Value: 0},
						LastOffsetDelta:      value.Int32{Value: int32(len(records) - 1)},
						FirstTimestamp:       value.Int64{Value: 1726045973899},
						MaxTimestamp:         value.Int64{Value: 1726045973899},
						ProducerId:           value.Int64{Value: 0},
						ProducerEpoch:        value.Int16{Value: 0},
						BaseSequence:         value.Int32{Value: 0},
						Records:              records,
					},
				},
			}

			partitionArray = append(partitionArray, partitionData)

		}

		topicsArray = append(topicsArray, kafkaapi.ProduceRequestTopicData{
			Name:       value.CompactString{Value: topic.TopicName},
			Partitions: partitionArray,
		})
	}

	return kafkaapi.ProduceRequest{
		Header: NewRequestHeaderBuilder().BuildProduceRequestHeader(b.correlationId),
		Body: kafkaapi.ProduceRequestBody{
			TransactionalId: value.CompactNullableString{},
			Acks:            value.Int16{Value: -1},
			TimeoutMs:       value.Int32{Value: 30000},
			Topics:          topicsArray,
		},
	}
}
