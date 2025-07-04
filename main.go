package main

import (
	"fmt"
	"strings"
	"time"

	"github.com/codecrafters-io/kafka-tester/protocol"
	kafkaapi "github.com/codecrafters-io/kafka-tester/protocol/api"
	"github.com/codecrafters-io/tester-utils/logger"
)

const UNKNOWN_TOPIC = "test-topic"

func main() {
	correlationId := int32(7)

	stageLogger := logger.GetLogger(true, "[test] ")
	broker := protocol.NewBroker("localhost:9092")
	if err := broker.Connect(); err != nil {
		stageLogger.Errorf("Failed to connect to broker: %v", err)
		return
	}

	// request := getUnknownTopicRequest(correlationId)
	// request := getWrongPartitionRequest(correlationId)
	// request := getSuccessRequest(correlationId)
	request := getPR5Request(correlationId)
	// request := getPR6MultiPartitionRequest(correlationId)
	// request := getPR7Request(correlationId)

	message := kafkaapi.EncodeProduceRequest(&request)
	stageLogger.Infof("Sending \"Produce\" (version: %v) request (Correlation id: %v)", request.Header.ApiVersion, request.Header.CorrelationId)
	stageLogger.Debugf("Hexdump of sent \"Produce\" request: \n%v\n", GetFormattedHexdump(message))

	response, err := broker.SendAndReceive(message)
	if err != nil {
		stageLogger.Errorf("Failed to send and receive \"Produce\" request: %v", err)
		return
	}
	stageLogger.Debugf("Hexdump of received \"Produce\" response: \n%v\n", GetFormattedHexdump(response.RawBytes))

	// For now, just decode the header to verify we get a response
	responseHeader, err := kafkaapi.DecodeProduceHeader(response.Payload, 11, stageLogger)
	if err != nil {
		stageLogger.Errorf("Failed to decode \"Produce\" response header: %v", err)
		return
	}

	if responseHeader.CorrelationId != int32(correlationId) {
		stageLogger.Errorf("Expected Correlation ID to be %v, got %v", correlationId, responseHeader.CorrelationId)
		return
	}
	stageLogger.Successf("✓ Correlation ID: %v", responseHeader.CorrelationId)
	stageLogger.Successf("✓ Produce request/response cycle completed!")

}

func GetFormattedHexdump(data []byte) string {
	// This is used for logs
	// Contains headers + vertical & horizontal separators + offset
	// We use a different format for the error logs
	var formattedHexdump strings.Builder
	var asciiChars strings.Builder

	formattedHexdump.WriteString("Idx  | Hex                                             | ASCII\n")
	formattedHexdump.WriteString("-----+-------------------------------------------------+-----------------\n")

	for i, b := range data {
		if i%16 == 0 && i != 0 {
			formattedHexdump.WriteString("| " + asciiChars.String() + "\n")
			asciiChars.Reset()
		}
		if i%16 == 0 {
			formattedHexdump.WriteString(fmt.Sprintf("%04x | ", i))
		}
		formattedHexdump.WriteString(fmt.Sprintf("%02x ", b))

		// Add ASCII representation
		if b >= 32 && b <= 126 {
			asciiChars.WriteByte(b)
		} else {
			asciiChars.WriteByte('.')
		}
	}

	// Pad the last line if necessary
	if len(data)%16 != 0 {
		padding := 16 - (len(data) % 16)
		for i := 0; i < padding; i++ {
			formattedHexdump.WriteString("   ")
		}
	}

	// Add the final ASCII representation
	formattedHexdump.WriteString("| " + asciiChars.String())

	return formattedHexdump.String()
}

func getUnknownTopicRequest(correlationId int32) kafkaapi.ProduceRequest {
	// Create a simple record to produce
	record := kafkaapi.Record{
		Length:         0, // Will be calculated during encoding
		Attributes:     0,
		TimestampDelta: 0,
		OffsetDelta:    0,
		Key:            nil, // No key
		Value:          []byte("Hello from Kafka tester!"),
		Headers:        []kafkaapi.RecordHeader{},
	}

	// Create a record batch containing our record
	recordBatch := kafkaapi.RecordBatch{
		BaseOffset:           0,
		BatchLength:          0, // Will be calculated during encoding
		PartitionLeaderEpoch: -1,
		Magic:                2, // Use magic byte 2 for current format
		CRC:                  0, // Will be calculated during encoding
		Attributes:           0,
		LastOffsetDelta:      0,
		FirstTimestamp:       1718878078402,
		MaxTimestamp:         1718878078402,
		ProducerId:           0,
		ProducerEpoch:        0,
		BaseSequence:         0,
		Records:              []kafkaapi.Record{record},
	}

	request := kafkaapi.ProduceRequest{
		Header: kafkaapi.RequestHeader{
			ApiKey:        0,  // Produce API
			ApiVersion:    11, // Use version 11
			CorrelationId: correlationId,
			ClientId:      "kafka-tester",
		},
		Body: kafkaapi.ProduceRequestBody{
			TransactionalID: "", // Empty string for non-transactional
			Acks:            1,  // Wait for leader acknowledgment
			TimeoutMs:       5000,
			Topics: []kafkaapi.TopicData{
				{
					Name: UNKNOWN_TOPIC, // Try to produce to a test topic
					Partitions: []kafkaapi.PartitionData{
						{
							Index:   0,
							Records: []kafkaapi.RecordBatch{recordBatch},
						},
					},
				},
			},
		},
	}

	return request
}

func getWrongPartitionRequest(correlationId int32) kafkaapi.ProduceRequest {
	// Create a simple record to produce
	record := kafkaapi.Record{
		Length:         0, // Will be calculated during encoding
		Attributes:     0,
		TimestampDelta: 0,
		OffsetDelta:    0,
		Key:            nil, // No key
		Value:          []byte("Hello from wrong partition test!"),
		Headers:        []kafkaapi.RecordHeader{},
	}

	// Create a record batch containing our record
	recordBatch := kafkaapi.RecordBatch{
		BaseOffset:           0,
		BatchLength:          0, // Will be calculated during encoding
		PartitionLeaderEpoch: -1,
		Magic:                2, // Use magic byte 2 for current format
		CRC:                  0, // Will be calculated during encoding
		Attributes:           0,
		LastOffsetDelta:      0,
		FirstTimestamp:       1718878078402,
		MaxTimestamp:         1718878078402,
		ProducerId:           0,
		ProducerEpoch:        0,
		BaseSequence:         0,
		Records:              []kafkaapi.Record{record},
	}

	request := kafkaapi.ProduceRequest{
		Header: kafkaapi.RequestHeader{
			ApiKey:        0,  // Produce API
			ApiVersion:    11, // Use version 11
			CorrelationId: correlationId,
			ClientId:      "kafka-tester",
		},
		Body: kafkaapi.ProduceRequestBody{
			TransactionalID: "", // Empty string for non-transactional
			Acks:            1,  // Wait for leader acknowledgment
			TimeoutMs:       5000,
			Topics: []kafkaapi.TopicData{
				{
					Name: "ryanproduce", // Existing topic with 3 partitions (0, 1, 2)
					Partitions: []kafkaapi.PartitionData{
						{
							Index:   5, // Invalid partition - topic only has partitions 0, 1, 2
							Records: []kafkaapi.RecordBatch{recordBatch},
						},
					},
				},
			},
		},
	}

	return request
}

func getSuccessRequest(correlationId int32) kafkaapi.ProduceRequest {
	// Create a simple record to produce
	record := kafkaapi.Record{
		Attributes:     0,
		TimestampDelta: 0,
		Key:            nil,
		Value:          []byte("Hello Ryan"),
		Headers:        []kafkaapi.RecordHeader{},
	}

	// Create a record batch containing our record
	recordBatch := kafkaapi.RecordBatch{
		BaseOffset:           0,
		PartitionLeaderEpoch: -1,
		Attributes:           0,
		LastOffsetDelta:      0,
		FirstTimestamp:       time.Now().UnixMilli(), // Time stamps need to be different else data gets overwritten
		MaxTimestamp:         time.Now().UnixMilli(), // Time stamps need to be different else data gets overwritten
		ProducerId:           0,
		ProducerEpoch:        0,
		BaseSequence:         0, // Just need to update the base sequence for successive requests
		Records:              []kafkaapi.Record{record},
	}

	request := kafkaapi.ProduceRequest{
		Header: kafkaapi.RequestHeader{
			ApiKey:        0,  // Produce API
			ApiVersion:    11, // Use version 11
			CorrelationId: correlationId,
			ClientId:      "kafka-tester",
		},
		Body: kafkaapi.ProduceRequestBody{
			TransactionalID: "", // Empty string for non-transactional
			Acks:            1,  // Wait for leader acknowledgment
			TimeoutMs:       5000,
			Topics: []kafkaapi.TopicData{
				{
					Name: "ryanproduce", // Existing topic with 3 partitions (0, 1, 2)
					Partitions: []kafkaapi.PartitionData{
						{
							Index:   0, // Valid partition
							Records: []kafkaapi.RecordBatch{recordBatch},
						},
					},
				},
			},
		},
	}

	return request
}

// getPR5Request returns a Produce request with multiple records in a single batch (Stage PR5)
func getPR5Request(correlationId int32) kafkaapi.ProduceRequest {
	// Create multiple records for the batch
	record1 := kafkaapi.Record{
		Attributes:     0,
		TimestampDelta: 0,
		Key:            nil,
		Value:          []byte("Ryan's Message 4"),
		Headers:        []kafkaapi.RecordHeader{},
	}

	record2 := kafkaapi.Record{
		Attributes:     0,
		TimestampDelta: 0,
		Key:            nil,
		Value:          []byte("Ryan's Message 5"),
		Headers:        []kafkaapi.RecordHeader{},
	}

	record3 := kafkaapi.Record{
		Attributes:     0,
		TimestampDelta: 0,
		Key:            nil,
		Value:          []byte("Ryan's Message 6"),
		Headers:        []kafkaapi.RecordHeader{},
	}

	// Create a record batch containing multiple records
	recordBatch := kafkaapi.RecordBatch{
		BaseOffset:           0,
		PartitionLeaderEpoch: -1,
		Attributes:           0,
		LastOffsetDelta:      2, // Number of records - 1 (3 records: 0, 1, 2)
		FirstTimestamp:       time.Now().UnixMilli(),
		MaxTimestamp:         time.Now().UnixMilli(),
		ProducerId:           0,
		ProducerEpoch:        0,
		BaseSequence:         3, // Count of records sent in previous requests
		Records:              []kafkaapi.Record{record1, record2, record3},
	}

	request := kafkaapi.ProduceRequest{
		Header: kafkaapi.RequestHeader{
			ApiKey:        0,
			ApiVersion:    11,
			CorrelationId: correlationId,
			ClientId:      "kafka-tester",
		},
		Body: kafkaapi.ProduceRequestBody{
			TransactionalID: "", // Empty string for non-transactional
			Acks:            1,  // Wait for leader acknowledgment
			TimeoutMs:       5000,
			Topics: []kafkaapi.TopicData{
				{
					Name: "ryanproduce", // Existing topic with 3 partitions (0, 1, 2)
					Partitions: []kafkaapi.PartitionData{
						{
							Index:   0, // Valid partition
							Records: []kafkaapi.RecordBatch{recordBatch},
						},
					},
				},
			},
		},
	}

	return request
}

// getPR6MultiPartitionRequest returns a Produce request with multiple partitions (Stage PR6)
func getPR6MultiPartitionRequest(correlationId int32) kafkaapi.ProduceRequest {
	// Create records for partition 0
	record1P0 := kafkaapi.Record{
		Attributes:     0,
		TimestampDelta: 0,
		Key:            nil,
		Value:          []byte("Ryan's Message for partition 0"),
		Headers:        []kafkaapi.RecordHeader{},
	}

	recordBatch1 := kafkaapi.RecordBatch{
		BaseOffset:           0,
		PartitionLeaderEpoch: -1,
		Attributes:           0,
		LastOffsetDelta:      0, // Single record
		FirstTimestamp:       1718878078402,
		MaxTimestamp:         1718878078402,
		ProducerId:           0,
		ProducerEpoch:        0,
		BaseSequence:         0,
		Records:              []kafkaapi.Record{record1P0},
	}

	// Create records for partition 1
	record1P1 := kafkaapi.Record{
		Attributes:     0,
		TimestampDelta: 0,
		Key:            nil,
		Value:          []byte("Ryan's Message for partition 1"),
		Headers:        []kafkaapi.RecordHeader{},
	}

	recordBatch2 := kafkaapi.RecordBatch{
		BaseOffset:           0,
		PartitionLeaderEpoch: -1,
		Attributes:           0,
		LastOffsetDelta:      0, // Single record
		FirstTimestamp:       1718878078402,
		MaxTimestamp:         1718878078402,
		ProducerId:           0,
		ProducerEpoch:        0,
		BaseSequence:         0,
		Records:              []kafkaapi.Record{record1P1},
	}

	// Create records for partition 2
	record1P2 := kafkaapi.Record{
		Attributes:     0,
		TimestampDelta: 0,
		Key:            nil,
		Value:          []byte("Ryan's Message for partition 2"),
		Headers:        []kafkaapi.RecordHeader{},
	}

	recordBatch3 := kafkaapi.RecordBatch{
		BaseOffset:           0,
		PartitionLeaderEpoch: -1,
		Attributes:           0,
		LastOffsetDelta:      0, // Single record
		FirstTimestamp:       1718878078402,
		MaxTimestamp:         1718878078402,
		ProducerId:           0,
		ProducerEpoch:        0,
		BaseSequence:         0,
		Records:              []kafkaapi.Record{record1P2},
	}

	request := kafkaapi.ProduceRequest{
		Header: kafkaapi.RequestHeader{
			ApiKey:        0,
			ApiVersion:    11,
			CorrelationId: correlationId,
			ClientId:      "kafka-tester",
		},
		Body: kafkaapi.ProduceRequestBody{
			TransactionalID: "", // Empty string for non-transactional
			Acks:            1,  // Wait for leader acknowledgment
			TimeoutMs:       5000,
			Topics: []kafkaapi.TopicData{
				{
					Name: "ryanproduce", // Same topic but multiple partitions
					Partitions: []kafkaapi.PartitionData{
						{
							Index:   0, // Partition 0
							Records: []kafkaapi.RecordBatch{recordBatch1},
						},
						{
							Index:   1, // Partition 1
							Records: []kafkaapi.RecordBatch{recordBatch2},
						},
						{
							Index:   2, // Partition 2
							Records: []kafkaapi.RecordBatch{recordBatch3},
						},
					},
				},
			},
		},
	}

	return request
}

// getPR7Request returns a Produce request with multiple topics (Stage PR6)
// Multiple topics
func getPR7Request(correlationId int32) kafkaapi.ProduceRequest {
	// Create records for first topic
	record1Topic1 := kafkaapi.Record{
		Attributes:     0,
		TimestampDelta: 0,
		Key:            nil,
		Value:          []byte("Message for ryanproduce"),
		Headers:        []kafkaapi.RecordHeader{},
	}

	recordBatch1 := kafkaapi.RecordBatch{
		BaseOffset:           0,
		PartitionLeaderEpoch: -1,
		Attributes:           0,
		LastOffsetDelta:      0, // Single record
		FirstTimestamp:       1718878078402,
		MaxTimestamp:         1718878078402,
		ProducerId:           0,
		ProducerEpoch:        0,
		BaseSequence:         0,
		Records:              []kafkaapi.Record{record1Topic1},
	}

	// Create records for second topic
	record1Topic2 := kafkaapi.Record{
		Attributes:     0,
		TimestampDelta: 0,
		Key:            nil,
		Value:          []byte("Message for foo"),
		Headers:        []kafkaapi.RecordHeader{},
	}

	recordBatch2 := kafkaapi.RecordBatch{
		BaseOffset:           0,
		PartitionLeaderEpoch: -1,
		Attributes:           0,
		LastOffsetDelta:      0, // Single record
		FirstTimestamp:       1718878078402,
		MaxTimestamp:         1718878078402,
		ProducerId:           0,
		ProducerEpoch:        0,
		BaseSequence:         0,
		Records:              []kafkaapi.Record{record1Topic2},
	}

	request := kafkaapi.ProduceRequest{
		Header: kafkaapi.RequestHeader{
			ApiKey:        0,
			ApiVersion:    11,
			CorrelationId: correlationId,
			ClientId:      "kafka-tester",
		},
		Body: kafkaapi.ProduceRequestBody{
			TransactionalID: "", // Empty string for non-transactional
			Acks:            1,  // Wait for leader acknowledgment
			TimeoutMs:       5000,
			Topics: []kafkaapi.TopicData{
				{
					Name: "ryanproduce", // First topic
					Partitions: []kafkaapi.PartitionData{
						{
							Index:   0,
							Records: []kafkaapi.RecordBatch{recordBatch1},
						},
					},
				},
				{
					Name: "foo", // Second topic (used in existing tests)
					Partitions: []kafkaapi.PartitionData{
						{
							Index:   0,
							Records: []kafkaapi.RecordBatch{recordBatch2},
						},
					},
				},
			},
		},
	}

	return request
}
