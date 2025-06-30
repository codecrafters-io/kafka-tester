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
	request := getSuccessRequest(correlationId)

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
		PartitionLeaderEpoch: 0,
		Magic:                2, // Use magic byte 2 for current format
		CRC:                  0, // Will be calculated during encoding
		Attributes:           0,
		LastOffsetDelta:      0,
		FirstTimestamp:       time.Now().UnixMilli(),
		MaxTimestamp:         time.Now().UnixMilli(),
		ProducerId:           -1,
		ProducerEpoch:        -1,
		BaseSequence:         -1,
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
		PartitionLeaderEpoch: 0,
		Magic:                2, // Use magic byte 2 for current format
		CRC:                  0, // Will be calculated during encoding
		Attributes:           0,
		LastOffsetDelta:      0,
		FirstTimestamp:       time.Now().UnixMilli(),
		MaxTimestamp:         time.Now().UnixMilli(),
		ProducerId:           -1,
		ProducerEpoch:        -1,
		BaseSequence:         -1,
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
		Length:         0, // Will be calculated during encoding
		Attributes:     0,
		TimestampDelta: 0,
		OffsetDelta:    0,
		Key:            nil, // No key
		Value:          []byte("Hello Ryan"),
		Headers:        []kafkaapi.RecordHeader{},
	}

	// Create a record batch containing our record
	recordBatch := kafkaapi.RecordBatch{
		BaseOffset:           0,
		BatchLength:          0, // Will be calculated during encoding
		PartitionLeaderEpoch: 0,
		Magic:                2, // Use magic byte 2 for current format
		CRC:                  0, // Will be calculated during encoding
		Attributes:           0,
		LastOffsetDelta:      0,
		FirstTimestamp:       time.Now().UnixMilli(),
		MaxTimestamp:         time.Now().UnixMilli(),
		ProducerId:           -1,
		ProducerEpoch:        -1,
		BaseSequence:         -1,
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
