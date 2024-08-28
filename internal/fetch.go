package internal

import (
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/codecrafters-io/kafka-tester/protocol"
	"github.com/codecrafters-io/kafka-tester/protocol/decoder"
	"github.com/codecrafters-io/kafka-tester/protocol/encoder"
)

type Partition struct {
	ID                int32
	FetchOffset       int64
	PartitionMaxBytes int32
}

type Topic struct {
	Name       string
	Partitions []Partition
}

type FetchRequest struct {
	ReplicaID int32
	MaxWaitMS int32
	MinBytes  int32
	Topics    []Topic
}

func fetchEncodev10() []byte {
	encoder := encoder.RealEncoder{}
	encoder.Init(make([]byte, 100))

	// Header
	request_api_key := int16(1)
	request_api_version := int16(10)
	correlation_id := int32(0)
	client_id := "sarama"

	encoder.PutInt16(request_api_key)
	encoder.PutInt16(request_api_version)
	encoder.PutInt32(correlation_id)
	encoder.PutString(client_id)

	// Body
	replica_id := int32(-1) // -1 for consumers
	max_wait_ms := int32(50000)
	min_bytes := int32(10)
	max_bytes := int32(10000)
	isolation_level := int8(0)
	fetch_session_id := int32(0)
	fetch_session_epoch := int32(0)

	encoder.PutInt32(replica_id)
	encoder.PutInt32(max_wait_ms)
	encoder.PutInt32(min_bytes)
	encoder.PutInt32(max_bytes)
	encoder.PutInt8(isolation_level)
	encoder.PutInt32(fetch_session_id)
	encoder.PutInt32(fetch_session_epoch)

	// Topics array
	topics := []struct {
		topic      string
		partitions []struct {
			partition          int32 // partition id
			currentLeaderEpoch int32 // current leader epoch
			fetchOffset        int64 // fetch offset
			logStartOffset     int64 // log start offset
			partitionMaxBytes  int32 // max bytes to fetch
		}
	}{
		{
			topic: "foo",
			partitions: []struct {
				partition          int32
				currentLeaderEpoch int32
				fetchOffset        int64
				logStartOffset     int64
				partitionMaxBytes  int32
			}{
				{
					partition:          0,
					currentLeaderEpoch: 0,
					fetchOffset:        0,
					logStartOffset:     0,
					partitionMaxBytes:  10000,
				},
			},
		},
	}

	// Encode topics array length
	encoder.PutInt32(int32(len(topics)))

	// Encode each topic
	for _, topic := range topics {
		if err := encoder.PutString(topic.topic); err != nil {
			return nil
		}

		// Encode partitions array length
		encoder.PutInt32(int32(len(topic.partitions)))

		// Encode each partition
		for _, partition := range topic.partitions {
			encoder.PutInt32(partition.partition)
			encoder.PutInt32(partition.currentLeaderEpoch)
			encoder.PutInt64(partition.fetchOffset)
			encoder.PutInt64(partition.logStartOffset)
			encoder.PutInt32(partition.partitionMaxBytes)
		}
	}

	encoder.PutInt32(int32(0))

	encoded := encoder.Bytes()
	encoded = encoded[:encoder.Offset()]
	length := int32(len(encoded))
	message := make([]byte, 4+len(encoded))
	binary.BigEndian.PutUint32(message[:4], uint32(length))
	copy(message[4:], encoded)

	return message
}

// Metadata response, get topic UUID
// ToDo: how to get UUID ? "82e9d296-c412-49f0-bcc9-b03cdcc4888a" ?
func fetchEncodev16() []byte {
	encoder := encoder.RealEncoder{}
	encoder.Init(make([]byte, 1000))

	// Header
	request_api_key := int16(1)
	request_api_version := int16(16)
	correlation_id := int32(11)
	client_id := "console-consumer"

	encoder.PutInt16(request_api_key)
	encoder.PutInt16(request_api_version)
	encoder.PutInt32(correlation_id)
	encoder.PutString(client_id)
	encoder.PutEmptyTaggedFieldArray()

	// Body
	max_wait_ms := int32(500)
	min_bytes := int32(1)
	max_bytes := int32(52428800)
	isolation_level := int8(0)
	fetch_session_id := int32(0)
	fetch_session_epoch := int32(0)

	encoder.PutInt32(max_wait_ms)
	encoder.PutInt32(min_bytes)
	encoder.PutInt32(max_bytes)
	encoder.PutInt8(isolation_level)
	encoder.PutInt32(fetch_session_id)
	encoder.PutInt32(fetch_session_epoch)

	// Topics array
	topics := []struct {
		topicUUID  string
		partitions []struct {
			partition          int32 // partition id
			currentLeaderEpoch int32 // current leader epoch
			fetchOffset        int64 // fetch offset
			lastFetchedOffset  int32 // last fetched offset
			logStartOffset     int64 // log start offset
			partitionMaxBytes  int32 // max bytes to fetch
		}
	}{
		{
			topicUUID: "82e9d296-c412-49f0-bcc9-b03cdcc4888a",
			partitions: []struct {
				partition          int32
				currentLeaderEpoch int32
				fetchOffset        int64
				lastFetchedOffset  int32
				logStartOffset     int64
				partitionMaxBytes  int32
			}{
				{
					partition:          0,
					currentLeaderEpoch: 0,
					fetchOffset:        0,
					lastFetchedOffset:  -1,
					logStartOffset:     -1,
					partitionMaxBytes:  1048576,
				},
			},
		},
	}

	// Encode topics array length
	// encoder.PutInt32(int32(len(topics)))
	// careful here, COMPACT_ARRAY: N+1
	// ToDo: Fix UVarInt, used here NOT INT8
	encoder.PutInt8(int8(len(topics) + 1))

	// Encode each topic
	for _, topic := range topics {
		uuid, err := encodeUUID(topic.topicUUID)
		if err != nil {
			return nil
		}

		if err := encoder.PutRawBytes(uuid); err != nil {
			return nil
		}

		// Encode partitions array length
		// encoder.PutInt32(int32(len(topic.partitions)))
		encoder.PutInt8(int8(len(topic.partitions) + 1))

		// Encode each partition
		for _, partition := range topic.partitions {
			encoder.PutInt32(partition.partition)
			encoder.PutInt32(partition.currentLeaderEpoch)
			encoder.PutInt64(partition.fetchOffset)
			encoder.PutInt32(partition.lastFetchedOffset)
			encoder.PutInt64(partition.logStartOffset)
			encoder.PutInt32(partition.partitionMaxBytes)
		}
	}

	encoder.PutEmptyTaggedFieldArray()

	encoder.PutEmptyTaggedFieldArray()

	// ToDo: what are these ?
	encoder.PutInt8(1)
	encoder.PutInt8(1)
	encoder.PutInt8(0)

	encoded := encoder.Bytes()
	encoded = encoded[:encoder.Offset()]
	length := int32(len(encoded))
	message := make([]byte, 4+len(encoded))
	binary.BigEndian.PutUint32(message[:4], uint32(length))
	copy(message[4:], encoded)

	return message
}

type FetchResponse struct {
	ThrottleTimeMs int32
	ErrorCode      int16
	SessionID      int32
	Responses      []TopicResponse
}

type TopicResponse struct {
	Topic      string
	Partitions []PartitionResponse
}

type PartitionResponse struct {
	PartitionIndex      int32
	ErrorCode           int16
	HighWatermark       int64
	LastStableOffset    int64
	LogStartOffset      int64
	AbortedTransactions []AbortedTransaction
	Records             []RecordBatch
}

type AbortedTransaction struct {
	ProducerID  int64
	FirstOffset int64
}

func fetchDecode(response []byte) (*FetchResponse, error) {
	decoder := decoder.RealDecoder{}
	decoder.Init(response)

	response_correlation_id, err := decoder.GetInt32()
	if err != nil {
		return nil, fmt.Errorf("failed to decode in func: %w", err)
	}
	fmt.Printf("Correlation ID: %d\n", response_correlation_id)

	decoder.GetEmptyTaggedFieldArray()

	throttle_time_ms, err := decoder.GetInt32()
	if err != nil {
		return nil, fmt.Errorf("failed to decode in func: %w", err)
	}
	fmt.Printf("Throttle time ms: %d\n", throttle_time_ms)

	error_code, err := decoder.GetInt16()
	if err != nil {
		return nil, fmt.Errorf("failed to decode in func: %w", err)
	}
	fmt.Printf("Error code: %d\n", error_code)

	session_id, err := decoder.GetInt32()
	if err != nil {
		return nil, fmt.Errorf("failed to decode in func: %w", err)
	}
	fmt.Printf("Session ID: %d\n", session_id)

	numResponses, err := decoder.GetCompactArrayLength()
	if err != nil {
		return nil, fmt.Errorf("failed to decode in func: %w", err)
	}
	fmt.Printf("Array length: %d\n", numResponses)

	var parsedResponse FetchResponse
	parsedResponse.Responses = make([]TopicResponse, numResponses)
	for i := range parsedResponse.Responses {
		topicUUIDBytes, err := decoder.GetRawBytes(16)
		if err != nil {
			return nil, fmt.Errorf("failed to decode in func: %w", err)
		}
		topicUUID, err := decodeUUID(topicUUIDBytes)
		if err != nil {
			return nil, fmt.Errorf("failed to decode in func: %w", err)
		}
		fmt.Printf("TopicUUID: %s\n", topicUUID)
		parsedResponse.Responses[i].Topic = topicUUID

		numPartitions, err := decoder.GetCompactArrayLength()
		if err != nil {
			return nil, fmt.Errorf("failed to decode in func: %w", err)
		}
		parsedResponse.Responses[i].Partitions = make([]PartitionResponse, numPartitions)

		for j := range parsedResponse.Responses[i].Partitions {
			partition := &parsedResponse.Responses[i].Partitions[j]

			partitionIndex, err := decoder.GetInt32()
			if err != nil {
				return nil, fmt.Errorf("failed to decode in func: %w", err)
			}
			parsedResponse.Responses[i].Partitions[j].PartitionIndex = partitionIndex
			fmt.Printf("Partition index: %d\n", partitionIndex)

			errorCode, err := decoder.GetInt16()
			if err != nil {
				return nil, fmt.Errorf("failed to decode in func: %w", err)
			}
			partition.ErrorCode = errorCode
			fmt.Printf("Error code: %d\n", errorCode)

			HighWatermark, err := decoder.GetInt64()
			if err != nil {
				return nil, fmt.Errorf("failed to decode in func: %w", err)
			}
			partition.HighWatermark = HighWatermark
			fmt.Printf("High watermark: %d\n", HighWatermark)

			lastStableOffset, err := decoder.GetInt64()
			if err != nil {
				return nil, fmt.Errorf("failed to decode in func: %w", err)
			}
			partition.LastStableOffset = lastStableOffset
			fmt.Printf("Last stable offset: %d\n", lastStableOffset)

			logStartOffset, err := decoder.GetInt64()
			if err != nil {
				return nil, fmt.Errorf("failed to decode in func: %w", err)
			}
			partition.LogStartOffset = logStartOffset
			fmt.Printf("Log start offset: %d\n", logStartOffset)

			numAbortedTransactions, err := decoder.GetCompactArrayLength()
			if err != nil {
				return nil, fmt.Errorf("failed to decode in func: %w", err)
			}
			fmt.Printf("Number of aborted transactions: %d\n", numAbortedTransactions)

			if numAbortedTransactions > 0 {
				partition.AbortedTransactions = make([]AbortedTransaction, numAbortedTransactions)
				for k := range partition.AbortedTransactions {
					producerID, err := decoder.GetInt64()
					if err != nil {
						return nil, fmt.Errorf("failed to decode in func: %w", err)
					}
					partition.AbortedTransactions[k].ProducerID = producerID

					firstOffset, err := decoder.GetInt64()
					if err != nil {
						return nil, fmt.Errorf("failed to decode in func: %w", err)
					}
					partition.AbortedTransactions[k].FirstOffset = firstOffset
				}
			}

			preferedReadReplica, err := decoder.GetInt32()
			if err != nil {
				return nil, fmt.Errorf("failed to decode in func: %w", err)
			}
			fmt.Printf("Prefered read replica: %d\n", preferedReadReplica)

			numBytes, err := decoder.GetUnsignedVarint()
			if err != nil {
				return nil, fmt.Errorf("failed to decode in func: %w", err)
			}
			fmt.Printf("Number of bytes: %d\n", numBytes-1)
			if numBytes-1 > 0 {
				for decoder.Remaining() > 10 { // TODO how to loop ?
					fmt.Printf("Starting decode record batch, remaining: %d\n", decoder.Remaining())
					recordBatch, err := fetchDecodeRecordBatch(&decoder)
					if err != nil {
						return nil, fmt.Errorf("failed to decode in func: %w", err)
					}
					fmt.Printf("Record batch: %v\n", recordBatch)
					partition.Records = append(partition.Records, *recordBatch)
				}
			}

			decoder.GetEmptyTaggedFieldArray()
		}
		decoder.GetEmptyTaggedFieldArray()
	}
	decoder.GetEmptyTaggedFieldArray()

	if decoder.Remaining() != 0 {
		return nil, fmt.Errorf("remaining bytes in decoder: %d", decoder.Remaining())
	}

	return &parsedResponse, nil
}

type RecordBatch struct {
	BaseOffset           int64
	BatchLength          int32
	PartitionLeaderEpoch int32
	Magic                int8
	CRC                  int32
	Attributes           int16
	LastOffsetDelta      int32
	FirstTimestamp       int64
	MaxTimestamp         int64
	ProducerId           int64
	ProducerEpoch        int16
	BaseSequence         int32
	Records              []Record
}

type Record struct {
	Length         int32
	Attributes     int8
	TimestampDelta int64
	OffsetDelta    int32
	Key            []byte
	Value          []byte
	Headers        []RecordHeader
}

type RecordHeader struct {
	Key   string
	Value []byte
}

func fetchDecodeRecord(decoder *decoder.RealDecoder) (*Record, error) {
	record := &Record{}

	length, err := decoder.GetUnsignedVarint()
	if err != nil {
		return nil, fmt.Errorf("failed to decode in func: %w", err)
	}
	record.Length = int32(length)
	fmt.Printf("Length: %d\n", length)

	attributes, err := decoder.GetInt8()
	if err != nil {
		return nil, fmt.Errorf("failed to decode in func: %w", err)
	}
	record.Attributes = attributes
	fmt.Printf("Attributes: %d\n", attributes)

	timestampDelta, err := decoder.GetSignedVarint()
	if err != nil {
		return nil, fmt.Errorf("failed to decode in func: %w", err)
	}
	record.TimestampDelta = timestampDelta
	fmt.Printf("Timestamp delta: %d\n", timestampDelta)

	offsetDelta, err := decoder.GetSignedVarint()
	if err != nil {
		return nil, fmt.Errorf("failed to decode in func: %w", err)
	}
	record.OffsetDelta = int32(offsetDelta)
	fmt.Printf("Offset delta: %d\n", offsetDelta)

	keyLength, err := decoder.GetSignedVarint()
	if err != nil {
		return nil, fmt.Errorf("failed to decode in func: %w", err)
	}
	fmt.Printf("Key length: %d\n", keyLength)

	var key []byte
	if keyLength > 0 {
		key, err = decoder.GetRawBytes(int(keyLength) - 1)
		if err != nil {
			return nil, fmt.Errorf("failed to decode in func: %w", err)
		}
		record.Key = key
		fmt.Printf("Key: %s\n", key)
	} else {
		record.Key = nil
		fmt.Printf("Key: <NULL>\n")
	}

	valueLength, err := decoder.GetSignedVarint()
	if err != nil {
		return nil, fmt.Errorf("failed to decode in func: %w", err)
	}
	fmt.Printf("Value length: %d\n", valueLength)

	value, err := decoder.GetRawBytes(int(valueLength) - 1) // XXX ?
	if err != nil {
		return nil, fmt.Errorf("failed to decode in func: %w", err)
	}
	record.Value = value
	fmt.Printf("Value: %s\n", value)

	decoder.GetEmptyTaggedFieldArray()

	return record, nil
}

func fetchDecodeRecordBatch(decoder *decoder.RealDecoder) (*RecordBatch, error) {
	batch := &RecordBatch{}

	baseOffset, err := decoder.GetInt64()
	if err != nil {
		return nil, fmt.Errorf("failed to decode in func: %w", err)
	}
	batch.BaseOffset = baseOffset
	fmt.Printf("Base offset: %d\n", baseOffset)

	batchLength, err := decoder.GetInt32()
	if err != nil {
		return nil, fmt.Errorf("failed to decode in func: %w", err)
	}
	batch.BatchLength = batchLength
	fmt.Printf("Batch length: %d\n", batchLength)

	partitionLeaderEpoch, err := decoder.GetInt32()
	if err != nil {
		return nil, fmt.Errorf("failed to decode in func: %w", err)
	}
	batch.PartitionLeaderEpoch = partitionLeaderEpoch
	fmt.Printf("Partition leader epoch: %d\n", partitionLeaderEpoch)

	magicByte, err := decoder.GetInt8()
	if err != nil {
		return nil, fmt.Errorf("failed to decode in func: %w", err)
	}
	batch.Magic = magicByte
	fmt.Printf("Magic: %d\n", magicByte)

	crc32, err := decoder.GetInt32()
	if err != nil {
		return nil, fmt.Errorf("failed to decode in func: %w", err)
	}
	batch.CRC = crc32
	fmt.Printf("CRC: %d\n", crc32)

	something, err := decoder.GetInt16()
	if err != nil {
		return nil, fmt.Errorf("failed to decode in func: %w", err)
	}
	fmt.Printf("Something: %d\n", something)

	lastOffsetDelta, err := decoder.GetInt32()
	if err != nil {
		return nil, fmt.Errorf("failed to decode in func: %w", err)
	}
	batch.LastOffsetDelta = lastOffsetDelta
	fmt.Printf("Last offset delta: %d\n", lastOffsetDelta)

	firstTimestamp, err := decoder.GetInt64()
	if err != nil {
		return nil, fmt.Errorf("failed to decode in func: %w", err)
	}
	batch.FirstTimestamp = firstTimestamp
	fmt.Printf("First timestamp: %d\n", firstTimestamp)

	lastTimestamp, err := decoder.GetInt64()
	if err != nil {
		return nil, fmt.Errorf("failed to decode in func: %w", err)
	}
	// batch.LastTimestamp = lastTimestamp
	fmt.Printf("Last timestamp: %d\n", lastTimestamp)

	producerId, err := decoder.GetInt64()
	if err != nil {
		return nil, fmt.Errorf("failed to decode in func: %w", err)
	}
	batch.ProducerId = producerId
	fmt.Printf("Producer ID: %d\n", producerId)

	producerEpoch, err := decoder.GetInt16()
	if err != nil {
		return nil, fmt.Errorf("failed to decode in func: %w", err)
	}
	batch.ProducerEpoch = producerEpoch
	fmt.Printf("Producer epoch: %d\n", producerEpoch)

	baseSequence, err := decoder.GetInt32()
	if err != nil {
		return nil, fmt.Errorf("failed to decode in func: %w", err)
	}
	batch.BaseSequence = baseSequence
	fmt.Printf("Base sequence: %d\n", baseSequence)

	sizeOfRecords, err := decoder.GetInt32()
	if err != nil {
		return nil, fmt.Errorf("failed to decode in func: %w", err)
	}
	// batch.SizeOfRecords = sizeOfRecords
	fmt.Printf("Size of records: %d\n", sizeOfRecords)
	if sizeOfRecords > 0 {
		record, err := fetchDecodeRecord(decoder)
		if err != nil {
			return nil, fmt.Errorf("failed to decode in func: %w", err)
		}
		fmt.Printf("Record: %v\n", record)
		batch.Records = append(batch.Records, *record)
	}

	fmt.Printf("RecordBatch: %v\n", batch)

	return batch, nil
}

func fetch(broker net.Conn) (*FetchResponse, error) {
	message := fetchEncodev16()
	broker.Write(message)

	lengthResponse := make([]byte, 4) // length
	broker.Read(lengthResponse)
	length := int32(binary.BigEndian.Uint32(lengthResponse))
	fmt.Printf("Length: %d\n", length)

	response := make([]byte, length)

	time.Sleep(100 * time.Millisecond)
	broker.Read(response)

	fmt.Printf("Hexdump of response:\n")
	for i, b := range append(lengthResponse, response...) {
		if i%16 == 0 {
			fmt.Printf("\n%04x  ", i)
		}
		fmt.Printf("%02x ", b)
	}
	fmt.Println()

	fetchResponse, err := fetchDecode(response)
	if err != nil {
		return nil, err
	}
	fmt.Println(fetchResponse.Responses[0].Partitions[0].Records)

	return nil, nil
}

func Fetch() {
	broker, err := protocol.Connect("localhost:9092")
	if err != nil {
		panic(err)
	}
	defer broker.Close()

	_, err = fetch(broker)
	if err != nil {
		panic(err)
	}
}

func encodeUUID(uuidString string) ([]byte, error) {
	// Remove any hyphens from the UUID string
	uuidString = strings.ReplaceAll(uuidString, "-", "")

	// Decode the hex string to bytes
	uuid, err := hex.DecodeString(uuidString)
	if err != nil {
		return nil, fmt.Errorf("invalid UUID string: %v", err)
	}

	// Check if the decoded bytes are exactly 16 bytes long
	if len(uuid) != 16 {
		return nil, fmt.Errorf("invalid UUID length: expected 16 bytes, got %d", len(uuid))
	}

	// The UUID is already in network byte order (big-endian),
	// so we don't need to do any byte order conversion

	return uuid, nil
}

func decodeUUID(encodedUUID []byte) (string, error) {
	// Check if the encoded UUID is exactly 16 bytes long
	if len(encodedUUID) != 16 {
		return "", fmt.Errorf("invalid UUID length: expected 16 bytes, got %d", len(encodedUUID))
	}

	// Convert the bytes to a hex string
	uuidHex := hex.EncodeToString(encodedUUID)

	// Insert hyphens to format the UUID string
	uuid := fmt.Sprintf("%s-%s-%s-%s-%s",
		uuidHex[0:8],
		uuidHex[8:12],
		uuidHex[12:16],
		uuidHex[16:20],
		uuidHex[20:32])

	return uuid, nil
}
