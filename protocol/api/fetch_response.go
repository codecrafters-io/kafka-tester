package kafkaapi

import (
	"fmt"

	"github.com/codecrafters-io/kafka-tester/protocol/decoder"
	"github.com/codecrafters-io/kafka-tester/protocol/encoder"
)

type FetchResponse struct {
	Version        int16
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

func (r *FetchResponse) Decode(pd *decoder.RealDecoder, version int16) (err error) {
	r.Version = version

	if r.ThrottleTimeMs, err = pd.GetInt32(); err != nil {
		return fmt.Errorf("failed to decode throttleTimeMs in fetch response: %w", err)
	}

	if r.ErrorCode, err = pd.GetInt16(); err != nil {
		return fmt.Errorf("failed to decode errorCode in fetch response: %w", err)
	}

	if r.SessionID, err = pd.GetInt32(); err != nil {
		return fmt.Errorf("failed to decode sessionID in fetch response: %w", err)
	}

	numResponses, err := pd.GetCompactArrayLength()
	if err != nil {
		return fmt.Errorf("failed to decode numResponses in fetch response: %w", err)
	}

	r.Responses = make([]TopicResponse, numResponses)
	for i := range r.Responses {
		topicUUIDBytes, err := pd.GetRawBytes(16)
		if err != nil {
			return fmt.Errorf("failed to decode in func: %w", err)
		}
		topicUUID, err := encoder.DecodeUUID(topicUUIDBytes)
		if err != nil {
			return fmt.Errorf("failed to decode in func: %w", err)
		}
		fmt.Printf("TopicUUID: %s\n", topicUUID)
		r.Responses[i].Topic = topicUUID

		numPartitions, err := pd.GetCompactArrayLength()
		if err != nil {
			return fmt.Errorf("failed to decode in func: %w", err)
		}
		r.Responses[i].Partitions = make([]PartitionResponse, numPartitions)

		for j := range r.Responses[i].Partitions {
			partition := &r.Responses[i].Partitions[j]

			partitionIndex, err := pd.GetInt32()
			if err != nil {
				return fmt.Errorf("failed to decode in func: %w", err)
			}
			r.Responses[i].Partitions[j].PartitionIndex = partitionIndex
			fmt.Printf("Partition index: %d\n", partitionIndex)

			errorCode, err := pd.GetInt16()
			if err != nil {
				return fmt.Errorf("failed to decode in func: %w", err)
			}
			partition.ErrorCode = errorCode
			fmt.Printf("Error code: %d\n", errorCode)

			HighWatermark, err := pd.GetInt64()
			if err != nil {
				return fmt.Errorf("failed to decode in func: %w", err)
			}
			partition.HighWatermark = HighWatermark
			fmt.Printf("High watermark: %d\n", HighWatermark)

			lastStableOffset, err := pd.GetInt64()
			if err != nil {
				return fmt.Errorf("failed to decode in func: %w", err)
			}
			partition.LastStableOffset = lastStableOffset
			fmt.Printf("Last stable offset: %d\n", lastStableOffset)

			logStartOffset, err := pd.GetInt64()
			if err != nil {
				return fmt.Errorf("failed to decode in func: %w", err)
			}
			partition.LogStartOffset = logStartOffset
			fmt.Printf("Log start offset: %d\n", logStartOffset)

			numAbortedTransactions, err := pd.GetCompactArrayLength()
			if err != nil {
				return fmt.Errorf("failed to decode in func: %w", err)
			}
			fmt.Printf("Number of aborted transactions: %d\n", numAbortedTransactions)

			if numAbortedTransactions > 0 {
				partition.AbortedTransactions = make([]AbortedTransaction, numAbortedTransactions)
				for k := range partition.AbortedTransactions {
					producerID, err := pd.GetInt64()
					if err != nil {
						return fmt.Errorf("failed to decode in func: %w", err)
					}
					partition.AbortedTransactions[k].ProducerID = producerID

					firstOffset, err := pd.GetInt64()
					if err != nil {
						return fmt.Errorf("failed to decode in func: %w", err)
					}
					partition.AbortedTransactions[k].FirstOffset = firstOffset
				}
			}

			preferedReadReplica, err := pd.GetInt32()
			if err != nil {
				return fmt.Errorf("failed to decode in func: %w", err)
			}
			fmt.Printf("Prefered read replica: %d\n", preferedReadReplica)

			numBytes, err := pd.GetUnsignedVarint()
			if err != nil {
				return fmt.Errorf("failed to decode in func: %w", err)
			}
			fmt.Printf("Number of bytes: %d\n", numBytes-1)
			if numBytes-1 > 0 {
				for pd.Remaining() > 10 { // TODO how to loop ?
					fmt.Printf("Starting decode record batch, remaining: %d\n", pd.Remaining())
					recordBatch, err := fetchDecodeRecordBatch(pd)
					if err != nil {
						return fmt.Errorf("failed to decode in func: %w", err)
					}
					fmt.Printf("Record batch: %v\n", recordBatch)
					partition.Records = append(partition.Records, *recordBatch)
				}
			}

			pd.GetEmptyTaggedFieldArray()
		}
		pd.GetEmptyTaggedFieldArray()
	}
	pd.GetEmptyTaggedFieldArray()

	if pd.Remaining() != 0 {
		return fmt.Errorf("remaining bytes in decoder: %d", pd.Remaining())
	}

	return nil
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

	value, err := decoder.GetRawBytes(int(valueLength) - 1)
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
