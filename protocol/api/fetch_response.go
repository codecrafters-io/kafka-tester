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

func (tr *TopicResponse) Decode(pd *decoder.RealDecoder) (err error) {
	topicUUIDBytes, err := pd.GetRawBytes(16)
	if err != nil {
		return fmt.Errorf("failed to decode topicUUID in topic response: %w", err)
	}
	topicUUID, err := encoder.DecodeUUID(topicUUIDBytes)
	if err != nil {
		return fmt.Errorf("failed to decode in func: %w", err)
	}
	fmt.Printf("TopicUUID: %s\n", topicUUID)
	tr.Topic = topicUUID

	numPartitions, err := pd.GetCompactArrayLength()
	if err != nil {
		return fmt.Errorf("failed to decode in func: %w", err)
	}
	tr.Partitions = make([]PartitionResponse, numPartitions)
	// partition := &tr.Partitions[j]

	for j := range tr.Partitions {
		partition := PartitionResponse{}
		err := partition.Decode(pd)
		if err != nil {
			return fmt.Errorf("failed to decode partition response in topic response: %w", err)
		}
		tr.Partitions[j] = partition
	}
	pd.GetEmptyTaggedFieldArray()

	return nil
}

type PartitionResponse struct {
	PartitionIndex      int32
	ErrorCode           int16
	HighWatermark       int64
	LastStableOffset    int64
	LogStartOffset      int64
	AbortedTransactions []AbortedTransaction
	Records             []RecordBatch
	PreferedReadReplica int32
}

func (pr *PartitionResponse) Decode(pd *decoder.RealDecoder) (err error) {
	if pr.PartitionIndex, err = pd.GetInt32(); err != nil {
		return fmt.Errorf("failed to decode in func: %w", err)
	}
	fmt.Printf("Partition index: %d\n", pr.PartitionIndex)

	if pr.ErrorCode, err = pd.GetInt16(); err != nil {
		return fmt.Errorf("failed to decode in func: %w", err)
	}
	fmt.Printf("Error code: %d\n", pr.ErrorCode)

	if pr.HighWatermark, err = pd.GetInt64(); err != nil {
		return fmt.Errorf("failed to decode in func: %w", err)
	}
	fmt.Printf("High watermark: %d\n", pr.HighWatermark)

	if pr.LastStableOffset, err = pd.GetInt64(); err != nil {
		return fmt.Errorf("failed to decode in func: %w", err)
	}
	fmt.Printf("Last stable offset: %d\n", pr.LastStableOffset)

	if pr.LogStartOffset, err = pd.GetInt64(); err != nil {
		return fmt.Errorf("failed to decode in func: %w", err)
	}
	fmt.Printf("Log start offset: %d\n", pr.LogStartOffset)

	numAbortedTransactions, err := pd.GetCompactArrayLength()
	if err != nil {
		return fmt.Errorf("failed to decode in func: %w", err)
	}
	fmt.Printf("Number of aborted transactions: %d\n", numAbortedTransactions)

	if numAbortedTransactions > 0 {
		pr.AbortedTransactions = make([]AbortedTransaction, numAbortedTransactions)
		for k := range pr.AbortedTransactions {
			producerID, err := pd.GetInt64()
			if err != nil {
				return fmt.Errorf("failed to decode in func: %w", err)
			}
			pr.AbortedTransactions[k].ProducerID = producerID

			firstOffset, err := pd.GetInt64()
			if err != nil {
				return fmt.Errorf("failed to decode in func: %w", err)
			}
			pr.AbortedTransactions[k].FirstOffset = firstOffset
		}
	}

	if pr.PreferedReadReplica, err = pd.GetInt32(); err != nil {
		return fmt.Errorf("failed to decode in func: %w", err)
	}
	fmt.Printf("Prefered read replica: %d\n", pr.PreferedReadReplica)

	// Record Batches are encoded as COMPACT_RECORDS
	numBytes, err := pd.GetUnsignedVarint()
	if err != nil {
		return fmt.Errorf("failed to decode in func: %w", err)
	}
	numBytes -= 1
	fmt.Printf("Number of bytes: %d\n", numBytes)

	for numBytes > 0 && pd.Remaining() > 10 {
		fmt.Printf("Starting decode record batch, remaining: %d\n", pd.Remaining())
		recordBatch := RecordBatch{}
		err := recordBatch.Decode(pd)
		if err != nil {
			return fmt.Errorf("failed to decode in func: %w", err)
		}
		fmt.Printf("Record batch: %v\n", recordBatch)
		pr.Records = append(pr.Records, recordBatch)
	}
	pd.GetEmptyTaggedFieldArray()

	return nil
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

func (rb *RecordBatch) Decode(pd *decoder.RealDecoder) (err error) {
	if rb.BaseOffset, err = pd.GetInt64(); err != nil {
		return fmt.Errorf("failed to decode baseOffset in record batch: %w", err)
	}
	fmt.Printf("Base offset: %d\n", rb.BaseOffset)

	if rb.BatchLength, err = pd.GetInt32(); err != nil {
		return fmt.Errorf("failed to decode batchLength in record batch: %w", err)
	}
	fmt.Printf("Batch length: %d\n", rb.BatchLength)

	if rb.PartitionLeaderEpoch, err = pd.GetInt32(); err != nil {
		return fmt.Errorf("failed to decode partitionLeaderEpoch in record batch: %w", err)
	}
	fmt.Printf("Partition leader epoch: %d\n", rb.PartitionLeaderEpoch)

	if rb.Magic, err = pd.GetInt8(); err != nil {
		return fmt.Errorf("failed to decode magicByte in record batch: %w", err)
	}
	fmt.Printf("Magic: %d\n", rb.Magic)

	if rb.CRC, err = pd.GetInt32(); err != nil {
		return fmt.Errorf("failed to decode CRC in record batch: %w", err)
	}
	// TODO: validate CRC
	fmt.Printf("CRC: %x\n", rb.CRC)

	if rb.Attributes, err = pd.GetInt16(); err != nil {
		return fmt.Errorf("failed to decode attributes in record batch: %w", err)
	}
	fmt.Printf("Attributes: %d\n", rb.Attributes)

	if rb.LastOffsetDelta, err = pd.GetInt32(); err != nil {
		return fmt.Errorf("failed to decode lastOffsetDelta in record batch: %w", err)
	}
	fmt.Printf("Last offset delta: %d\n", rb.LastOffsetDelta)

	if rb.FirstTimestamp, err = pd.GetInt64(); err != nil {
		return fmt.Errorf("failed to decode firstTimestamp in record batch: %w", err)
	}
	fmt.Printf("First timestamp: %d\n", rb.FirstTimestamp)

	if rb.MaxTimestamp, err = pd.GetInt64(); err != nil {
		return fmt.Errorf("failed to decode maxTimestamp in record batch: %w", err)
	}
	fmt.Printf("Last timestamp: %d\n", rb.MaxTimestamp)

	if rb.ProducerId, err = pd.GetInt64(); err != nil {
		return fmt.Errorf("failed to decode producerId in record batch: %w", err)
	}
	fmt.Printf("Producer ID: %d\n", rb.ProducerId)

	if rb.ProducerEpoch, err = pd.GetInt16(); err != nil {
		return fmt.Errorf("failed to decode producerEpoch in record batch: %w", err)
	}
	fmt.Printf("Producer epoch: %d\n", rb.ProducerEpoch)

	if rb.BaseSequence, err = pd.GetInt32(); err != nil {
		return fmt.Errorf("failed to decode baseSequence in record batch: %w", err)
	}
	fmt.Printf("Base sequence: %d\n", rb.BaseSequence)

	numRecords, err := pd.GetInt32()
	if err != nil {
		return fmt.Errorf("failed to decode numRecords in record batch: %w", err)
	}
	fmt.Printf("Number of records: %d\n", numRecords)

	for i := 0; i < int(numRecords); i++ {
		record := Record{}
		err := record.Decode(pd)
		if err != nil {
			return fmt.Errorf("failed to decode record in record batch: %w", err)
		}
		fmt.Printf("Record: %v\n", record)
		rb.Records = append(rb.Records, record)
	}

	fmt.Printf("RecordBatch: %v\n", rb)

	return nil
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

func (r *Record) Decode(pd *decoder.RealDecoder) (err error) {
	length, err := pd.GetUnsignedVarint()
	if err != nil {
		return fmt.Errorf("failed to decode length in record: %w", err)
	}
	r.Length = int32(length)
	fmt.Printf("Length: %d\n", r.Length)

	if r.Attributes, err = pd.GetInt8(); err != nil {
		return fmt.Errorf("failed to decode attributes in record: %w", err)
	}
	fmt.Printf("Attributes: %d\n", r.Attributes)

	if r.TimestampDelta, err = pd.GetSignedVarint(); err != nil {
		return fmt.Errorf("failed to decode timestampDelta in record: %w", err)
	}
	fmt.Printf("Timestamp delta: %d\n", r.TimestampDelta)

	offsetDelta, err := pd.GetSignedVarint()
	if err != nil {
		return fmt.Errorf("failed to decode offsetDelta in record: %w", err)
	}
	r.OffsetDelta = int32(offsetDelta)
	fmt.Printf("Offset delta: %d\n", r.OffsetDelta)

	keyLength, err := pd.GetSignedVarint()
	if err != nil {
		return fmt.Errorf("failed to decode keyLength in record: %w", err)
	}
	fmt.Printf("Key length: %d\n", keyLength)

	var key []byte
	if keyLength > 0 {
		key, err = pd.GetRawBytes(int(keyLength) - 1)
		if err != nil {
			return fmt.Errorf("failed to decode key in record: %w", err)
		}
		r.Key = key
		fmt.Printf("Key: %s\n", key)
	} else {
		r.Key = nil
		fmt.Printf("Key: <NULL>\n")
	}

	valueLength, err := pd.GetSignedVarint()
	if err != nil {
		return fmt.Errorf("failed to decode valueLength in record: %w", err)
	}
	fmt.Printf("Value length: %d\n", valueLength)

	value, err := pd.GetRawBytes(int(valueLength))
	if err != nil {
		return fmt.Errorf("failed to decode value in record: %w", err)
	}
	r.Value = value
	fmt.Printf("Value: %s\n", value)

	pd.GetEmptyTaggedFieldArray()

	return nil
}

type RecordHeader struct {
	Key   string
	Value []byte
}

func (r *FetchResponse) Decode(pd *decoder.RealDecoder, version int16) (err error) {
	fmt.Printf("Decoding fetch response using protocol/api/fetch_response.go\n")
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
		topicResponse := TopicResponse{}
		err := topicResponse.Decode(pd)
		if err != nil {
			return fmt.Errorf("failed to decode topic response in fetch response: %w", err)
		}
		r.Responses[i] = topicResponse
	}
	pd.GetEmptyTaggedFieldArray()

	if pd.Remaining() != 0 {
		return fmt.Errorf("remaining bytes in decoder: %d", pd.Remaining())
	}

	return nil
}
