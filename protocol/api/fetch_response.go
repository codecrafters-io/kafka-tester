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

	if pr.ErrorCode, err = pd.GetInt16(); err != nil {
		return fmt.Errorf("failed to decode in func: %w", err)
	}

	if pr.HighWatermark, err = pd.GetInt64(); err != nil {
		return fmt.Errorf("failed to decode in func: %w", err)
	}

	if pr.LastStableOffset, err = pd.GetInt64(); err != nil {
		return fmt.Errorf("failed to decode in func: %w", err)
	}

	if pr.LogStartOffset, err = pd.GetInt64(); err != nil {
		return fmt.Errorf("failed to decode in func: %w", err)
	}

	numAbortedTransactions, err := pd.GetCompactArrayLength()
	if err != nil {
		return fmt.Errorf("failed to decode in func: %w", err)
	}

	if numAbortedTransactions > 0 {
		pr.AbortedTransactions = make([]AbortedTransaction, numAbortedTransactions)
		for k := range pr.AbortedTransactions {
			abortedTransaction := AbortedTransaction{}
			err := abortedTransaction.Decode(pd)
			if err != nil {
				return fmt.Errorf("failed to decode aborted transaction in partition response: %w", err)
			}
			pr.AbortedTransactions[k] = abortedTransaction
		}
	}

	if pr.PreferedReadReplica, err = pd.GetInt32(); err != nil {
		return fmt.Errorf("failed to decode in func: %w", err)
	}

	// Record Batches are encoded as COMPACT_RECORDS
	numBytes, err := pd.GetUnsignedVarint()
	if err != nil {
		return fmt.Errorf("failed to decode in func: %w", err)
	}
	numBytes -= 1

	for numBytes > 0 && pd.Remaining() > 10 {
		recordBatch := RecordBatch{}
		err := recordBatch.Decode(pd)
		if err != nil {
			return fmt.Errorf("failed to decode in func: %w", err)
		}
		pr.Records = append(pr.Records, recordBatch)
	}
	pd.GetEmptyTaggedFieldArray()

	return nil
}

type AbortedTransaction struct {
	ProducerID  int64
	FirstOffset int64
}

func (ab *AbortedTransaction) Decode(pd *decoder.RealDecoder) (err error) {
	if ab.ProducerID, err = pd.GetInt64(); err != nil {
		return fmt.Errorf("failed to decode producerID in aborted transaction: %w", err)
	}

	if ab.FirstOffset, err = pd.GetInt64(); err != nil {
		return fmt.Errorf("failed to decode firstOffset in aborted transaction: %w", err)
	}

	return nil
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

	if rb.BatchLength, err = pd.GetInt32(); err != nil {
		return fmt.Errorf("failed to decode batchLength in record batch: %w", err)
	}

	if rb.PartitionLeaderEpoch, err = pd.GetInt32(); err != nil {
		return fmt.Errorf("failed to decode partitionLeaderEpoch in record batch: %w", err)
	}

	if rb.Magic, err = pd.GetInt8(); err != nil {
		return fmt.Errorf("failed to decode magicByte in record batch: %w", err)
	}

	if rb.CRC, err = pd.GetInt32(); err != nil {
		return fmt.Errorf("failed to decode CRC in record batch: %w", err)
	}
	// TODO: validate CRC

	if rb.Attributes, err = pd.GetInt16(); err != nil {
		return fmt.Errorf("failed to decode attributes in record batch: %w", err)
	}

	if rb.LastOffsetDelta, err = pd.GetInt32(); err != nil {
		return fmt.Errorf("failed to decode lastOffsetDelta in record batch: %w", err)
	}

	if rb.FirstTimestamp, err = pd.GetInt64(); err != nil {
		return fmt.Errorf("failed to decode firstTimestamp in record batch: %w", err)
	}

	if rb.MaxTimestamp, err = pd.GetInt64(); err != nil {
		return fmt.Errorf("failed to decode maxTimestamp in record batch: %w", err)
	}

	if rb.ProducerId, err = pd.GetInt64(); err != nil {
		return fmt.Errorf("failed to decode producerId in record batch: %w", err)
	}

	if rb.ProducerEpoch, err = pd.GetInt16(); err != nil {
		return fmt.Errorf("failed to decode producerEpoch in record batch: %w", err)
	}

	if rb.BaseSequence, err = pd.GetInt32(); err != nil {
		return fmt.Errorf("failed to decode baseSequence in record batch: %w", err)
	}

	numRecords, err := pd.GetInt32()
	if err != nil {
		return fmt.Errorf("failed to decode numRecords in record batch: %w", err)
	}

	for i := 0; i < int(numRecords); i++ {
		record := Record{}
		err := record.Decode(pd)
		if err != nil {
			return fmt.Errorf("failed to decode record in record batch: %w", err)
		}
		rb.Records = append(rb.Records, record)
	}

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

	if r.Attributes, err = pd.GetInt8(); err != nil {
		return fmt.Errorf("failed to decode attributes in record: %w", err)
	}

	if r.TimestampDelta, err = pd.GetSignedVarint(); err != nil {
		return fmt.Errorf("failed to decode timestampDelta in record: %w", err)
	}

	offsetDelta, err := pd.GetSignedVarint()
	if err != nil {
		return fmt.Errorf("failed to decode offsetDelta in record: %w", err)
	}
	r.OffsetDelta = int32(offsetDelta)

	keyLength, err := pd.GetSignedVarint()
	if err != nil {
		return fmt.Errorf("failed to decode keyLength in record: %w", err)
	}

	var key []byte
	if keyLength > 0 {
		key, err = pd.GetRawBytes(int(keyLength) - 1)
		if err != nil {
			return fmt.Errorf("failed to decode key in record: %w", err)
		}
		r.Key = key
	} else {
		r.Key = nil
	}

	valueLength, err := pd.GetSignedVarint()
	if err != nil {
		return fmt.Errorf("failed to decode valueLength in record: %w", err)
	}

	value, err := pd.GetRawBytes(int(valueLength))
	if err != nil {
		return fmt.Errorf("failed to decode value in record: %w", err)
	}
	r.Value = value

	pd.GetEmptyTaggedFieldArray()

	return nil
}

type RecordHeader struct {
	Key   string
	Value []byte
}
