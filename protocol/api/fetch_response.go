package kafkaapi

import (
	"fmt"

	"github.com/codecrafters-io/kafka-tester/protocol/decoder"
	"github.com/codecrafters-io/kafka-tester/protocol/encoder"
	"github.com/codecrafters-io/kafka-tester/protocol/errors"
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
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return decodingErr.WithAddedContext("throttleTimeMs").WithAddedContext("FetchResponse")
		}
		return err
	}

	if r.ErrorCode, err = pd.GetInt16(); err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return decodingErr.WithAddedContext("errorCode").WithAddedContext("FetchResponse")
		}
		return err
	}

	if r.SessionID, err = pd.GetInt32(); err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return decodingErr.WithAddedContext("sessionID").WithAddedContext("FetchResponse")
		}
		return err
	}

	numResponses, err := pd.GetCompactArrayLength()
	if err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return decodingErr.WithAddedContext("numResponses").WithAddedContext("FetchResponse")
		}
		return err
	}

	r.Responses = make([]TopicResponse, numResponses)
	for i := range r.Responses {
		topicResponse := TopicResponse{}
		err := topicResponse.Decode(pd)
		if err != nil {
			if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
				return decodingErr.WithAddedContext(fmt.Sprintf("topicResponse[%d]", i)).WithAddedContext("FetchResponse")
			}
			return err
		}
		r.Responses[i] = topicResponse
	}
	_, err = pd.GetEmptyTaggedFieldArray()
	if err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return decodingErr.WithAddedContext("taggedFieldArray").WithAddedContext("FetchResponse")
		}
		return err
	}

	if pd.Remaining() != 0 {
		return errors.NewPacketDecodingError(fmt.Sprintf("unexpected %d bytes remaining in decoder after decoding FetchResponse", pd.Remaining()), "FetchResponse")
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
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return decodingErr.WithAddedContext("topicUUIDBytes")
		}
		return err
	}
	topicUUID, err := encoder.DecodeUUID(topicUUIDBytes)
	if err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return decodingErr.WithAddedContext("topicUUID").WithAddedContext("topicResponse")
		}
		return err
	}
	tr.Topic = topicUUID

	numPartitions, err := pd.GetCompactArrayLength()
	if err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return decodingErr.WithAddedContext("numPartitions")
		}
		return err
	}
	tr.Partitions = make([]PartitionResponse, numPartitions)

	for j := range tr.Partitions {
		partition := PartitionResponse{}
		err := partition.Decode(pd)
		if err != nil {
			if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
				return decodingErr.WithAddedContext(fmt.Sprintf("partitionResponse[%d]", j))
			}
			return err
		}
		tr.Partitions[j] = partition
	}
	// Add errors for all the EmptyTaggedFieldArray calls
	// ToDo
	_, err = pd.GetEmptyTaggedFieldArray()
	if err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return decodingErr.WithAddedContext("taggedFieldArray")
		}
		return err
	}

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
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return decodingErr.WithAddedContext("partitionIndex")
		}
		return err
	}

	if pr.ErrorCode, err = pd.GetInt16(); err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return decodingErr.WithAddedContext("errorCode")
		}
		return err
	}

	if pr.HighWatermark, err = pd.GetInt64(); err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return decodingErr.WithAddedContext("highWatermark")
		}
		return err
	}

	if pr.LastStableOffset, err = pd.GetInt64(); err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return decodingErr.WithAddedContext("lastStableOffset")
		}
		return err
	}

	if pr.LogStartOffset, err = pd.GetInt64(); err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return decodingErr.WithAddedContext("logStartOffset")
		}
		return err
	}

	numAbortedTransactions, err := pd.GetCompactArrayLength()
	if err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return decodingErr.WithAddedContext("numAbortedTransactions")
		}
		return err
	}

	if numAbortedTransactions > 0 {
		pr.AbortedTransactions = make([]AbortedTransaction, numAbortedTransactions)
		for k := range pr.AbortedTransactions {
			abortedTransaction := AbortedTransaction{}
			err := abortedTransaction.Decode(pd)
			if err != nil {
				if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
					return decodingErr.WithAddedContext(fmt.Sprintf("abortedTransaction[%d]", k))
				}
				return err
			}
			pr.AbortedTransactions[k] = abortedTransaction
		}
	}

	if pr.PreferedReadReplica, err = pd.GetInt32(); err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return decodingErr.WithAddedContext("preferedReadReplica")
		}
		return err
	}

	// Record Batches are encoded as COMPACT_RECORDS
	numBytes, err := pd.GetUnsignedVarint()
	if err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return decodingErr.WithAddedContext("compactRecordsLength")
		}
		return err
	}
	numBytes -= 1

	k := 0
	for numBytes > 0 && pd.Remaining() > 10 {
		recordBatch := RecordBatch{}
		err := recordBatch.Decode(pd)
		if err != nil {
			if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
				return decodingErr.WithAddedContext(fmt.Sprintf("recordBatch[%d]", k))
			}
			return err
		}
		pr.Records = append(pr.Records, recordBatch)
		k++
	}
	_, err = pd.GetEmptyTaggedFieldArray()
	if err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return decodingErr.WithAddedContext("taggedFieldArray")
		}
		return err
	}

	return nil
}

type AbortedTransaction struct {
	ProducerID  int64
	FirstOffset int64
}

func (ab *AbortedTransaction) Decode(pd *decoder.RealDecoder) (err error) {
	if ab.ProducerID, err = pd.GetInt64(); err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return decodingErr.WithAddedContext("producerID").WithAddedContext("abortedTransaction")
		}
		return err
	}

	if ab.FirstOffset, err = pd.GetInt64(); err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return decodingErr.WithAddedContext("firstOffset").WithAddedContext("abortedTransaction")
		}
		return err
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
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return decodingErr.WithAddedContext("baseOffset")
		}
		return err
	}

	if rb.BatchLength, err = pd.GetInt32(); err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return decodingErr.WithAddedContext("batchLength")
		}
		return err
	}

	if rb.PartitionLeaderEpoch, err = pd.GetInt32(); err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return decodingErr.WithAddedContext("partitionLeaderEpoch")
		}
		return err
	}

	if rb.Magic, err = pd.GetInt8(); err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return decodingErr.WithAddedContext("magicByte")
		}
		return err
	}

	if rb.CRC, err = pd.GetInt32(); err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return decodingErr.WithAddedContext("CRC")
		}
		return err
	}
	// TODO: validate CRC

	if rb.Attributes, err = pd.GetInt16(); err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return decodingErr.WithAddedContext("recordAttributes")
		}
		return err
	}

	if rb.LastOffsetDelta, err = pd.GetInt32(); err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return decodingErr.WithAddedContext("lastOffsetDelta")
		}
		return err
	}

	if rb.FirstTimestamp, err = pd.GetInt64(); err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return decodingErr.WithAddedContext("firstTimestamp")
		}
		return err
	}

	if rb.MaxTimestamp, err = pd.GetInt64(); err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return decodingErr.WithAddedContext("maxTimestamp")
		}
		return err
	}

	if rb.ProducerId, err = pd.GetInt64(); err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return decodingErr.WithAddedContext("producerId")
		}
		return err
	}

	if rb.ProducerEpoch, err = pd.GetInt16(); err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return decodingErr.WithAddedContext("producerEpoch")
		}
		return err
	}

	if rb.BaseSequence, err = pd.GetInt32(); err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return decodingErr.WithAddedContext("baseSequence")
		}
		return err
	}

	numRecords, err := pd.GetInt32()
	if err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return decodingErr.WithAddedContext("numRecords")
		}
		return err
	}

	for i := 0; i < int(numRecords); i++ {
		record := Record{}
		err := record.Decode(pd)
		if err != nil {
			if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
				return decodingErr.WithAddedContext(fmt.Sprintf("record[%d]", i))
			}
			return err
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
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return decodingErr.WithAddedContext("length")
		}
		return err
	}
	r.Length = int32(length)

	if r.Attributes, err = pd.GetInt8(); err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return decodingErr.WithAddedContext("attributes")
		}
		return err
	}

	if r.TimestampDelta, err = pd.GetSignedVarint(); err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return decodingErr.WithAddedContext("timestampDelta")
		}
		return err
	}

	offsetDelta, err := pd.GetSignedVarint()
	if err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return decodingErr.WithAddedContext("offsetDelta")
		}
		return err
	}
	r.OffsetDelta = int32(offsetDelta)

	keyLength, err := pd.GetSignedVarint()
	if err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return decodingErr.WithAddedContext("keyLength")
		}
		return err
	}

	var key []byte
	if keyLength > 0 {
		key, err = pd.GetRawBytes(int(keyLength) - 1)
		if err != nil {
			if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
				return decodingErr.WithAddedContext("messageKey")
			}
			return err
		}
		r.Key = key
	} else {
		r.Key = nil
	}

	valueLength, err := pd.GetSignedVarint()
	if err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return decodingErr.WithAddedContext("valueLength")
		}
		return err
	}

	value, err := pd.GetRawBytes(int(valueLength))
	if err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return decodingErr.WithAddedContext("messageValue")
		}
		return err
	}
	r.Value = value

	_, err = pd.GetEmptyTaggedFieldArray()
	if err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return decodingErr.WithAddedContext("taggedFieldArray")
		}
		return err
	}

	return nil
}

type RecordHeader struct {
	Key   string
	Value []byte
}
