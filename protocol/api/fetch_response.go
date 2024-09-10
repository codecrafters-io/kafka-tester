package kafkaapi

import (
	"fmt"
	"hash/crc32"

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
			return decodingErr.WithAddedContext("throttle_time_ms")
		}
		return err
	}

	if r.ErrorCode, err = pd.GetInt16(); err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return decodingErr.WithAddedContext("error_code")
		}
		return err
	}

	if r.SessionID, err = pd.GetInt32(); err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return decodingErr.WithAddedContext("session_id")
		}
		return err
	}

	numResponses, err := pd.GetCompactArrayLength()
	if err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return decodingErr.WithAddedContext("num_responses")
		}
		return err
	}

	r.Responses = make([]TopicResponse, numResponses)
	for i := range r.Responses {
		topicResponse := TopicResponse{}
		err := topicResponse.Decode(pd)
		if err != nil {
			if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
				return decodingErr.WithAddedContext(fmt.Sprintf("TopicResponse[%d]", i))
			}
			return err
		}
		r.Responses[i] = topicResponse
	}
	_, err = pd.GetEmptyTaggedFieldArray()
	if err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return decodingErr.WithAddedContext("TAG_BUFFER")
		}
		return err
	}

	if pd.Remaining() != 0 {
		return errors.NewPacketDecodingError(fmt.Sprintf("unexpected %d bytes remaining in decoder after decoding FetchResponse", pd.Remaining()))
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
			return decodingErr.WithAddedContext("topic_id_bytes")
		}
		return err
	}
	topicUUID, err := encoder.DecodeUUID(topicUUIDBytes)
	if err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return decodingErr.WithAddedContext("topic_id")
		}
		return err
	}
	tr.Topic = topicUUID

	numPartitions, err := pd.GetCompactArrayLength()
	if err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return decodingErr.WithAddedContext("num_partitions")
		}
		return err
	}
	tr.Partitions = make([]PartitionResponse, numPartitions)

	for j := range tr.Partitions {
		partition := PartitionResponse{}
		err := partition.Decode(pd)
		if err != nil {
			if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
				return decodingErr.WithAddedContext(fmt.Sprintf("PartitionResponse[%d]", j))
			}
			return err
		}
		tr.Partitions[j] = partition
	}
	_, err = pd.GetEmptyTaggedFieldArray()
	if err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return decodingErr.WithAddedContext("TAG_BUFFER")
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
			return decodingErr.WithAddedContext("partition_index")
		}
		return err
	}

	if pr.ErrorCode, err = pd.GetInt16(); err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return decodingErr.WithAddedContext("error_code")
		}
		return err
	}

	if pr.HighWatermark, err = pd.GetInt64(); err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return decodingErr.WithAddedContext("high_watermark")
		}
		return err
	}

	if pr.LastStableOffset, err = pd.GetInt64(); err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return decodingErr.WithAddedContext("last_stable_offset")
		}
		return err
	}

	if pr.LogStartOffset, err = pd.GetInt64(); err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return decodingErr.WithAddedContext("log_start_offset")
		}
		return err
	}

	numAbortedTransactions, err := pd.GetCompactArrayLength()
	if err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return decodingErr.WithAddedContext("num_aborted_transactions")
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
					return decodingErr.WithAddedContext(fmt.Sprintf("AbortedTransaction[%d]", k))
				}
				return err
			}
			pr.AbortedTransactions[k] = abortedTransaction
		}
	}

	if pr.PreferedReadReplica, err = pd.GetInt32(); err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return decodingErr.WithAddedContext("preferred_read_replica")
		}
		return err
	}

	// Record Batches are encoded as COMPACT_RECORDS
	numBytes, err := pd.GetUnsignedVarint()
	if err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return decodingErr.WithAddedContext("compact_records_length")
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
				return decodingErr.WithAddedContext(fmt.Sprintf("RecordBatch[%d]", k))
			}
			return err
		}
		pr.Records = append(pr.Records, recordBatch)
		k++
	}
	_, err = pd.GetEmptyTaggedFieldArray()
	if err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return decodingErr.WithAddedContext("TAG_BUFFER")
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
			return decodingErr.WithAddedContext("producer_id")
		}
		return err
	}

	if ab.FirstOffset, err = pd.GetInt64(); err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return decodingErr.WithAddedContext("first_offset")
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
			return decodingErr.WithAddedContext("base_offset")
		}
		return err
	}

	if rb.BatchLength, err = pd.GetInt32(); err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return decodingErr.WithAddedContext("batch_length")
		}
		return err
	}

	if rb.PartitionLeaderEpoch, err = pd.GetInt32(); err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return decodingErr.WithAddedContext("partition_leader_epoch")
		}
		return err
	}

	if rb.Magic, err = pd.GetInt8(); err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return decodingErr.WithAddedContext("magic_byte")
		}
		return err
	}

	if rb.CRC, err = pd.GetInt32(); err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return decodingErr.WithAddedContext("crc")
		}
		return err
	}

	crcTable := crc32.MakeTable(crc32.Castagnoli)
	// BatchLength / Message Size contains the size of the message excluding the BaseOffset & the BatchLength
	// From the BatchLength, we need to subtract 9 bytes to get the size of the message that is used in computing the CRC
	// The 9 bytes are:
	// - PartitionLeaderEpoch : 4 bytes
	// - MagicByte : 1 byte
	// - CRC : 4 bytes
	data, _ := pd.GetRawBytesFromOffset(int(rb.BatchLength) - 9)
	computedChecksum := crc32.Checksum(data, crcTable)
	// ToDo: Add debug logging ?
	// fmt.Printf("CRC-32C checksum: 0x%08x 0x%08x\n", checksum, uint32(rb.CRC))
	if computedChecksum != uint32(rb.CRC) {
		return errors.NewPacketDecodingError(fmt.Sprintf("CRC mismatch: calculated %08x, expected %08x", computedChecksum, uint32(rb.CRC)), "crc")
	}

	if rb.Attributes, err = pd.GetInt16(); err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return decodingErr.WithAddedContext("record_attributes")
		}
		return err
	}

	if rb.LastOffsetDelta, err = pd.GetInt32(); err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return decodingErr.WithAddedContext("last_offset_delta")
		}
		return err
	}

	if rb.FirstTimestamp, err = pd.GetInt64(); err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return decodingErr.WithAddedContext("base_timestamp")
		}
		return err
	}

	if rb.MaxTimestamp, err = pd.GetInt64(); err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return decodingErr.WithAddedContext("max_timestamp")
		}
		return err
	}

	if rb.ProducerId, err = pd.GetInt64(); err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return decodingErr.WithAddedContext("producer_id")
		}
		return err
	}

	if rb.ProducerEpoch, err = pd.GetInt16(); err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return decodingErr.WithAddedContext("producer_epoch")
		}
		return err
	}

	if rb.BaseSequence, err = pd.GetInt32(); err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return decodingErr.WithAddedContext("base_sequence")
		}
		return err
	}

	numRecords, err := pd.GetInt32()
	if err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return decodingErr.WithAddedContext("num_records")
		}
		return err
	}

	for i := 0; i < int(numRecords); i++ {
		record := Record{}
		err := record.Decode(pd)
		if err != nil {
			if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
				return decodingErr.WithAddedContext(fmt.Sprintf("Record[%d]", i))
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
			return decodingErr.WithAddedContext("timestamp_delta")
		}
		return err
	}

	offsetDelta, err := pd.GetSignedVarint()
	if err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return decodingErr.WithAddedContext("offset_delta")
		}
		return err
	}
	r.OffsetDelta = int32(offsetDelta)

	keyLength, err := pd.GetSignedVarint()
	if err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return decodingErr.WithAddedContext("key_length")
		}
		return err
	}

	var key []byte
	if keyLength > 0 {
		key, err = pd.GetRawBytes(int(keyLength) - 1)
		if err != nil {
			if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
				return decodingErr.WithAddedContext("key")
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
			return decodingErr.WithAddedContext("value_length")
		}
		return err
	}

	value, err := pd.GetRawBytes(int(valueLength))
	if err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return decodingErr.WithAddedContext("value")
		}
		return err
	}
	r.Value = value

	_, err = pd.GetEmptyTaggedFieldArray()
	if err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return decodingErr.WithAddedContext("TAG_BUFFER")
		}
		return err
	}

	return nil
}

type RecordHeader struct {
	Key   string
	Value []byte
}
