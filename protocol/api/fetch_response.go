package kafkaapi

import (
	"fmt"
	"hash/crc32"
	"strings"

	"github.com/codecrafters-io/kafka-tester/protocol/decoder"
	"github.com/codecrafters-io/kafka-tester/protocol/encoder"
	"github.com/codecrafters-io/kafka-tester/protocol/errors"
	"github.com/codecrafters-io/tester-utils/logger"
)

type FetchResponse struct {
	Version        int16
	ThrottleTimeMs int32
	ErrorCode      int16
	SessionID      int32
	TopicResponses []TopicResponse
}

func (r *FetchResponse) Decode(pd *decoder.RealDecoder, version int16, logger *logger.Logger, indentation int) (err error) {
	// After every element in the struct is decoded
	// We log out the value at the current indentation level
	// As we nest deeper, we increment the indentation level
	r.Version = version

	if r.ThrottleTimeMs, err = pd.GetInt32(); err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return decodingErr.WithAddedContext("throttle_time_ms")
		}
		return err
	}
	logWithIndentation(logger, indentation, "✔️ .throttle_time_ms (%d)", r.ThrottleTimeMs)

	if r.ErrorCode, err = pd.GetInt16(); err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return decodingErr.WithAddedContext("error_code")
		}
		return err
	}
	logWithIndentation(logger, indentation, "✔️ .error_code (%d)", r.ErrorCode)

	if r.SessionID, err = pd.GetInt32(); err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return decodingErr.WithAddedContext("session_id")
		}
		return err
	}
	logWithIndentation(logger, indentation, "✔️ .session_id (%d)", r.SessionID)

	numResponses, err := pd.GetCompactArrayLength()
	if err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return decodingErr.WithAddedContext("num_responses")
		}
		return err
	}
	logWithIndentation(logger, indentation, "✔️ .num_responses (%d)", numResponses)

	r.TopicResponses = make([]TopicResponse, numResponses)
	for i := range r.TopicResponses {
		topicResponse := TopicResponse{}
		logWithIndentation(logger, indentation, "- .TopicResponse[%d]", i)
		err := topicResponse.Decode(pd, logger, indentation+1)
		if err != nil {
			if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
				return decodingErr.WithAddedContext(fmt.Sprintf("TopicResponse[%d]", i))
			}
			return err
		}
		r.TopicResponses[i] = topicResponse
	}
	_, err = pd.GetEmptyTaggedFieldArray()
	if err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return decodingErr.WithAddedContext("TAG_BUFFER")
		}
		return err
	}
	logWithIndentation(logger, indentation, "✔️ .TAG_BUFFER")

	if pd.Remaining() != 0 {
		return errors.NewPacketDecodingError(fmt.Sprintf("unexpected %d bytes remaining in decoder after decoding FetchResponse", pd.Remaining()))
	}

	return nil
}

type TopicResponse struct {
	Topic              string
	PartitionResponses []PartitionResponse
}

func (tr *TopicResponse) Decode(pd *decoder.RealDecoder, logger *logger.Logger, indentation int) (err error) {
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
	logWithIndentation(logger, indentation, "✔️ .topic_id (%s)", tr.Topic)

	numPartitions, err := pd.GetCompactArrayLength()
	if err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return decodingErr.WithAddedContext("num_partitions")
		}
		return err
	}
	tr.PartitionResponses = make([]PartitionResponse, numPartitions)
	logWithIndentation(logger, indentation, "✔️ .num_partitions (%d)", numPartitions)

	for j := range tr.PartitionResponses {
		partition := PartitionResponse{}
		logWithIndentation(logger, indentation, "- .PartitionResponse[%d]", j)
		err := partition.Decode(pd, logger, indentation+1)
		if err != nil {
			if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
				return decodingErr.WithAddedContext(fmt.Sprintf("PartitionResponse[%d]", j))
			}
			return err
		}
		tr.PartitionResponses[j] = partition
	}
	_, err = pd.GetEmptyTaggedFieldArray()
	if err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return decodingErr.WithAddedContext("TAG_BUFFER")
		}
		return err
	}
	logWithIndentation(logger, indentation, "✔️ .TAG_BUFFER")

	return nil
}

type PartitionResponse struct {
	PartitionIndex      int32
	ErrorCode           int16
	HighWatermark       int64
	LastStableOffset    int64
	LogStartOffset      int64
	AbortedTransactions []AbortedTransaction
	RecordBatches       []RecordBatch
	PreferedReadReplica int32
}

func (pr *PartitionResponse) Decode(pd *decoder.RealDecoder, logger *logger.Logger, indentation int) (err error) {
	if pr.PartitionIndex, err = pd.GetInt32(); err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return decodingErr.WithAddedContext("partition_index")
		}
		return err
	}
	logWithIndentation(logger, indentation, "✔️ .partition_index (%d)", pr.PartitionIndex)

	if pr.ErrorCode, err = pd.GetInt16(); err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return decodingErr.WithAddedContext("error_code")
		}
		return err
	}
	logWithIndentation(logger, indentation, "✔️ .error_code (%d)", pr.ErrorCode)

	if pr.HighWatermark, err = pd.GetInt64(); err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return decodingErr.WithAddedContext("high_watermark")
		}
		return err
	}
	logWithIndentation(logger, indentation, "✔️ .high_watermark (%d)", pr.HighWatermark)

	if pr.LastStableOffset, err = pd.GetInt64(); err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return decodingErr.WithAddedContext("last_stable_offset")
		}
		return err
	}
	logWithIndentation(logger, indentation, "✔️ .last_stable_offset (%d)", pr.LastStableOffset)

	if pr.LogStartOffset, err = pd.GetInt64(); err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return decodingErr.WithAddedContext("log_start_offset")
		}
		return err
	}
	logWithIndentation(logger, indentation, "✔️ .log_start_offset (%d)", pr.LogStartOffset)

	numAbortedTransactions, err := pd.GetCompactArrayLength()
	if err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return decodingErr.WithAddedContext("num_aborted_transactions")
		}
		return err
	}
	logWithIndentation(logger, indentation, "✔️ .num_aborted_transactions (%d)", numAbortedTransactions)

	if numAbortedTransactions > 0 {
		pr.AbortedTransactions = make([]AbortedTransaction, numAbortedTransactions)
		for k := range pr.AbortedTransactions {
			abortedTransaction := AbortedTransaction{}
			logWithIndentation(logger, indentation, "- .AbortedTransaction[%d]", k)
			err := abortedTransaction.Decode(pd, logger, indentation+1)
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
	logWithIndentation(logger, indentation, "✔️ .preferred_read_replica (%d)", pr.PreferedReadReplica)

	// Record Batches are encoded as COMPACT_RECORDS
	numBytes, err := pd.GetUnsignedVarint()
	if err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return decodingErr.WithAddedContext("compact_records_length")
		}
		return err
	}
	numBytes -= 1
	logWithIndentation(logger, indentation, "✔️ .compact_records_length (%d)", numBytes)

	k := 0
	for numBytes > 0 && pd.Remaining() > 10 {
		recordBatch := RecordBatch{}
		logWithIndentation(logger, indentation, "- .RecordBatch[%d]", k)
		err := recordBatch.Decode(pd, logger, indentation+1)
		if err != nil {
			if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
				return decodingErr.WithAddedContext(fmt.Sprintf("RecordBatch[%d]", k))
			}
			return err
		}
		pr.RecordBatches = append(pr.RecordBatches, recordBatch)
		k++
	}
	_, err = pd.GetEmptyTaggedFieldArray()
	if err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return decodingErr.WithAddedContext("TAG_BUFFER")
		}
		return err
	}
	logWithIndentation(logger, indentation, "✔️ .TAG_BUFFER")

	return nil
}

type AbortedTransaction struct {
	ProducerID  int64
	FirstOffset int64
}

func (ab *AbortedTransaction) Decode(pd *decoder.RealDecoder, logger *logger.Logger, indentation int) (err error) {
	if ab.ProducerID, err = pd.GetInt64(); err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return decodingErr.WithAddedContext("producer_id")
		}
		return err
	}
	logWithIndentation(logger, indentation, "✔️ .producer_id (%d)", ab.ProducerID)

	if ab.FirstOffset, err = pd.GetInt64(); err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return decodingErr.WithAddedContext("first_offset")
		}
		return err
	}
	logWithIndentation(logger, indentation, "✔️ .first_offset (%d)", ab.FirstOffset)

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

func (rb *RecordBatch) Decode(pd *decoder.RealDecoder, logger *logger.Logger, indentation int) (err error) {
	if rb.BaseOffset, err = pd.GetInt64(); err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return decodingErr.WithAddedContext("base_offset")
		}
		return err
	}
	logWithIndentation(logger, indentation, "✔️ .base_offset (%d)", rb.BaseOffset)

	if rb.BatchLength, err = pd.GetInt32(); err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return decodingErr.WithAddedContext("batch_length")
		}
		return err
	}
	logWithIndentation(logger, indentation, "✔️ .batch_length (%d)", rb.BatchLength)

	if rb.PartitionLeaderEpoch, err = pd.GetInt32(); err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return decodingErr.WithAddedContext("partition_leader_epoch")
		}
		return err
	}
	logWithIndentation(logger, indentation, "✔️ .partition_leader_epoch (%d)", rb.PartitionLeaderEpoch)

	if rb.Magic, err = pd.GetInt8(); err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return decodingErr.WithAddedContext("magic_byte")
		}
		return err
	}
	logWithIndentation(logger, indentation, "✔️ .magic_byte (%d)", rb.Magic)

	if rb.CRC, err = pd.GetInt32(); err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return decodingErr.WithAddedContext("crc")
		}
		return err
	}
	logWithIndentation(logger, indentation, "✔️ .crc (%d)", rb.CRC)

	crcTable := crc32.MakeTable(crc32.Castagnoli)
	// BatchLength / Message Size contains the size of the message excluding the BaseOffset & the BatchLength
	// From the BatchLength, we need to subtract 9 bytes to get the size of the message that is used in computing the CRC
	// The 9 bytes are:
	// - PartitionLeaderEpoch : 4 bytes
	// - MagicByte : 1 byte
	// - CRC : 4 bytes
	data, _ := pd.GetRawBytesFromOffset(int(rb.BatchLength) - 9)
	computedChecksum := crc32.Checksum(data, crcTable)
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
	logWithIndentation(logger, indentation, "✔️ .record_attributes (%d)", rb.Attributes)

	if rb.LastOffsetDelta, err = pd.GetInt32(); err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return decodingErr.WithAddedContext("last_offset_delta")
		}
		return err
	}
	logWithIndentation(logger, indentation, "✔️ .last_offset_delta (%d)", rb.LastOffsetDelta)

	if rb.FirstTimestamp, err = pd.GetInt64(); err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return decodingErr.WithAddedContext("base_timestamp")
		}
		return err
	}
	logWithIndentation(logger, indentation, "✔️ .base_timestamp (%d)", rb.FirstTimestamp)

	if rb.MaxTimestamp, err = pd.GetInt64(); err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return decodingErr.WithAddedContext("max_timestamp")
		}
		return err
	}
	logWithIndentation(logger, indentation, "✔️ .max_timestamp (%d)", rb.MaxTimestamp)

	if rb.ProducerId, err = pd.GetInt64(); err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return decodingErr.WithAddedContext("producer_id")
		}
		return err
	}
	logWithIndentation(logger, indentation, "✔️ .producer_id (%d)", rb.ProducerId)

	if rb.ProducerEpoch, err = pd.GetInt16(); err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return decodingErr.WithAddedContext("producer_epoch")
		}
		return err
	}
	logWithIndentation(logger, indentation, "✔️ .producer_epoch (%d)", rb.ProducerEpoch)

	if rb.BaseSequence, err = pd.GetInt32(); err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return decodingErr.WithAddedContext("base_sequence")
		}
		return err
	}
	logWithIndentation(logger, indentation, "✔️ .base_sequence (%d)", rb.BaseSequence)

	numRecords, err := pd.GetInt32()
	if err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return decodingErr.WithAddedContext("num_records")
		}
		return err
	}
	logWithIndentation(logger, indentation, "✔️ .num_records (%d)", numRecords)

	for i := 0; i < int(numRecords); i++ {
		record := Record{}
		logWithIndentation(logger, indentation, "- .Record[%d]", i)
		err := record.Decode(pd, logger, indentation+1)
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

func (r *Record) Decode(pd *decoder.RealDecoder, logger *logger.Logger, indentation int) (err error) {
	length, err := pd.GetUnsignedVarint()
	if err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return decodingErr.WithAddedContext("length")
		}
		return err
	}
	r.Length = int32(length)
	logWithIndentation(logger, indentation, "✔️ .length (%d)", r.Length)

	if r.Attributes, err = pd.GetInt8(); err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return decodingErr.WithAddedContext("attributes")
		}
		return err
	}
	logWithIndentation(logger, indentation, "✔️ .attributes (%d)", r.Attributes)

	if r.TimestampDelta, err = pd.GetSignedVarint(); err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return decodingErr.WithAddedContext("timestamp_delta")
		}
		return err
	}
	logWithIndentation(logger, indentation, "✔️ .timestamp_delta (%d)", r.TimestampDelta)

	offsetDelta, err := pd.GetSignedVarint()
	if err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return decodingErr.WithAddedContext("offset_delta")
		}
		return err
	}
	r.OffsetDelta = int32(offsetDelta)
	logWithIndentation(logger, indentation, "✔️ .offset_delta (%d)", r.OffsetDelta)

	keyLength, err := pd.GetSignedVarint()
	if err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return decodingErr.WithAddedContext("key_length")
		}
		return err
	}
	logWithIndentation(logger, indentation, "✔️ .key_length (%d)", keyLength)

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
	logWithIndentation(logger, indentation, "✔️ .key")

	valueLength, err := pd.GetSignedVarint()
	if err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return decodingErr.WithAddedContext("value_length")
		}
		return err
	}
	logWithIndentation(logger, indentation, "✔️ .value_length (%d)", valueLength)

	value, err := pd.GetRawBytes(int(valueLength))
	if err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return decodingErr.WithAddedContext("value")
		}
		return err
	}
	r.Value = value
	logWithIndentation(logger, indentation, "✔️ .value")

	_, err = pd.GetEmptyTaggedFieldArray()
	if err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return decodingErr.WithAddedContext("TAG_BUFFER")
		}
		return err
	}
	logWithIndentation(logger, indentation, "✔️ .TAG_BUFFER")

	return nil
}

type RecordHeader struct {
	Key   string
	Value []byte
}

func logWithIndentation(logger *logger.Logger, indentation int, message string, args ...interface{}) {
	logger.Debugf(fmt.Sprintf("%s%s", strings.Repeat(" ", indentation*2), message), args...)
}
