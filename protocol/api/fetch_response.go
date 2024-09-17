package kafkaapi

import (
	"encoding/json"
	"fmt"
	"hash/crc32"

	"github.com/codecrafters-io/kafka-tester/protocol"
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
	protocol.LogWithIndentation(logger, indentation, "✔️ .throttle_time_ms (%d)", r.ThrottleTimeMs)

	if r.ErrorCode, err = pd.GetInt16(); err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return decodingErr.WithAddedContext("error_code")
		}
		return err
	}
	protocol.LogWithIndentation(logger, indentation, "✔️ .error_code (%d)", r.ErrorCode)

	if r.SessionID, err = pd.GetInt32(); err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return decodingErr.WithAddedContext("session_id")
		}
		return err
	}
	protocol.LogWithIndentation(logger, indentation, "✔️ .session_id (%d)", r.SessionID)

	numResponses, err := pd.GetCompactArrayLength()
	if err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return decodingErr.WithAddedContext("num_responses")
		}
		return err
	}
	protocol.LogWithIndentation(logger, indentation, "✔️ .num_responses (%d)", numResponses)

	if numResponses < 0 {
		return errors.NewPacketDecodingError(fmt.Sprintf("Count of TopicResponses cannot be negative: %d", numResponses))
	}

	r.TopicResponses = make([]TopicResponse, numResponses)
	for i := range r.TopicResponses {
		topicResponse := TopicResponse{}
		protocol.LogWithIndentation(logger, indentation, "- .TopicResponse[%d]", i)
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
	protocol.LogWithIndentation(logger, indentation, "✔️ .TAG_BUFFER")

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
	protocol.LogWithIndentation(logger, indentation, "✔️ .topic_id (%s)", tr.Topic)

	numPartitions, err := pd.GetCompactArrayLength()
	if err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return decodingErr.WithAddedContext("num_partitions")
		}
		return err
	}
	tr.PartitionResponses = make([]PartitionResponse, numPartitions)
	protocol.LogWithIndentation(logger, indentation, "✔️ .num_partitions (%d)", numPartitions)

	if numPartitions < 0 {
		return errors.NewPacketDecodingError(fmt.Sprintf("Count of PartitionResponses cannot be negative: %d", numPartitions))
	}

	for j := range tr.PartitionResponses {
		partition := PartitionResponse{}
		protocol.LogWithIndentation(logger, indentation, "- .PartitionResponse[%d]", j)
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
	protocol.LogWithIndentation(logger, indentation, "✔️ .TAG_BUFFER")

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
	protocol.LogWithIndentation(logger, indentation, "✔️ .partition_index (%d)", pr.PartitionIndex)

	if pr.ErrorCode, err = pd.GetInt16(); err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return decodingErr.WithAddedContext("error_code")
		}
		return err
	}
	protocol.LogWithIndentation(logger, indentation, "✔️ .error_code (%d)", pr.ErrorCode)

	if pr.HighWatermark, err = pd.GetInt64(); err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return decodingErr.WithAddedContext("high_watermark")
		}
		return err
	}
	protocol.LogWithIndentation(logger, indentation, "✔️ .high_watermark (%d)", pr.HighWatermark)

	if pr.LastStableOffset, err = pd.GetInt64(); err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return decodingErr.WithAddedContext("last_stable_offset")
		}
		return err
	}
	protocol.LogWithIndentation(logger, indentation, "✔️ .last_stable_offset (%d)", pr.LastStableOffset)

	if pr.LogStartOffset, err = pd.GetInt64(); err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return decodingErr.WithAddedContext("log_start_offset")
		}
		return err
	}
	protocol.LogWithIndentation(logger, indentation, "✔️ .log_start_offset (%d)", pr.LogStartOffset)

	numAbortedTransactions, err := pd.GetCompactArrayLength()
	if err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return decodingErr.WithAddedContext("num_aborted_transactions")
		}
		return err
	}
	protocol.LogWithIndentation(logger, indentation, "✔️ .num_aborted_transactions (%d)", numAbortedTransactions)

	if numAbortedTransactions < 0 {
		return errors.NewPacketDecodingError(fmt.Sprintf("Count of AbortedTransactions cannot be negative: %d", numAbortedTransactions))
	}

	if numAbortedTransactions > 0 {
		pr.AbortedTransactions = make([]AbortedTransaction, numAbortedTransactions)
		for k := range pr.AbortedTransactions {
			abortedTransaction := AbortedTransaction{}
			protocol.LogWithIndentation(logger, indentation, "- .AbortedTransaction[%d]", k)
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
	protocol.LogWithIndentation(logger, indentation, "✔️ .preferred_read_replica (%d)", pr.PreferedReadReplica)

	// Record Batches are encoded as COMPACT_RECORDS
	numBytes, err := pd.GetUnsignedVarint()
	if err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return decodingErr.WithAddedContext("compact_records_length")
		}
		return err
	}
	numBytes -= 1
	protocol.LogWithIndentation(logger, indentation, "✔️ .compact_records_length (%d)", numBytes)

	k := 0
	for numBytes > 0 && pd.Remaining() > 10 {
		recordBatch := RecordBatch{}
		protocol.LogWithIndentation(logger, indentation, "- .RecordBatch[%d]", k)
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
	protocol.LogWithIndentation(logger, indentation, "✔️ .TAG_BUFFER")

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
	protocol.LogWithIndentation(logger, indentation, "✔️ .producer_id (%d)", ab.ProducerID)

	if ab.FirstOffset, err = pd.GetInt64(); err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return decodingErr.WithAddedContext("first_offset")
		}
		return err
	}
	protocol.LogWithIndentation(logger, indentation, "✔️ .first_offset (%d)", ab.FirstOffset)

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
	protocol.LogWithIndentation(logger, indentation, "✔️ .base_offset (%d)", rb.BaseOffset)

	if rb.BatchLength, err = pd.GetInt32(); err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return decodingErr.WithAddedContext("batch_length")
		}
		return err
	}
	protocol.LogWithIndentation(logger, indentation, "✔️ .batch_length (%d)", rb.BatchLength)

	if rb.PartitionLeaderEpoch, err = pd.GetInt32(); err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return decodingErr.WithAddedContext("partition_leader_epoch")
		}
		return err
	}
	protocol.LogWithIndentation(logger, indentation, "✔️ .partition_leader_epoch (%d)", rb.PartitionLeaderEpoch)

	if rb.Magic, err = pd.GetInt8(); err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return decodingErr.WithAddedContext("magic_byte")
		}
		return err
	}
	protocol.LogWithIndentation(logger, indentation, "✔️ .magic_byte (%d)", rb.Magic)

	if rb.CRC, err = pd.GetInt32(); err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return decodingErr.WithAddedContext("crc")
		}
		return err
	}
	protocol.LogWithIndentation(logger, indentation, "✔️ .crc (%d)", rb.CRC)

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
	protocol.LogWithIndentation(logger, indentation, "✔️ .record_attributes (%d)", rb.Attributes)

	if rb.LastOffsetDelta, err = pd.GetInt32(); err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return decodingErr.WithAddedContext("last_offset_delta")
		}
		return err
	}
	protocol.LogWithIndentation(logger, indentation, "✔️ .last_offset_delta (%d)", rb.LastOffsetDelta)

	if rb.FirstTimestamp, err = pd.GetInt64(); err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return decodingErr.WithAddedContext("base_timestamp")
		}
		return err
	}
	protocol.LogWithIndentation(logger, indentation, "✔️ .base_timestamp (%d)", rb.FirstTimestamp)

	if rb.MaxTimestamp, err = pd.GetInt64(); err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return decodingErr.WithAddedContext("max_timestamp")
		}
		return err
	}
	protocol.LogWithIndentation(logger, indentation, "✔️ .max_timestamp (%d)", rb.MaxTimestamp)

	if rb.ProducerId, err = pd.GetInt64(); err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return decodingErr.WithAddedContext("producer_id")
		}
		return err
	}
	protocol.LogWithIndentation(logger, indentation, "✔️ .producer_id (%d)", rb.ProducerId)

	if rb.ProducerEpoch, err = pd.GetInt16(); err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return decodingErr.WithAddedContext("producer_epoch")
		}
		return err
	}
	protocol.LogWithIndentation(logger, indentation, "✔️ .producer_epoch (%d)", rb.ProducerEpoch)

	if rb.BaseSequence, err = pd.GetInt32(); err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return decodingErr.WithAddedContext("base_sequence")
		}
		return err
	}
	protocol.LogWithIndentation(logger, indentation, "✔️ .base_sequence (%d)", rb.BaseSequence)

	numRecords, err := pd.GetInt32()
	if err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return decodingErr.WithAddedContext("num_records")
		}
		return err
	}
	protocol.LogWithIndentation(logger, indentation, "✔️ .num_records (%d)", numRecords)

	if numRecords < 0 {
		return errors.NewPacketDecodingError(fmt.Sprintf("Count of Records cannot be negative: %d", numRecords))
	}

	for i := 0; i < int(numRecords); i++ {
		record := Record{}
		protocol.LogWithIndentation(logger, indentation, "- .Record[%d]", i)
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
	ProcessedValue payload
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
	protocol.LogWithIndentation(logger, indentation, "✔️ .length (%d)", r.Length)

	if r.Attributes, err = pd.GetInt8(); err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return decodingErr.WithAddedContext("attributes")
		}
		return err
	}
	protocol.LogWithIndentation(logger, indentation, "✔️ .attributes (%d)", r.Attributes)

	if r.TimestampDelta, err = pd.GetSignedVarint(); err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return decodingErr.WithAddedContext("timestamp_delta")
		}
		return err
	}
	protocol.LogWithIndentation(logger, indentation, "✔️ .timestamp_delta (%d)", r.TimestampDelta)

	offsetDelta, err := pd.GetSignedVarint()
	if err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return decodingErr.WithAddedContext("offset_delta")
		}
		return err
	}
	r.OffsetDelta = int32(offsetDelta)
	protocol.LogWithIndentation(logger, indentation, "✔️ .offset_delta (%d)", r.OffsetDelta)

	keyLength, err := pd.GetSignedVarint()
	if err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return decodingErr.WithAddedContext("key_length")
		}
		return err
	}
	protocol.LogWithIndentation(logger, indentation, "✔️ .key_length (%d)", keyLength)

	var key []byte
	if keyLength > 0 {
		key, err = pd.GetRawBytes(int(keyLength))
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
	protocol.LogWithIndentation(logger, indentation, "✔️ .key (%q)", string(r.Key))

	valueLength, err := pd.GetSignedVarint()
	if err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return decodingErr.WithAddedContext("value_length")
		}
		return err
	}
	protocol.LogWithIndentation(logger, indentation, "✔️ .value_length (%d)", valueLength)

	value, err := pd.GetRawBytes(int(valueLength))
	if err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return decodingErr.WithAddedContext("value")
		}
		return err
	}
	r.Value = value
	protocol.LogWithIndentation(logger, indentation, "✔️ .value (%q)", string(r.Value))

	if pd.ShouldParseClusterMetadataValues() {
		payload := payload{}
		if err := payload.Decode(r.Value); err != nil {
			return err
		}
		r.ProcessedValue = payload
	}

	numHeaders, err := pd.GetSignedVarint()
	if err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return decodingErr.WithAddedContext("num_headers")
		}
		return err
	}
	protocol.LogWithIndentation(logger, indentation, "✔️ .num_headers (%d)", numHeaders)

	for i := 0; i < int(numHeaders); i++ {
		header := RecordHeader{}
		protocol.LogWithIndentation(logger, indentation, "- .RecordHeader[%d]", i)
		err := header.Decode(pd, logger, indentation+1)
		if err != nil {
			if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
				return decodingErr.WithAddedContext(fmt.Sprintf("RecordHeader[%d]", i))
			}
			return err
		}
		r.Headers = append(r.Headers, header)
	}

	return nil
}

type RecordHeader struct {
	Key   string
	Value []byte
}

func (rh *RecordHeader) Decode(pd *decoder.RealDecoder, logger *logger.Logger, indentation int) (err error) {
	keyLength, err := pd.GetSignedVarint()
	if err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return decodingErr.WithAddedContext("key_length")
		}
		return err
	}
	protocol.LogWithIndentation(logger, indentation, "✔️ .key_length (%d)", keyLength)

	key, err := pd.GetRawBytes(int(keyLength))
	if err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return decodingErr.WithAddedContext("key")
		}
		return err
	}
	rh.Key = string(key)
	protocol.LogWithIndentation(logger, indentation, "✔️ .key (%s)", rh.Key)

	valueLength, err := pd.GetSignedVarint()
	if err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return decodingErr.WithAddedContext("value_length")
		}
		return err
	}
	protocol.LogWithIndentation(logger, indentation, "✔️ .value_length (%d)", valueLength)

	value, err := pd.GetRawBytes(int(valueLength))
	if err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return decodingErr.WithAddedContext("value")
		}
		return err
	}
	rh.Value = value
	protocol.LogWithIndentation(logger, indentation, "✔️ .value (%s)", rh.Value)

	return nil
}

type payload struct {
	FrameVersion int8
	Type         int8
	Version      int8
	Data         json.RawMessage
}

func (p *payload) Decode(data []byte) (err error) {
	partialDecoder := decoder.RealDecoder{}
	partialDecoder.Init(data)

	p.FrameVersion, err = partialDecoder.GetInt8() // Frame Version: 0
	if err != nil {
		return err
	}

	p.Type, err = partialDecoder.GetInt8()
	if err != nil {
		return err
	}

	p.Version, err = partialDecoder.GetInt8()
	if err != nil {
		return err
	}

	jsonObject := map[string]interface{}{}

	switch p.Type {
	case 5:
		jsonObject["type"] = "PARTITION_CHANGE_RECORD"
		jsonObject["version"] = p.Version
		partitionId, err := partialDecoder.GetInt32()
		if err != nil {
			return err
		}
		jsonObject["partitionId"] = partitionId

		topicId, err := getUUID(&partialDecoder)
		if err != nil {
			return err
		}
		jsonObject["topicId"] = topicId

		// skip 3 bytes (not sure why) ToDo
		_, err = partialDecoder.GetRawBytes(3)
		if err != nil {
			return err
		}

		leader, err := partialDecoder.GetInt32()
		if err != nil {
			return err
		}
		jsonObject["leader"] = leader

		if partialDecoder.Remaining() > 0 {
			return errors.NewPacketDecodingError(fmt.Sprintf("Remaining bytes after decoding: %d", partialDecoder.Remaining()), "PARTITION_CHANGE_RECORD")
		}
	case 20:
		jsonObject["type"] = "NO_OP_RECORD"
		jsonObject["version"] = p.Version

		// COMPACT_NULLABLE_BYTES
		// 0x00 - NULL
		dataLength, err := partialDecoder.GetUnsignedVarint()
		if err != nil {
			return err
		}
		if dataLength == 0 {
			jsonObject["data"] = nil
		} else {
			jsonObject["data"] = make([]interface{}, dataLength)
		}

		if partialDecoder.Remaining() > 0 {
			return errors.NewPacketDecodingError(fmt.Sprintf("Remaining bytes after decoding: %d", partialDecoder.Remaining()), "PARTITION_CHANGE_RECORD")
		}
	case 17:
		jsonObject["type"] = "NO_OP_RECORD"
		jsonObject["version"] = p.Version

		brokerId, err := partialDecoder.GetInt32()
		if err != nil {
			return err
		}
		jsonObject["brokerId"] = brokerId

		brokerEpoch, err := partialDecoder.GetInt64()
		if err != nil {
			return err
		}
		jsonObject["brokerEpoch"] = brokerEpoch

		fenced, err := partialDecoder.GetInt8()
		if err != nil {
			return err
		}
		jsonObject["fenced"] = fenced

		// if p.Version == 1 { // seems to be present always
		inControlledShutdown, err := partialDecoder.GetInt8()
		if err != nil {
			return err
		}
		jsonObject["inControlledShutdown"] = inControlledShutdown
		// }

		// ToDo: Not sure why we need this
		_, err = partialDecoder.GetRawBytes(2)
		if err != nil {
			return err
		}

		if partialDecoder.Remaining() > 0 {
			return errors.NewPacketDecodingError(fmt.Sprintf("Remaining bytes after decoding: %d", partialDecoder.Remaining()), "PARTITION_CHANGE_RECORD")
		}
	}
	jsonData, err := json.Marshal(jsonObject)
	if err != nil {
		return err
	}
	p.Data = json.RawMessage(jsonData)

	return nil
}

func getUUID(pd *decoder.RealDecoder) (string, error) {
	topicUUIDBytes, err := pd.GetRawBytes(16)
	if err != nil {
		return "", err
	}
	topicUUID, err := encoder.DecodeUUID(topicUUIDBytes)
	if err != nil {
		return "", err
	}
	return topicUUID, nil
}
