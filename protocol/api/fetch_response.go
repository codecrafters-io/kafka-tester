package kafkaapi

import (
	"fmt"

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
	protocol.LogWithIndentation(logger, indentation, "- .throttle_time_ms (%d)", r.ThrottleTimeMs)

	if r.ErrorCode, err = pd.GetInt16(); err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return decodingErr.WithAddedContext("error_code")
		}
		return err
	}
	protocol.LogWithIndentation(logger, indentation, "- .error_code (%d)", r.ErrorCode)

	if r.SessionID, err = pd.GetInt32(); err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return decodingErr.WithAddedContext("session_id")
		}
		return err
	}
	protocol.LogWithIndentation(logger, indentation, "- .session_id (%d)", r.SessionID)

	numResponses, err := pd.GetCompactArrayLength()
	if err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return decodingErr.WithAddedContext("num_responses")
		}
		return err
	}
	protocol.LogWithIndentation(logger, indentation, "- .num_responses (%d)", numResponses)

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
	protocol.LogWithIndentation(logger, indentation, "- .TAG_BUFFER")

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
	protocol.LogWithIndentation(logger, indentation, "- .topic_id (%s)", tr.Topic)

	numPartitions, err := pd.GetCompactArrayLength()
	if err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return decodingErr.WithAddedContext("num_partitions")
		}
		return err
	}
	tr.PartitionResponses = make([]PartitionResponse, numPartitions)
	protocol.LogWithIndentation(logger, indentation, "- .num_partitions (%d)", numPartitions)

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
	protocol.LogWithIndentation(logger, indentation, "- .TAG_BUFFER")

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
	protocol.LogWithIndentation(logger, indentation, "- .partition_index (%d)", pr.PartitionIndex)

	if pr.ErrorCode, err = pd.GetInt16(); err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return decodingErr.WithAddedContext("error_code")
		}
		return err
	}
	protocol.LogWithIndentation(logger, indentation, "- .error_code (%d)", pr.ErrorCode)

	if pr.HighWatermark, err = pd.GetInt64(); err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return decodingErr.WithAddedContext("high_watermark")
		}
		return err
	}
	protocol.LogWithIndentation(logger, indentation, "- .high_watermark (%d)", pr.HighWatermark)

	if pr.LastStableOffset, err = pd.GetInt64(); err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return decodingErr.WithAddedContext("last_stable_offset")
		}
		return err
	}
	protocol.LogWithIndentation(logger, indentation, "- .last_stable_offset (%d)", pr.LastStableOffset)

	if pr.LogStartOffset, err = pd.GetInt64(); err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return decodingErr.WithAddedContext("log_start_offset")
		}
		return err
	}
	protocol.LogWithIndentation(logger, indentation, "- .log_start_offset (%d)", pr.LogStartOffset)

	numAbortedTransactions, err := pd.GetCompactArrayLength()
	if err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return decodingErr.WithAddedContext("num_aborted_transactions")
		}
		return err
	}
	protocol.LogWithIndentation(logger, indentation, "- .num_aborted_transactions (%d)", numAbortedTransactions)

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
	protocol.LogWithIndentation(logger, indentation, "- .preferred_read_replica (%d)", pr.PreferedReadReplica)

	// Record Batches are encoded as COMPACT_RECORDS
	numBytes, err := pd.GetCompactArrayLength()
	if err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return decodingErr.WithAddedContext("compact_records_length")
		}
		return err
	}
	protocol.LogWithIndentation(logger, indentation, "- .compact_records_length (%d)", numBytes)

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

		// Adding back 12 bytes before subtracting from numBytes
		// because BatchLength excludes BaseOffset (8 bytes) and itself (4 bytes)
		numBytes -= int(recordBatch.BatchLength + 12)
		k++
	}
	_, err = pd.GetEmptyTaggedFieldArray()
	if err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return decodingErr.WithAddedContext("TAG_BUFFER")
		}
		return err
	}
	protocol.LogWithIndentation(logger, indentation, "- .TAG_BUFFER")

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
	protocol.LogWithIndentation(logger, indentation, "- .producer_id (%d)", ab.ProducerID)

	if ab.FirstOffset, err = pd.GetInt64(); err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return decodingErr.WithAddedContext("first_offset")
		}
		return err
	}
	protocol.LogWithIndentation(logger, indentation, "- .first_offset (%d)", ab.FirstOffset)

	return nil
}
