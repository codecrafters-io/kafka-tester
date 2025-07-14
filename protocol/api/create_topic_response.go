package kafkaapi

import (
	"fmt"

	"github.com/codecrafters-io/kafka-tester/protocol"
	"github.com/codecrafters-io/kafka-tester/protocol/decoder"
	"github.com/codecrafters-io/kafka-tester/protocol/errors"
	"github.com/codecrafters-io/tester-utils/logger"
)

// Structure
// CreateTopics Response (Version: 6) => throttle_time_ms [topics] _tagged_fields
//   throttle_time_ms => INT32
//   topics => name error_code error_message num_partitions replication_factor [configs] _tagged_fields
//     name => COMPACT_STRING
//     error_code => INT16
//     error_message => COMPACT_NULLABLE_STRING
//     num_partitions => INT32
//     replication_factor => INT16
//     configs => name value read_only config_source is_sensitive _tagged_fields
//       name => COMPACT_STRING
//       value => COMPACT_NULLABLE_STRING
//       read_only => BOOLEAN
//       config_source => INT8
//       is_sensitive => BOOLEAN

type CreateTopicResponseBody struct {
	ThrottleTimeMs int32
	TopicErrors    []TopicError
}

func (ct *CreateTopicResponseBody) Decode(rd *decoder.RealDecoder, version int16, logger *logger.Logger, indentation int) error {
	throttleTimeMs, err := rd.GetInt32()
	if err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return decodingErr.WithAddedContext("throttle_time_ms")
		}
		return err
	}
	ct.ThrottleTimeMs = throttleTimeMs
	protocol.LogWithIndentation(logger, indentation, "- .throttle_time_ms (%d)", throttleTimeMs)

	topicErrorsLength, err := rd.GetCompactArrayLength()
	if err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return decodingErr.WithAddedContext("topics.length")
		}
		return err
	}
	protocol.LogWithIndentation(logger, indentation, "- .topics.length (%d)", topicErrorsLength)

	ct.TopicErrors = make([]TopicError, topicErrorsLength)
	for i := 0; i < topicErrorsLength; i++ {
		protocol.LogWithIndentation(logger, indentation, "- .topics[%d]", i)
		err = ct.TopicErrors[i].Decode(rd, logger, indentation+1)
		if err != nil {
			if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
				return decodingErr.WithAddedContext(fmt.Sprintf("topics[%d]", i))
			}
			return err
		}
	}

	_, err = rd.GetEmptyTaggedFieldArray()
	if err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return decodingErr.WithAddedContext("TAG_BUFFER")
		}
		return err
	}
	protocol.LogWithIndentation(logger, indentation, "- .TAG_BUFFER")

	return nil
}

type TopicError struct {
	TopicName         string
	ErrorCode         int16
	ErrorMessage      *string
	NumPartitions     int32
	ReplicationFactor int16
	Configs           []TopicConfig
}

type TopicConfig struct {
	Name         string
	Value        *string
	ReadOnly     bool
	ConfigSource int8
	IsSensitive  bool
}

func (tc *TopicConfig) Decode(rd *decoder.RealDecoder, logger *logger.Logger, indentation int) error {
	name, err := rd.GetCompactString()
	if err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return decodingErr.WithAddedContext("config_name")
		}
		return err
	}
	tc.Name = name
	protocol.LogWithIndentation(logger, indentation, "- .config_name (%s)", name)

	value, err := rd.GetCompactNullableString()
	if err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return decodingErr.WithAddedContext("config_value")
		}
		return err
	}
	tc.Value = value
	if value != nil {
		protocol.LogWithIndentation(logger, indentation, "- .config_value (%s)", *value)
	} else {
		protocol.LogWithIndentation(logger, indentation, "- .config_value (null)")
	}

	readOnly, err := rd.GetBool()
	if err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return decodingErr.WithAddedContext("read_only")
		}
		return err
	}
	tc.ReadOnly = readOnly
	protocol.LogWithIndentation(logger, indentation, "- .read_only (%t)", readOnly)

	configSource, err := rd.GetInt8()
	if err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return decodingErr.WithAddedContext("config_source")
		}
		return err
	}
	tc.ConfigSource = configSource
	protocol.LogWithIndentation(logger, indentation, "- .config_source (%d)", configSource)

	isSensitive, err := rd.GetBool()
	if err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return decodingErr.WithAddedContext("is_sensitive")
		}
		return err
	}
	tc.IsSensitive = isSensitive
	protocol.LogWithIndentation(logger, indentation, "- .is_sensitive (%t)", isSensitive)

	_, err = rd.GetEmptyTaggedFieldArray()
	if err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return decodingErr.WithAddedContext("TAG_BUFFER")
		}
		return err
	}
	protocol.LogWithIndentation(logger, indentation, "- .TAG_BUFFER")
	return nil
}

func (te *TopicError) Decode(rd *decoder.RealDecoder, logger *logger.Logger, indentation int) error {
	topicName, err := rd.GetCompactString()
	if err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return decodingErr.WithAddedContext("topic_name")
		}
		return err
	}
	te.TopicName = topicName
	protocol.LogWithIndentation(logger, indentation, "- .topic_name (%s)", topicName)

	errorCode, err := rd.GetInt16()
	if err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return decodingErr.WithAddedContext("error_code")
		}
		return err
	}
	te.ErrorCode = errorCode
	protocol.LogWithIndentation(logger, indentation, "- .error_code (%d)", errorCode)

	errorMessage, err := rd.GetCompactNullableString()
	if err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return decodingErr.WithAddedContext("error_message")
		}
		return err
	}
	te.ErrorMessage = errorMessage
	if errorMessage != nil {
		protocol.LogWithIndentation(logger, indentation, "- .error_message (%s)", *errorMessage)
	} else {
		protocol.LogWithIndentation(logger, indentation, "- .error_message (null)")
	}

	numPartitions, err := rd.GetInt32()
	if err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return decodingErr.WithAddedContext("num_partitions")
		}
		return err
	}
	te.NumPartitions = numPartitions
	protocol.LogWithIndentation(logger, indentation, "- .num_partitions (%d)", numPartitions)

	replicationFactor, err := rd.GetInt16()
	if err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return decodingErr.WithAddedContext("replication_factor")
		}
		return err
	}
	te.ReplicationFactor = replicationFactor
	protocol.LogWithIndentation(logger, indentation, "- .replication_factor (%d)", replicationFactor)

	configsLength, err := rd.GetCompactArrayLength()
	if err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return decodingErr.WithAddedContext("configs.length")
		}
		return err
	}
	protocol.LogWithIndentation(logger, indentation, "- .configs.length (%d)", configsLength)

	te.Configs = make([]TopicConfig, configsLength)
	for i := 0; i < configsLength; i++ {
		protocol.LogWithIndentation(logger, indentation, "- .configs[%d]", i)
		err = te.Configs[i].Decode(rd, logger, indentation+1)
		if err != nil {
			if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
				return decodingErr.WithAddedContext(fmt.Sprintf("configs[%d]", i))
			}
			return err
		}
	}

	_, err = rd.GetEmptyTaggedFieldArray()
	if err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return decodingErr.WithAddedContext("TAG_BUFFER")
		}
		return err
	}
	protocol.LogWithIndentation(logger, indentation, "- .TAG_BUFFER")
	return nil
}

type RecordError struct {
	BatchIndex            int32
	BatchIndexErrorMessage *string
}

func (r *RecordError) Decode(rd *decoder.RealDecoder, logger *logger.Logger, indentation int) error {
	batchIndex, err := rd.GetInt32()
	if err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return decodingErr.WithAddedContext("batch_index")
		}
		return err
	}
	r.BatchIndex = batchIndex
	protocol.LogWithIndentation(logger, indentation, "- .batch_index (%d)", batchIndex)

	batchIndexErrorMessage, err := rd.GetCompactNullableString()
	if err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return decodingErr.WithAddedContext("batch_index_error_message")
		}
		return err
	}
	r.BatchIndexErrorMessage = batchIndexErrorMessage
	if batchIndexErrorMessage != nil {
		protocol.LogWithIndentation(logger, indentation, "- .batch_index_error_message (%s)", *batchIndexErrorMessage)
	} else {
		protocol.LogWithIndentation(logger, indentation, "- .batch_index_error_message (null)")
	}

	_, err = rd.GetEmptyTaggedFieldArray()
	if err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return decodingErr.WithAddedContext("TAG_BUFFER")
		}
		return err
	}
	protocol.LogWithIndentation(logger, indentation, "- .TAG_BUFFER")
	return nil
}

type ProducePartitionResponse struct {
	Index     int32
	ErrorCode int16
	// For invalid partitions, would be -1
	BaseOffset      int64 // Number of existing logs in the partition's log file
	LogAppendTimeMs int64
	// For fresh valid partitions (without compaction or truncation), would be 0
	// For invalid partitions, would be -1
	LogStartOffset int64 // Earliest available offset in the partition's log file
	RecordErrors   []RecordError
	ErrorMessage   *string
}

func (p *ProducePartitionResponse) Decode(rd *decoder.RealDecoder, logger *logger.Logger, indentation int) error {
	index, err := rd.GetInt32()
	if err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return decodingErr.WithAddedContext("partition_index")
		}
		return err
	}
	p.Index = index
	protocol.LogWithIndentation(logger, indentation, "- .partition_index (%d)", index)

	errorCode, err := rd.GetInt16()
	if err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return decodingErr.WithAddedContext("error_code")
		}
		return err
	}
	p.ErrorCode = errorCode
	protocol.LogWithIndentation(logger, indentation, "- .error_code (%d)", errorCode)

	baseOffset, err := rd.GetInt64()
	if err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return decodingErr.WithAddedContext("base_offset")
		}
		return err
	}
	p.BaseOffset = baseOffset
	protocol.LogWithIndentation(logger, indentation, "- .base_offset (%d)", baseOffset)

	logAppendTimeMs, err := rd.GetInt64()
	if err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return decodingErr.WithAddedContext("log_append_time_ms")
		}
		return err
	}
	p.LogAppendTimeMs = logAppendTimeMs
	protocol.LogWithIndentation(logger, indentation, "- .log_append_time_ms (%d)", logAppendTimeMs)

	logStartOffset, err := rd.GetInt64()
	if err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return decodingErr.WithAddedContext("log_start_offset")
		}
		return err
	}
	p.LogStartOffset = logStartOffset
	protocol.LogWithIndentation(logger, indentation, "- .log_start_offset (%d)", logStartOffset)

	recordErrorsLength, err := rd.GetCompactArrayLength()
	if err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return decodingErr.WithAddedContext("record_errors.length")
		}
		return err
	}
	protocol.LogWithIndentation(logger, indentation, "- .record_errors.length (%d)", recordErrorsLength)

	p.RecordErrors = make([]RecordError, recordErrorsLength)
	for i := 0; i < recordErrorsLength; i++ {
		protocol.LogWithIndentation(logger, indentation, "- .RecordErrors[%d]", i)
		err = p.RecordErrors[i].Decode(rd, logger, indentation+1)
		if err != nil {
			if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
				return decodingErr.WithAddedContext(fmt.Sprintf("RecordErrors[%d]", i))
			}
			return err
		}
	}

	errorMessage, err := rd.GetCompactNullableString()
	if err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return decodingErr.WithAddedContext("error_message")
		}
		return err
	}
	p.ErrorMessage = errorMessage
	if errorMessage != nil {
		protocol.LogWithIndentation(logger, indentation, "- .error_message (%s)", *errorMessage)
	} else {
		protocol.LogWithIndentation(logger, indentation, "- .error_message (null)")
	}

	_, err = rd.GetEmptyTaggedFieldArray()
	if err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return decodingErr.WithAddedContext("TAG_BUFFER")
		}
		return err
	}
	protocol.LogWithIndentation(logger, indentation, "- .TAG_BUFFER")

	return nil
}

type ProduceTopicResponse struct {
	Name               string
	PartitionResponses []ProducePartitionResponse
}

func (t *ProduceTopicResponse) Decode(rd *decoder.RealDecoder, logger *logger.Logger, indentation int) error {
	name, err := rd.GetCompactString()
	if err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return decodingErr.WithAddedContext("topic_name")
		}
		return err
	}
	t.Name = name
	protocol.LogWithIndentation(logger, indentation, "- .topic_name (%s)", name)

	partitionResponsesLength, err := rd.GetCompactArrayLength()
	if err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return decodingErr.WithAddedContext("partition_responses.length")
		}
		return err
	}
	protocol.LogWithIndentation(logger, indentation, "- .partition_responses.length (%d)", partitionResponsesLength)

	t.PartitionResponses = make([]ProducePartitionResponse, partitionResponsesLength)
	for i := 0; i < partitionResponsesLength; i++ {
		protocol.LogWithIndentation(logger, indentation, "- .PartitionResponses[%d]", i)
		err = t.PartitionResponses[i].Decode(rd, logger, indentation+1)
		if err != nil {
			if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
				return decodingErr.WithAddedContext(fmt.Sprintf("PartitionResponses[%d]", i))
			}
			return err
		}
	}

	_, err = rd.GetEmptyTaggedFieldArray()
	if err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return decodingErr.WithAddedContext("TAG_BUFFER")
		}
		return err
	}
	protocol.LogWithIndentation(logger, indentation, "- .TAG_BUFFER")
	return nil
}

type ProduceResponseBody struct {
	Version        int16
	Responses      []ProduceTopicResponse
	ThrottleTimeMs int32
}

func (p *ProduceResponseBody) Decode(rd *decoder.RealDecoder, version int16, logger *logger.Logger, indentation int) error {
	p.Version = version

	responsesLength, err := rd.GetCompactArrayLength()
	if err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return decodingErr.WithAddedContext("responses.length")
		}
		return err
	}
	protocol.LogWithIndentation(logger, indentation, "- .responses.length (%d)", responsesLength)

	p.Responses = make([]ProduceTopicResponse, responsesLength)
	for i := 0; i < responsesLength; i++ {
		protocol.LogWithIndentation(logger, indentation, "- .Responses[%d]", i)
		err = p.Responses[i].Decode(rd, logger, indentation+1)
		if err != nil {
			if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
				return decodingErr.WithAddedContext(fmt.Sprintf("Responses[%d]", i))
			}
			return err
		}
	}

	throttleTimeMs, err := rd.GetInt32()
	if err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return decodingErr.WithAddedContext("throttle_time_ms")
		}
		return err
	}
	p.ThrottleTimeMs = throttleTimeMs
	protocol.LogWithIndentation(logger, indentation, "- .throttle_time_ms (%d)", throttleTimeMs)

	_, err = rd.GetEmptyTaggedFieldArray()
	if err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return decodingErr.WithAddedContext("TAG_BUFFER")
		}
		return err
	}
	protocol.LogWithIndentation(logger, indentation, "- .TAG_BUFFER")

	// Check if there are any remaining bytes in the decoder
	if rd.Remaining() != 0 {
		return errors.NewPacketDecodingError(fmt.Sprintf("unexpected %d bytes remaining in decoder after decoding ProduceResponseBody", rd.Remaining()))
	}

	return nil
}

type CreateTopicResponse struct {
	Header ResponseHeader
	Body   CreateTopicResponseBody
}

func DecodeCreateTopicResponse(payload []byte, version int16, logger *logger.Logger) (*CreateTopicResponse, error) {
	rd := decoder.RealDecoder{}
	rd.Init(payload)

	response := &CreateTopicResponse{}

	logger.UpdateSecondaryPrefix("Decoder")
	logger.Debugf("- .ResponseHeader")
	err := response.Header.DecodeV1(&rd, logger, 1)
	if err != nil {
		return nil, err
	}

	logger.Debugf("- .CreateTopicResponseBody")
	err = response.Body.Decode(&rd, version, logger, 1)
	if err != nil {
		return nil, err
	}

	logger.ResetSecondaryPrefix()
	return response, nil
}

type ProduceResponse struct {
	Header ResponseHeader
	Body   ProduceResponseBody
}
