package kafkaapi

import (
	"fmt"

	"github.com/codecrafters-io/kafka-tester/protocol"
	"github.com/codecrafters-io/kafka-tester/protocol/api/headers"
	"github.com/codecrafters-io/kafka-tester/protocol/decoder"
	"github.com/codecrafters-io/kafka-tester/protocol/errors"
	"github.com/codecrafters-io/tester-utils/logger"
)

type RecordError struct {
	BatchIndex             int32
	BatchIndexErrorMessage *string
}

func (r *RecordError) decode(rd *decoder.Decoder, logger *logger.Logger, indentation int) error {
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
			return decodingErr.WithAddedContext("_tagged_fields")
		}
		return err
	}
	protocol.LogWithIndentation(logger, indentation, "- ._tagged_fields")
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

func (p *ProducePartitionResponse) decode(rd *decoder.Decoder, logger *logger.Logger, indentation int) error {
	index, err := rd.GetInt32()
	if err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return decodingErr.WithAddedContext("index")
		}
		return err
	}
	p.Index = index
	protocol.LogWithIndentation(logger, indentation, "- .index (%d)", index)

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
	for i := range recordErrorsLength {
		protocol.LogWithIndentation(logger, indentation, "- .record_errors[%d]", i)
		err = p.RecordErrors[i].decode(rd, logger, indentation+1)
		if err != nil {
			if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
				return decodingErr.WithAddedContext(fmt.Sprintf("record_errors[%d]", i))
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
			return decodingErr.WithAddedContext("_tagged_fields")
		}
		return err
	}
	protocol.LogWithIndentation(logger, indentation, "- ._tagged_fields")

	return nil
}

type ProduceTopicResponse struct {
	Name               string
	PartitionResponses []ProducePartitionResponse
}

func (t *ProduceTopicResponse) decode(rd *decoder.Decoder, logger *logger.Logger, indentation int) error {
	name, err := rd.GetCompactString()
	if err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return decodingErr.WithAddedContext("name")
		}
		return err
	}
	t.Name = name
	protocol.LogWithIndentation(logger, indentation, "- .name (%s)", name)

	partitionResponsesLength, err := rd.GetCompactArrayLength()
	if err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return decodingErr.WithAddedContext("partition_responses.length")
		}
		return err
	}
	protocol.LogWithIndentation(logger, indentation, "- .partition_responses.length (%d)", partitionResponsesLength)

	t.PartitionResponses = make([]ProducePartitionResponse, partitionResponsesLength)
	for i := range partitionResponsesLength {
		protocol.LogWithIndentation(logger, indentation, "- .partition_responses[%d]", i)
		err = t.PartitionResponses[i].decode(rd, logger, indentation+1)
		if err != nil {
			if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
				return decodingErr.WithAddedContext(fmt.Sprintf("partition_responses[%d]", i))
			}
			return err
		}
	}

	_, err = rd.GetEmptyTaggedFieldArray()
	if err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return decodingErr.WithAddedContext("_tagged_fields")
		}
		return err
	}
	protocol.LogWithIndentation(logger, indentation, "- ._tagged_fields")
	return nil
}

type ProduceResponseBody struct {
	TopicResponses []ProduceTopicResponse
	ThrottleTimeMs int32
}

func (p *ProduceResponseBody) decode(rd *decoder.Decoder, logger *logger.Logger, indentation int) error {
	responsesLength, err := rd.GetCompactArrayLength()
	if err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return decodingErr.WithAddedContext("responses.length")
		}
		return err
	}
	protocol.LogWithIndentation(logger, indentation, "- .responses.length (%d)", responsesLength)

	p.TopicResponses = make([]ProduceTopicResponse, responsesLength)
	for i := range responsesLength {
		protocol.LogWithIndentation(logger, indentation, "- .responses[%d]", i)
		err = p.TopicResponses[i].decode(rd, logger, indentation+1)
		if err != nil {
			if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
				return decodingErr.WithAddedContext(fmt.Sprintf("responses[%d]", i))
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
			return decodingErr.WithAddedContext("_tagged_fields")
		}
		return err
	}
	protocol.LogWithIndentation(logger, indentation, "- ._tagged_fields")

	// Check if there are any remaining bytes in the decoder
	if rd.Remaining() != 0 {
		return errors.NewPacketDecodingError(fmt.Sprintf("unexpected %d bytes remaining in decoder after decoding ProduceResponseBody", rd.Remaining()))
	}

	return nil
}

type ProduceResponse struct {
	Header headers.ResponseHeader
	Body   ProduceResponseBody
}

func (r *ProduceResponse) Decode(response []byte, logger *logger.Logger) error {
	decoder := decoder.Decoder{}
	decoder.Init(response)
	logger.UpdateLastSecondaryPrefix("Decoder")
	defer logger.ResetSecondaryPrefixes()

	logger.Debugf("- .ResponseHeader")
	if err := r.Header.Decode(&decoder, logger, 1); err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			detailedError := decodingErr.WithAddedContext("Response Header").WithAddedContext("Produce Response v11")
			return decoder.FormatDetailedError(detailedError.Error())
		}
		return err
	}

	logger.Debugf("- .ResponseBody")
	if err := r.Body.decode(&decoder, logger, 1); err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			detailedError := decodingErr.WithAddedContext("Response Body").WithAddedContext("Produce Response v11")
			return decoder.FormatDetailedError(detailedError.Error())
		}
		return err
	}

	return nil
}
