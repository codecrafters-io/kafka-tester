package kafkaapi

import (
	"fmt"

	"github.com/codecrafters-io/kafka-tester/protocol"
	"github.com/codecrafters-io/kafka-tester/protocol/decoder"
	"github.com/codecrafters-io/kafka-tester/protocol/errors"
	"github.com/codecrafters-io/tester-utils/logger"
)

type RecordError struct {
	BatchIndex             int32
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
	Index           int32
	ErrorCode       int16
	BaseOffset      int64
	LogAppendTimeMs int64
	LogStartOffset  int64
	RecordErrors    []RecordError
	ErrorMessage    *string
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

type ProduceResponse struct {
	Header ResponseHeader
	Body   ProduceResponseBody
}

// func DecodeProduceResponse(payload []byte, version int16, logger *logger.Logger) (*ProduceResponse, error) {
// 	rd := decoder.NewRealDecoder(payload)
// 	response := &ProduceResponse{}

// 	// Decode header
// 	if version >= 1 {
// 		err := response.Header.DecodeV1(rd, logger, 0)
// 		if err != nil {
// 			if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
// 				return nil, decodingErr.WithAddedContext("header")
// 			}
// 			return nil, err
// 		}
// 	} else {
// 		err := response.Header.DecodeV0(rd, logger, 0)
// 		if err != nil {
// 			if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
// 				return nil, decodingErr.WithAddedContext("header")
// 			}
// 			return nil, err
// 		}
// 	}

// 	// Decode body
// 	err := response.Body.Decode(rd, logger, 0)
// 	if err != nil {
// 		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
// 			return nil, decodingErr.WithAddedContext("body")
// 		}
// 		return nil, err
// 	}

// 	return response, nil
// }
