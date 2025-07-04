package kafkaapi

import (
	"github.com/codecrafters-io/kafka-tester/protocol/decoder"
	"github.com/codecrafters-io/tester-utils/logger"
)

type RecordError struct {
	BatchIndex             int32
	BatchIndexErrorMessage *string
}

func (r *RecordError) Decode(rd *decoder.RealDecoder, logger *logger.Logger, indentation int) error {
	batchIndex, err := rd.GetInt32()
	if err != nil {
		return err
	}
	r.BatchIndex = batchIndex

	batchIndexErrorMessage, err := rd.GetCompactNullableString()
	if err != nil {
		return err
	}
	r.BatchIndexErrorMessage = batchIndexErrorMessage

	_, err = rd.GetEmptyTaggedFieldArray()
	return err
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
		return err
	}
	p.Index = index

	errorCode, err := rd.GetInt16()
	if err != nil {
		return err
	}
	p.ErrorCode = errorCode

	baseOffset, err := rd.GetInt64()
	if err != nil {
		return err
	}
	p.BaseOffset = baseOffset

	logAppendTimeMs, err := rd.GetInt64()
	if err != nil {
		return err
	}
	p.LogAppendTimeMs = logAppendTimeMs

	logStartOffset, err := rd.GetInt64()
	if err != nil {
		return err
	}
	p.LogStartOffset = logStartOffset

	recordErrorsLength, err := rd.GetCompactArrayLength()
	if err != nil {
		return err
	}

	p.RecordErrors = make([]RecordError, recordErrorsLength)
	for i := 0; i < recordErrorsLength; i++ {
		err = p.RecordErrors[i].Decode(rd, logger, indentation+1)
		if err != nil {
			return err
		}
	}

	errorMessage, err := rd.GetCompactNullableString()
	if err != nil {
		return err
	}
	p.ErrorMessage = errorMessage

	_, err = rd.GetEmptyTaggedFieldArray()
	return err
}

type ProduceTopicResponse struct {
	Name               string
	PartitionResponses []ProducePartitionResponse
}

func (t *ProduceTopicResponse) Decode(rd *decoder.RealDecoder, logger *logger.Logger, indentation int) error {
	name, err := rd.GetCompactString()
	if err != nil {
		return err
	}
	t.Name = name

	partitionResponsesLength, err := rd.GetCompactArrayLength()
	if err != nil {
		return err
	}

	t.PartitionResponses = make([]ProducePartitionResponse, partitionResponsesLength)
	for i := 0; i < partitionResponsesLength; i++ {
		err = t.PartitionResponses[i].Decode(rd, logger, indentation+1)
		if err != nil {
			return err
		}
	}

	_, err = rd.GetEmptyTaggedFieldArray()
	return err
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
		return err
	}

	p.Responses = make([]ProduceTopicResponse, responsesLength)
	for i := 0; i < responsesLength; i++ {
		err = p.Responses[i].Decode(rd, logger, indentation+1)
		if err != nil {
			return err
		}
	}

	throttleTimeMs, err := rd.GetInt32()
	if err != nil {
		return err
	}
	p.ThrottleTimeMs = throttleTimeMs

	_, err = rd.GetEmptyTaggedFieldArray()
	return err
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
