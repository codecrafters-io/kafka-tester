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
	ThrottleTimeMs            int32
	CreateTopicTopicResponses []CreateTopicTopicResponse
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

	topicResponsesLength, err := rd.GetCompactArrayLength()
	if err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return decodingErr.WithAddedContext("topics.length")
		}
		return err
	}
	protocol.LogWithIndentation(logger, indentation, "- .topics.length (%d)", topicResponsesLength)

	ct.CreateTopicTopicResponses = make([]CreateTopicTopicResponse, topicResponsesLength)
	for i := range topicResponsesLength {
		protocol.LogWithIndentation(logger, indentation, "- .topics[%d]", i)
		err = ct.CreateTopicTopicResponses[i].Decode(rd, logger, indentation+1)
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

type CreateTopicTopicResponse struct {
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

func (t *CreateTopicTopicResponse) Decode(rd *decoder.RealDecoder, logger *logger.Logger, indentation int) error {
	name, err := rd.GetCompactString()
	if err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return decodingErr.WithAddedContext("topic_name")
		}
		return err
	}
	t.TopicName = name
	protocol.LogWithIndentation(logger, indentation, "- .topic_name (%s)", name)

	errorCode, err := rd.GetInt16()
	if err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return decodingErr.WithAddedContext("error_code")
		}
		return err
	}
	t.ErrorCode = errorCode
	protocol.LogWithIndentation(logger, indentation, "- .error_code (%d)", errorCode)

	errorMessage, err := rd.GetCompactNullableString()
	if err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return decodingErr.WithAddedContext("error_message")
		}
		return err
	}
	t.ErrorMessage = errorMessage
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
	t.NumPartitions = numPartitions
	protocol.LogWithIndentation(logger, indentation, "- .num_partitions (%d)", numPartitions)

	replicationFactor, err := rd.GetInt16()
	if err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return decodingErr.WithAddedContext("replication_factor")
		}
		return err
	}
	t.ReplicationFactor = replicationFactor
	protocol.LogWithIndentation(logger, indentation, "- .replication_factor (%d)", replicationFactor)

	configsLength, err := rd.GetCompactArrayLength()
	if err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return decodingErr.WithAddedContext("configs.length")
		}
		return err
	}
	protocol.LogWithIndentation(logger, indentation, "- .configs.length (%d)", configsLength)

	t.Configs = make([]TopicConfig, configsLength)
	for i := 0; i < configsLength; i++ {
		protocol.LogWithIndentation(logger, indentation, "- .configs[%d]", i)
		err = t.Configs[i].Decode(rd, logger, indentation+1)
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
