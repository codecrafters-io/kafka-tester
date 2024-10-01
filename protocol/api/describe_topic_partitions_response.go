package kafkaapi

import (
	"fmt"

	"github.com/codecrafters-io/kafka-tester/protocol"
	"github.com/codecrafters-io/kafka-tester/protocol/decoder"
	"github.com/codecrafters-io/kafka-tester/protocol/encoder"
	"github.com/codecrafters-io/kafka-tester/protocol/errors"
	"github.com/codecrafters-io/tester-utils/logger"
)

type DescribeTopicPartitionsResponse struct {
	ThrottleTimeMs int32
	Topics         []DescribeTopicPartitionsResponseTopic
	NextCursor     DescribeTopicPartitionsResponseCursor
}

func (a *DescribeTopicPartitionsResponse) Decode(pd *decoder.RealDecoder, logger *logger.Logger, indentation int) (err error) {
	a.ThrottleTimeMs, err = pd.GetInt32()
	if err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return decodingErr.WithAddedContext("throttle_time_ms")
		}
		return err
	}
	protocol.LogWithIndentation(logger, indentation, "- .throttle_time_ms (%d)", a.ThrottleTimeMs)

	var numTopics int
	if numTopics, err = pd.GetCompactArrayLength(); err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return decodingErr.WithAddedContext("topic.length")
		}
		return err
	}
	protocol.LogWithIndentation(logger, indentation, "- .topic.length (%d)", numTopics)

	a.Topics = make([]DescribeTopicPartitionsResponseTopic, numTopics)

	for i := 0; i < numTopics; i++ {
		var topic DescribeTopicPartitionsResponseTopic
		protocol.LogWithIndentation(logger, indentation, "- .Topics[%d]", i)
		if err = topic.Decode(pd, logger, indentation+1); err != nil {
			if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
				return decodingErr.WithAddedContext(fmt.Sprintf("Topics[%d]", i))
			}
			return err
		}
		a.Topics[i] = topic
	}

	a.NextCursor = DescribeTopicPartitionsResponseCursor{}
	err = a.NextCursor.Decode(pd, logger, indentation)
	if err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return decodingErr.WithAddedContext("next_cursor")
		}
		return err
	}

	if _, err := pd.GetEmptyTaggedFieldArray(); err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return decodingErr.WithAddedContext("TAG_BUFFER")
		}
		return err
	}
	protocol.LogWithIndentation(logger, indentation, "- .TAG_BUFFER")

	return nil
}

type DescribeTopicPartitionsResponseTopic struct {
	ErrorCode                 int16
	Name                      string
	TopicID                   string
	IsInternal                bool
	Partitions                []DescribeTopicPartitionsResponsePartition
	TopicAuthorizedOperations int32
}

func (a *DescribeTopicPartitionsResponseTopic) Decode(pd *decoder.RealDecoder, logger *logger.Logger, indentation int) (err error) {
	a.ErrorCode, err = pd.GetInt16()
	if err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return decodingErr.WithAddedContext("error_code")
		}
		return err
	}
	protocol.LogWithIndentation(logger, indentation, "- .error_code (%d)", a.ErrorCode)

	name, err := pd.GetCompactNullableString()
	if err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return decodingErr.WithAddedContext("name")
		}
		return err
	}
	a.Name = *name
	protocol.LogWithIndentation(logger, indentation, "- .name (%s)", a.Name)

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
	a.TopicID = topicUUID
	protocol.LogWithIndentation(logger, indentation, "- .topic_id (%s)", a.TopicID)

	a.IsInternal, err = pd.GetBool()
	if err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return decodingErr.WithAddedContext("is_internal")
		}
		return err
	}
	protocol.LogWithIndentation(logger, indentation, "- .is_internal (%t)", a.IsInternal)

	var numPartitions int
	if numPartitions, err = pd.GetCompactArrayLength(); err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return decodingErr.WithAddedContext("num_partitions")
		}
		return err
	}
	protocol.LogWithIndentation(logger, indentation, "- .num_partitions (%d)", numPartitions)

	a.Partitions = make([]DescribeTopicPartitionsResponsePartition, numPartitions)
	for i := 0; i < numPartitions; i++ {
		var partition DescribeTopicPartitionsResponsePartition
		protocol.LogWithIndentation(logger, indentation, "- .Partitions[%d]", i)
		if err = partition.Decode(pd, logger, indentation+1); err != nil {
			if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
				return decodingErr.WithAddedContext(fmt.Sprintf("Partitions[%d]", i))
			}
			return err
		}
		a.Partitions[i] = partition
	}

	topicAuthorizedOperations, err := pd.GetInt32()
	if err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return decodingErr.WithAddedContext("topic_authorized_operations")
		}
		return err
	}
	a.TopicAuthorizedOperations = topicAuthorizedOperations
	protocol.LogWithIndentation(logger, indentation, "- .topic_authorized_operations (%d)", a.TopicAuthorizedOperations)

	if _, err := pd.GetEmptyTaggedFieldArray(); err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return decodingErr.WithAddedContext("TAG_BUFFER")
		}
		return err
	}
	protocol.LogWithIndentation(logger, indentation, "- .TAG_BUFFER")

	return nil
}

type DescribeTopicPartitionsResponsePartition struct {
	ErrorCode              int16
	PartitionIndex         int32
	LeaderID               int32
	LeaderEpoch            int32
	ReplicaNodes           []int32
	IsrNodes               []int32
	EligibleLeaderReplicas []int32
	LastKnownELR           []int32
	OfflineReplicas        []int32
}

func (d *DescribeTopicPartitionsResponsePartition) Decode(pd *decoder.RealDecoder, logger *logger.Logger, indentation int) (err error) {
	d.ErrorCode, err = pd.GetInt16()
	if err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return decodingErr.WithAddedContext("error_code")
		}
		return err
	}
	protocol.LogWithIndentation(logger, indentation, "- .error_code (%d)", d.ErrorCode)

	d.PartitionIndex, err = pd.GetInt32()
	if err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return decodingErr.WithAddedContext("partition_index")
		}
		return err
	}
	protocol.LogWithIndentation(logger, indentation, "- .partition_index (%d)", d.PartitionIndex)

	d.LeaderID, err = pd.GetInt32()
	if err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return decodingErr.WithAddedContext("leader_id")
		}
		return err
	}
	protocol.LogWithIndentation(logger, indentation, "- .leader_id (%d)", d.LeaderID)

	d.LeaderEpoch, err = pd.GetInt32()
	if err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return decodingErr.WithAddedContext("leader_epoch")
		}
		return err
	}
	protocol.LogWithIndentation(logger, indentation, "- .leader_epoch (%d)", d.LeaderEpoch)

	if d.ReplicaNodes, err = pd.GetCompactInt32Array(); err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return decodingErr.WithAddedContext("replica_nodes")
		}
		return err
	}
	protocol.LogWithIndentation(logger, indentation, "- .replica_nodes (%v)", d.ReplicaNodes)

	if d.IsrNodes, err = pd.GetCompactInt32Array(); err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return decodingErr.WithAddedContext("isr_nodes")
		}
		return err
	}
	protocol.LogWithIndentation(logger, indentation, "- .isr_nodes (%v)", d.IsrNodes)

	if d.EligibleLeaderReplicas, err = pd.GetCompactInt32Array(); err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return decodingErr.WithAddedContext("eligible_leader_replicas")
		}
		return err
	}
	protocol.LogWithIndentation(logger, indentation, "- .eligible_leader_replicas (%v)", d.EligibleLeaderReplicas)

	if d.LastKnownELR, err = pd.GetCompactInt32Array(); err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return decodingErr.WithAddedContext("last_known_elr")
		}
		return err
	}
	protocol.LogWithIndentation(logger, indentation, "- .last_known_elr (%v)", d.LastKnownELR)

	if d.OfflineReplicas, err = pd.GetCompactInt32Array(); err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return decodingErr.WithAddedContext("offline_replicas")
		}
		return err
	}
	protocol.LogWithIndentation(logger, indentation, "- .offline_replicas (%v)", d.OfflineReplicas)

	if _, err := pd.GetEmptyTaggedFieldArray(); err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return decodingErr.WithAddedContext("TAG_BUFFER")
		}
		return err
	}
	protocol.LogWithIndentation(logger, indentation, "- .TAG_BUFFER")

	return nil
}

type DescribeTopicPartitionsResponseCursor struct {
	TopicName      string
	PartitionIndex int32
}

// String implements the Stringer interface for the Cursor type
func (c DescribeTopicPartitionsResponseCursor) String() string {
	if c.TopicName == "" && c.PartitionIndex == 0 {
		return "null"
	}
	return fmt.Sprintf("{TopicName: %s, PartitionIndex: %d}", c.TopicName, c.PartitionIndex)
}

func (c *DescribeTopicPartitionsResponseCursor) Decode(pd *decoder.RealDecoder, logger *logger.Logger, indentation int) error {
	var err error

	// This field is nullable, the first byte indicates whether it's null or not
	checkPresence, err := pd.GetInt8()
	if err != nil {
		if _, ok := err.(*errors.PacketDecodingError); ok {
			customError := errors.NewPacketDecodingError(fmt.Sprintf("Expected either 0xFF (cursor is null) or 0x01 (cursor is not null), got 0x%02X", uint8(checkPresence)), "RAW_BYTES")
			return customError.WithAddedContext("cursor_is_null")
		}
		return err
	}

	if checkPresence == -1 {
		protocol.LogWithIndentation(logger, indentation, "- .next_cursor (null)")
		c = nil
		return nil
	} else {
		protocol.LogWithIndentation(logger, indentation, "- .next_cursor")
	}

	if c.TopicName, err = pd.GetCompactString(); err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return decodingErr.WithAddedContext("topic_name")
		}
		return err
	}
	protocol.LogWithIndentation(logger, indentation+1, "- .topic_name (%s)", c.TopicName)

	if c.PartitionIndex, err = pd.GetInt32(); err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return decodingErr.WithAddedContext("partition_index")
		}
		return err
	}
	protocol.LogWithIndentation(logger, indentation+1, "- .partition_index (%d)", c.PartitionIndex)

	if _, err := pd.GetEmptyTaggedFieldArray(); err != nil {
		if decodingErr, ok := err.(*errors.PacketDecodingError); ok {
			return decodingErr.WithAddedContext("TAG_BUFFER")
		}
		return err
	}
	protocol.LogWithIndentation(logger, indentation+1, "- .TAG_BUFFER")
	return nil
}
