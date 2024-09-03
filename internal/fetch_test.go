package internal

import (
	"encoding/hex"
	"fmt"
	"testing"

	kafkaapi "github.com/codecrafters-io/kafka-tester/protocol/api"
	"github.com/codecrafters-io/kafka-tester/protocol/decoder"
	"github.com/stretchr/testify/assert"
)

func TestFetchv163messages1(t *testing.T) {
	hexdump := "0000000b0000000000000019ffd72602c2a21ee23db74b6cbcc32a051cc51fc90200000000000000000000000000030000000000000003000000000000000000ffffffffd90100000000000000000000003c00000000026eca8a5d0000000000000000019192c16b5d0000019192c16b5d0000000000000000000000000000000000011400000001086d7367310000000000000000010000003c0000000002ad286abc0000000000000000019192c170b70000019192c170b70000000000000000000000000001000000011400000001086d7367320000000000000000020000003c0000000002d0470b040000000000000000019192c176990000019192c176990000000000000000000000000002000000011400000001086d73673300000000"

	b, err := hex.DecodeString(hexdump)
	if err != nil {
		panic(err)
	}

	messages := []string{}
	decoder := decoder.RealDecoder{}
	decoder.Init(b)

	header := kafkaapi.ResponseHeader{}

	if err = header.DecodeV1(&decoder); err != nil {
		fmt.Println(decoder.FormatDetailedError(err.Error()))
		panic("I QUIT")
	}

	response := kafkaapi.FetchResponse{Version: 16}
	if err = response.Decode(&decoder, 16); err != nil {
		fmt.Println(decoder.FormatDetailedError(err.Error()))
		panic("I QUIT")
	}

	assert.NoError(t, err)

	assert.Equal(t, 11, int(header.CorrelationId))
	assert.Equal(t, 1, len(response.Responses))
	assert.Equal(t, "c2a21ee2-3db7-4b6c-bcc3-2a051cc51fc9", response.Responses[0].Topic)
	for _, partition := range response.Responses {
		assert.Equal(t, 1, len(partition.Partitions))
		assert.Equal(t, 0, int(partition.Partitions[0].PartitionIndex))
		assert.Equal(t, 0, int(partition.Partitions[0].ErrorCode))
		assert.Equal(t, 3, int(partition.Partitions[0].LastStableOffset))
		assert.Equal(t, 0, int(partition.Partitions[0].LogStartOffset))
		assert.Equal(t, 3, len(partition.Partitions[0].Records))
		for _, partition := range partition.Partitions {
			assert.Equal(t, 3, len(partition.Records))
			for _, record := range partition.Records {
				for _, message := range record.Records {
					messages = append(messages, string(message.Value))
				}
			}
		}
	}

	assert.Equal(t, []string{"msg1", "msg2", "msg3"}, messages)
}
