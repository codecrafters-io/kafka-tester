package internal

import (
	"encoding/hex"
	"fmt"
	"testing"

	kafkaapi "github.com/codecrafters-io/kafka-tester/protocol/api"
	"github.com/codecrafters-io/kafka-tester/protocol/decoder"
	"github.com/stretchr/testify/assert"
)

func TestFetchv16_0m(t *testing.T) {
	hexdump := "0000000b000000000000004fd5de7102c2a21ee23db74b6cbcc32a051cc51fc90200000000000000000000000000000000000000000000000000000000000000ffffffff01000000"

	b, err := hex.DecodeString(hexdump)
	if err != nil {
		panic(err)
	}

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
	assert.Equal(t, 0, int(response.ErrorCode))
	assert.Equal(t, 0, int(response.ThrottleTimeMs))
	assert.Equal(t, 1339416177, int(response.SessionID))
	assert.Equal(t, 1, len(response.Responses))
	assert.Equal(t, "c2a21ee2-3db7-4b6c-bcc3-2a051cc51fc9", response.Responses[0].Topic)
	assert.Equal(t, 0, len(response.Responses[0].Partitions[0].RecordBatches))

	for _, partition := range response.Responses {
		assert.Equal(t, 1, len(partition.Partitions))
		assert.Equal(t, 0, int(partition.Partitions[0].PartitionIndex))
		assert.Equal(t, 0, int(partition.Partitions[0].ErrorCode))
		assert.Equal(t, 0, int(partition.Partitions[0].LastStableOffset))
		assert.Equal(t, 0, int(partition.Partitions[0].LogStartOffset))
		assert.Equal(t, 0, len(partition.Partitions[0].RecordBatches))
	}
}

func TestFetchv16_1m(t *testing.T) {
	hexdump := "0000000b000000000000007a983da40282e9d296c41249f0bcc9b03cdcc4888a0200000000000000000000000000010000000000000001000000000000000000ffffffff4900000000000000000000003c0000000002efe33e7c00000000000000000191937e258d00000191937e258d0000000000000001000000000000000000011400000001086d73673100000000"

	b, err := hex.DecodeString(hexdump)
	if err != nil {
		panic(err)
	}

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
	messages := []string{}

	assert.Equal(t, 11, int(header.CorrelationId))
	assert.Equal(t, 0, int(response.ErrorCode))
	assert.Equal(t, 0, int(response.ThrottleTimeMs))
	assert.Equal(t, 2056797604, int(response.SessionID))
	assert.Equal(t, 1, len(response.Responses))
	assert.Equal(t, "82e9d296-c412-49f0-bcc9-b03cdcc4888a", response.Responses[0].Topic)

	for _, partition := range response.Responses {
		for _, partition := range partition.Partitions {
			assert.Equal(t, 0, int(partition.ErrorCode))
			assert.Equal(t, 1, int(partition.LastStableOffset))
			assert.Equal(t, 0, int(partition.LogStartOffset))
			assert.Equal(t, 1, len(partition.RecordBatches))
			assert.Equal(t, 0, int(partition.PartitionIndex))
			for _, recordBatch := range partition.RecordBatches {
				for _, message := range recordBatch.Records {
					messages = append(messages, string(message.Value))
				}
			}
		}
	}

	assert.Equal(t, []string{"msg1"}, messages)
}

func TestFetchv16_2m(t *testing.T) {
	hexdump := "0000000b00000000000000064771770282e9d296c41249f0bcc9b03cdcc4888a0200000000000000000000000000020000000000000002000000000000000000ffffffff910100000000000000000000003c0000000002efe33e7c00000000000000000191937e258d00000191937e258d0000000000000001000000000000000000011400000001086d7367310000000000000000010000003c00000000026501ee010000000000000000019193877fe20000019193877fe20000000000000002000000000000000000011400000001086d73673200000000"

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
	assert.Equal(t, "82e9d296-c412-49f0-bcc9-b03cdcc4888a", response.Responses[0].Topic)
	for _, partition := range response.Responses {
		assert.Equal(t, 1, len(partition.Partitions))
		assert.Equal(t, 0, int(partition.Partitions[0].PartitionIndex))
		assert.Equal(t, 0, int(partition.Partitions[0].ErrorCode))
		assert.Equal(t, 2, int(partition.Partitions[0].LastStableOffset))
		assert.Equal(t, 0, int(partition.Partitions[0].LogStartOffset))
		assert.Equal(t, 2, len(partition.Partitions[0].RecordBatches))
		for _, partition := range partition.Partitions {
			assert.Equal(t, 2, len(partition.RecordBatches))
			for _, recordBatch := range partition.RecordBatches {
				for _, message := range recordBatch.Records {
					messages = append(messages, string(message.Value))
				}
			}
		}
	}

	assert.Equal(t, []string{"msg1", "msg2"}, messages)
}

func TestFetchv16_3m(t *testing.T) {
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
		assert.Equal(t, 3, len(partition.Partitions[0].RecordBatches))
		for _, partition := range partition.Partitions {
			assert.Equal(t, 3, len(partition.RecordBatches))
			for _, recordBatch := range partition.RecordBatches {
				for _, message := range recordBatch.Records {
					messages = append(messages, string(message.Value))
				}
			}
		}
	}

	assert.Equal(t, []string{"msg1", "msg2", "msg3"}, messages)
}

func TestAPIVersionv3(t *testing.T) {
	hexdump := "c61574e10000010012000300"

	b, err := hex.DecodeString(hexdump)
	if err != nil {
		panic(err)
	}

	decoder := decoder.RealDecoder{}
	decoder.Init(b)

	responseHeader := kafkaapi.ResponseHeader{}
	if err := responseHeader.DecodeV0(&decoder); err != nil {
		fmt.Println(decoder.FormatDetailedError(err.Error()))
		panic("I QUIT")
	}

	apiVersionsResponse := kafkaapi.ApiVersionsResponse{Version: 3}
	if err := apiVersionsResponse.Decode(&decoder, 3); err != nil {
		fmt.Println(decoder.FormatDetailedError(err.Error()))
		panic("I QUIT")
	}

	assert.NoError(t, err)
}
