package internal

import (
	"encoding/hex"
	"testing"

	"github.com/codecrafters-io/kafka-tester/internal/field_decoder"
	"github.com/codecrafters-io/kafka-tester/internal/response_decoders"
	"github.com/stretchr/testify/assert"
)

func TestFetchv16With0Messages(t *testing.T) {
	hexdump := "0000000b000000000000004fd5de7102c2a21ee23db74b6cbcc32a051cc51fc90200000000000000000000000000000000000000000000000000000000000000ffffffff01000000"

	b, err := hex.DecodeString(hexdump)
	if err != nil {
		panic(err)
	}

	decoder := field_decoder.NewFieldDecoder(b)
	response, err := response_decoders.DecodeFetchResponse(decoder)

	assert.Nil(t, err, "Could not decode Fetch Response")

	assert.NoError(t, err)

	assert.Equal(t, 11, int(response.Header.CorrelationId.Value))

	assert.Equal(t, 0, int(response.Body.ErrorCode.Value))
	assert.Equal(t, 0, int(response.Body.ThrottleTimeMs.Value))
	assert.Equal(t, 1339416177, int(response.Body.SessionId.Value))
	assert.Equal(t, 1, len(response.Body.TopicResponses))
	assert.Equal(t, "c2a21ee2-3db7-4b6c-bcc3-2a051cc51fc9", response.Body.TopicResponses[0].UUID.Value)
	assert.Equal(t, 0, len(response.Body.TopicResponses[0].PartitionResponses[0].RecordBatches))

	for _, partition := range response.Body.TopicResponses {
		assert.Equal(t, 1, len(partition.PartitionResponses))
		assert.Equal(t, 0, int(partition.PartitionResponses[0].Id.Value))
		assert.Equal(t, 0, int(partition.PartitionResponses[0].ErrorCode.Value))
		assert.Equal(t, 0, int(partition.PartitionResponses[0].LastStableOffset.Value))
		assert.Equal(t, 0, int(partition.PartitionResponses[0].LogStartOffset.Value))
		assert.Equal(t, 0, len(partition.PartitionResponses[0].RecordBatches))
	}
}

func TestFetchv16With1Message(t *testing.T) {
	hexdump := "0000000b000000000000007a983da40282e9d296c41249f0bcc9b03cdcc4888a0200000000000000000000000000010000000000000001000000000000000000ffffffff4900000000000000000000003c0000000002efe33e7c00000000000000000191937e258d00000191937e258d0000000000000001000000000000000000011400000001086d73673100000000"

	b, err := hex.DecodeString(hexdump)
	if err != nil {
		panic(err)
	}

	decoder := field_decoder.NewFieldDecoder(b)
	response, err := response_decoders.DecodeFetchResponse(decoder)

	assert.Nil(t, err, "Could not decode Fetch Response")

	assert.NoError(t, err)

	assert.Equal(t, 11, int(response.Header.CorrelationId.Value))

	assert.Equal(t, 0, int(response.Body.ErrorCode.Value))
	assert.Equal(t, 0, int(response.Body.ThrottleTimeMs.Value))
	assert.Equal(t, 2056797604, int(response.Body.SessionId.Value))
	assert.Equal(t, 1, len(response.Body.TopicResponses))
	assert.Equal(t, "82e9d296-c412-49f0-bcc9-b03cdcc4888a", response.Body.TopicResponses[0].UUID.Value)

	messages := []string{}

	for _, partition := range response.Body.TopicResponses {
		for _, partition := range partition.PartitionResponses {
			assert.Equal(t, 0, int(partition.ErrorCode.Value))
			assert.Equal(t, 1, int(partition.LastStableOffset.Value))
			assert.Equal(t, 0, int(partition.LogStartOffset.Value))
			assert.Equal(t, 1, len(partition.RecordBatches))
			assert.Equal(t, 0, int(partition.Id.Value))
			for _, recordBatch := range partition.RecordBatches {
				for _, message := range recordBatch.Records {
					messages = append(messages, string(message.Value))
				}
			}
		}
	}

	assert.Equal(t, []string{"msg1"}, messages)
}

func TestFetchv16With2Messages(t *testing.T) {
	hexdump := "0000000b00000000000000064771770282e9d296c41249f0bcc9b03cdcc4888a0200000000000000000000000000020000000000000002000000000000000000ffffffff910100000000000000000000003c0000000002efe33e7c00000000000000000191937e258d00000191937e258d0000000000000001000000000000000000011400000001086d7367310000000000000000010000003c00000000026501ee010000000000000000019193877fe20000019193877fe20000000000000002000000000000000000011400000001086d73673200000000"

	b, err := hex.DecodeString(hexdump)
	if err != nil {
		panic(err)
	}

	decoder := field_decoder.NewFieldDecoder(b)
	response, err := response_decoders.DecodeFetchResponse(decoder)

	assert.Nil(t, err, "Could not decode Fetch Response")
	assert.NoError(t, err)

	messages := []string{}

	assert.Equal(t, 11, int(response.Header.CorrelationId.Value))
	assert.Equal(t, 1, len(response.Body.TopicResponses))
	assert.Equal(t, "82e9d296-c412-49f0-bcc9-b03cdcc4888a", response.Body.TopicResponses[0].UUID.Value)

	for _, topicResponse := range response.Body.TopicResponses {
		assert.Equal(t, 1, len(topicResponse.PartitionResponses))
		assert.Equal(t, 0, int(topicResponse.PartitionResponses[0].Id.Value))
		assert.Equal(t, 0, int(topicResponse.PartitionResponses[0].ErrorCode.Value))
		assert.Equal(t, 2, int(topicResponse.PartitionResponses[0].LastStableOffset.Value))
		assert.Equal(t, 0, int(topicResponse.PartitionResponses[0].LogStartOffset.Value))
		assert.Equal(t, 2, len(topicResponse.PartitionResponses[0].RecordBatches))

		for _, partitionResponse := range topicResponse.PartitionResponses {
			assert.Equal(t, 2, len(partitionResponse.RecordBatches))
			for _, recordBatch := range partitionResponse.RecordBatches {
				for _, record := range recordBatch.Records {
					messages = append(messages, string(record.Value))
				}
			}
		}
	}

	assert.Equal(t, []string{"msg1", "msg2"}, messages)
}

func TestFetchv16With3Messages(t *testing.T) {
	hexdump := "0000000b0000000000000019ffd72602c2a21ee23db74b6cbcc32a051cc51fc90200000000000000000000000000030000000000000003000000000000000000ffffffffd90100000000000000000000003c00000000026eca8a5d0000000000000000019192c16b5d0000019192c16b5d0000000000000000000000000000000000011400000001086d7367310000000000000000010000003c0000000002ad286abc0000000000000000019192c170b70000019192c170b70000000000000000000000000001000000011400000001086d7367320000000000000000020000003c0000000002d0470b040000000000000000019192c176990000019192c176990000000000000000000000000002000000011400000001086d73673300000000"

	b, err := hex.DecodeString(hexdump)
	if err != nil {
		panic(err)
	}

	decoder := field_decoder.NewFieldDecoder(b)
	response, err := response_decoders.DecodeFetchResponse(decoder)

	assert.Nil(t, err, "Could not decode Fetch Response")
	assert.NoError(t, err)

	messages := []string{}

	assert.Equal(t, 11, int(response.Header.CorrelationId.Value))
	assert.Equal(t, 1, len(response.Body.TopicResponses))
	assert.Equal(t, "c2a21ee2-3db7-4b6c-bcc3-2a051cc51fc9", response.Body.TopicResponses[0].UUID.Value)

	for _, topicResponse := range response.Body.TopicResponses {
		assert.Equal(t, 1, len(topicResponse.PartitionResponses))
		assert.Equal(t, 0, int(topicResponse.PartitionResponses[0].Id.Value))
		assert.Equal(t, 0, int(topicResponse.PartitionResponses[0].ErrorCode.Value))
		assert.Equal(t, 3, int(topicResponse.PartitionResponses[0].LastStableOffset.Value))
		assert.Equal(t, 0, int(topicResponse.PartitionResponses[0].LogStartOffset.Value))
		assert.Equal(t, 3, len(topicResponse.PartitionResponses[0].RecordBatches))

		for _, partitionResponse := range topicResponse.PartitionResponses {
			assert.Equal(t, 3, len(partitionResponse.RecordBatches))
			for _, recordBatch := range partitionResponse.RecordBatches {
				for _, record := range recordBatch.Records {
					messages = append(messages, string(record.Value))
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

	decoder := field_decoder.NewFieldDecoder(b)
	response, err := response_decoders.DecodeApiVersionsResponse(decoder)

	assert.Nil(t, err, "Could not decode API Versions Response")
	assert.NoError(t, err)

	// Basic validation that response was decoded successfully
	assert.NotNil(t, response.Header)
	assert.NotNil(t, response.Body)
}
