package response_assertions

import (
	"bytes"
	"fmt"
	"os"
	"regexp"

	"github.com/codecrafters-io/kafka-tester/internal/field"
	int32_assertions "github.com/codecrafters-io/kafka-tester/internal/value_assertions/int32"
	"github.com/codecrafters-io/kafka-tester/protocol/encoder"
	"github.com/codecrafters-io/kafka-tester/protocol/kafkaapi"
	"github.com/codecrafters-io/kafka-tester/protocol/utils"
	"github.com/codecrafters-io/tester-utils/logger"
)

type ProduceResponsePartitionData struct {
	Id              int32
	ErrorCode       int16
	BaseOffset      int64
	LogAppendTimeMs int64
	LogStartOffset  int64
}

// GetTopicExpectationData returns what to expect from a response given produce request body
func GetTopicExpectationData(topics []kafkaapi.ProduceRequestTopicData) []ProduceResponseTopicData {
	expectedTopicsData := []ProduceResponseTopicData{}

	for _, topic := range topics {

		// Prepare partition data for each topic
		expectedPartitionsData := []ProduceResponsePartitionData{}

		for _, partition := range topic.Partitions {
			expectedPartitionsData = append(
				expectedPartitionsData,
				ProduceResponsePartitionData{
					Id:              partition.Id.Value,
					ErrorCode:       0,
					BaseOffset:      0,
					LogAppendTimeMs: -1,
					LogStartOffset:  0,
				},
			)
		}

		expectedTopicsData = append(expectedTopicsData, ProduceResponseTopicData{
			Name:       topic.Name.Value,
			Partitions: expectedPartitionsData,
		})
	}

	return expectedTopicsData
}

type ProduceResponseTopicData struct {
	Name       string
	Partitions []ProduceResponsePartitionData
}

type ProduceResponseAssertion struct {
	expectedCorrelationId   int32
	expectedThrottleTimeMs  int32
	expectedTopicProperties []ProduceResponseTopicData
}

func NewProduceResponseAssertion() *ProduceResponseAssertion {
	return &ProduceResponseAssertion{}
}

func (a *ProduceResponseAssertion) ExpectCorrelationId(correlationId int32) *ProduceResponseAssertion {
	a.expectedCorrelationId = correlationId
	return a
}

func (a *ProduceResponseAssertion) ExpectThrottleTimeMs(throttleTimeMs int32) *ProduceResponseAssertion {
	a.expectedThrottleTimeMs = throttleTimeMs
	return a
}

func (a *ProduceResponseAssertion) ExpectTopicProperties(topicProperties []ProduceResponseTopicData) *ProduceResponseAssertion {
	a.expectedTopicProperties = topicProperties
	return a
}

func (a *ProduceResponseAssertion) AssertSingleField(field field.Field) error {
	fieldPath := field.Path.String()

	// Header fields
	if fieldPath == "ProduceResponse.Header.CorrelationID" {
		return int32_assertions.IsEqualTo(a.expectedCorrelationId, field.Value)
	}

	// Body level fields
	if fieldPath == "ProduceResponse.Body.ThrottleTimeMS" {
		return int32_assertions.IsEqualTo(a.expectedThrottleTimeMs, field.Value)
	}

	// Everything related to topics will be handled by AssertAcrossFields
	if regexp.MustCompile(`\.Topics\..*$`).MatchString(fieldPath) {
		return nil
	}

	// This ensures that we're handling ALL possible fields
	panic("Codecrafters Internal Error: Unhandled field path: " + fieldPath)
}

func (a *ProduceResponseAssertion) AssertAcrossFields(response kafkaapi.ProduceResponse, logger *logger.Logger) error {
	logger.Successf("✓ CorrelationID: %d", a.expectedCorrelationId)
	logger.Successf("✓ ThrottleTimeMS: %d", a.expectedThrottleTimeMs)

	expectedTopicCount := len(a.expectedTopicProperties)
	actualTopicCount := len(response.Body.Topics)
	if actualTopicCount != expectedTopicCount {
		return fmt.Errorf("Expected topics.length to be %d, got %d", expectedTopicCount, actualTopicCount)
	}
	logger.Successf("✓ Topics Length: %d", actualTopicCount)

	for _, expectedTopic := range a.expectedTopicProperties {
		var actualTopic *kafkaapi.ProduceResponseTopicData
		var actualTopicIndex int

		// Search for the expected topic in the Produce API response
		// Unlike DescribeTopicPartitions, in Produce, the order of topics cannot be guaranted
		// This is because Kafka internally uses Map<TopicIdPartition, PartitionResponse> to store these data
		// and order cannot be guaranteed using map
		// Ref. https://github.com/apache/kafka/blob/71efb892900387cf4cd8c65cd949609c712c19cc/clients/src/main/java/org/apache/kafka/common/requests/ProduceResponse.java#L106
		topicFound := false
		for topicIndex, topic := range response.Body.Topics {
			if topic.Name.Value == expectedTopic.Name {
				actualTopic = &topic
				actualTopicIndex = topicIndex
				topicFound = true
				break
			}
		}

		// Assert that all expected topics are found in the response
		if !topicFound {
			return fmt.Errorf("Expected topic %s not found in response", expectedTopic.Name)
		}

		logger.Successf("✓ Topic[%d].Name: %s", actualTopicIndex, actualTopic.Name.Value)

		// Assert the number of partitions in the topic
		expectedPartitionCount := len(expectedTopic.Partitions)
		actualPartitionCount := len(actualTopic.Partitions)

		if actualPartitionCount != expectedPartitionCount {
			return fmt.Errorf("Expected Topic[%d].Partitions.Length to be %d, got %d", actualTopicIndex, expectedPartitionCount, actualPartitionCount)
		}

		logger.Successf("✓ Topic[%d].Partitions.Length: %d", actualTopicIndex, actualPartitionCount)

		// The partitions can appear in any order so, search for the partition whose ID matches the expected partition
		for _, expectedPartition := range expectedTopic.Partitions {
			var actualPartition *kafkaapi.ProduceResponsePartitionData
			var actualPartitionIndex int

			partitionFound := false
			for partitionIndex, partition := range actualTopic.Partitions {
				if partition.Id.Value == expectedPartition.Id {
					actualPartition = &partition
					actualPartitionIndex = partitionIndex
					partitionFound = true
					break
				}
			}

			// Ensure all expected partitions are present in the response
			if !partitionFound {
				return fmt.Errorf("Expected Topic[%d].Partition with Id %d not found in topic %s", actualTopicIndex, expectedPartition.Id, expectedTopic.Name)
			}

			logger.Successf("✓ Topic[%d].Partition[%d].Id: %d", actualTopicIndex, actualPartitionIndex, actualPartition.Id.Value)

			// Assert Error Code
			if actualPartition.ErrorCode.Value != expectedPartition.ErrorCode {
				return fmt.Errorf("Expected Topic[%d].Partition[%d].ErrorCode to be %d, got %d", actualTopicIndex, actualPartitionIndex, expectedPartition.ErrorCode, actualPartition.ErrorCode.Value)
			}

			logger.Successf("✓ Topic[%d].Partition[%d].ErrorCode: %d", actualTopicIndex, actualPartitionIndex, actualPartition.ErrorCode.Value)

			// Assert Base Offset
			if actualPartition.BaseOffset.Value != expectedPartition.BaseOffset {
				return fmt.Errorf("Expected Topic[%d].Partition[%d].BaseOffset to be %d, got %d", actualTopicIndex, actualPartitionIndex, expectedPartition.BaseOffset, actualPartition.BaseOffset.Value)
			}

			logger.Successf("✓ Topic[%d].Partition[%d].BaseOffset: %d", actualTopicIndex, actualPartitionIndex, actualPartition.BaseOffset.Value)

			// Assert LogAppendTimeMs
			if actualPartition.LogAppendTimeMs.Value != expectedPartition.LogAppendTimeMs {
				return fmt.Errorf("Expected Topic[%d].Partition[%d].LogAppendTime to be %d, got %d", actualTopicIndex, actualPartitionIndex, expectedPartition.LogAppendTimeMs, actualPartition.LogAppendTimeMs.Value)
			}

			logger.Successf("✓ Topic[%d].Partition[%d].LogAppendTime: %d", actualTopicIndex, actualPartitionIndex, actualPartition.LogAppendTimeMs.Value)

			// Assert LogStartOffset
			if actualPartition.LogStartOffset.Value != expectedPartition.LogStartOffset {
				return fmt.Errorf("Expected Topic[%d].Partition[%d].LogStartOffset to be %d, got %d", actualTopicIndex, actualPartitionIndex, expectedPartition.LogStartOffset, actualPartition.LogStartOffset.Value)
			}

			logger.Successf("✓ Topic[%d].Partition[%d].LogStartOffset: %d", actualTopicIndex, actualPartitionIndex, actualPartition.LogStartOffset.Value)
		}
	}

	return nil
}

func (a *ProduceResponseAssertion) AssertFilesOnDisk(topics []kafkaapi.ProduceRequestTopicData, stageLogger *logger.Logger) error {
	for _, topic := range topics {
		for _, partition := range topic.Partitions {

			// We support one recordbatch per partition in the stages
			if len(partition.RecordBatches) != 1 {
				panic(fmt.Sprintf("Codecrafters Internal Error - Expected exactly one record batch per partition, got %d for topic %s partition %d",
					len(partition.RecordBatches), topic.Name.Value, partition.Id.Value))
			}

			recordBatch := partition.RecordBatches[0]

			// Encode the record batch to get the expected byte representation
			expectedBytes := getRecordBatchBytes(recordBatch)

			// Read the actual log file content
			logFilePath := fmt.Sprintf("/tmp/kraft-combined-logs/%s-%d/00000000000000000000.log",
				topic.Name.Value, partition.Id.Value)

			stageLogger.Infof("Checking file contents of the file %s", logFilePath)

			actualBytes, err := os.ReadFile(logFilePath)
			if err != nil {
				return fmt.Errorf("failed to read log file %s: %w", logFilePath, err)
			}

			// Compare the bytes
			if !bytes.Equal(expectedBytes, actualBytes) {
				// Print hexdump in case of difference
				stageLogger.Infof("Expected bytes for topic %s partition %d:\n%s\n",
					topic.Name.Value, partition.Id.Value, utils.GetFormattedHexdump(expectedBytes))
				stageLogger.Infof("Actual bytes in log file:\n%s\n",
					utils.GetFormattedHexdump(actualBytes))

				return fmt.Errorf("log file content mismatch for topic %s partition %d",
					topic.Name.Value, partition.Id.Value)
			}

			stageLogger.Successf("✓ Contents of the file %s matches the recordbatch format sent in request", logFilePath)

		}
	}

	return nil
}

// getRecordBatchBytes encodes a record batch and returns its byte representation
func getRecordBatchBytes(recordBatch kafkaapi.RecordBatch) []byte {
	encoder := encoder.NewEncoder()
	recordBatch.Encode(encoder)
	return encoder.Bytes()
}
