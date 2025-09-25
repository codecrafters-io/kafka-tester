package internal

import (
	"time"

	"github.com/codecrafters-io/tester-utils/tester_definition"
)

var testerDefinition = tester_definition.TesterDefinition{
	AntiCheatTestCases:       []tester_definition.TestCase{},
	ExecutableFileName:       "your_program.sh",
	LegacyExecutableFileName: "your_program.sh",
	TestCases: []tester_definition.TestCase{
		// Base stages
		{
			Slug:     "vi6",
			TestFunc: testBindToPort,
			Timeout:  15 * time.Second,
		},
		{
			Slug:     "nv3",
			TestFunc: testHardcodedCorrelationId,
		},
		{
			Slug:     "wa6",
			TestFunc: testCorrelationId,
		},
		{
			Slug:     "nc5",
			TestFunc: testAPIVersionErrorCase,
		},
		{
			Slug:     "pv1",
			TestFunc: testAPIVersion,
		},
		// Concurrent clients
		{
			Slug:     "nh4",
			TestFunc: testSequentialRequests,
		},
		{
			Slug:     "sk0",
			TestFunc: testConcurrentRequests,
		},
		// DescribeTopicPartitions
		{
			Slug:     "yk1",
			TestFunc: testAPIVersionWithDescribeTopicPartitions,
		},
		{
			Slug:     "vt6",
			TestFunc: testDTPartitionWithUnknownTopic,
		},
		{
			Slug:     "ea7",
			TestFunc: testDTPartitionWithTopicAndSinglePartition,
		},
		{
			Slug:     "ku4",
			TestFunc: testDTPartitionWithTopicAndMultiplePartitions,
		},
		{
			Slug:     "wq2",
			TestFunc: testDTPartitionWithTopics,
		},
		// Fetch
		{
			Slug:     "gs0",
			TestFunc: testAPIVersionWithFetchKey,
		},
		{
			Slug:     "dh6",
			TestFunc: testFetchWithNoTopics,
		},
		{
			Slug:     "hn6",
			TestFunc: testFetchWithUnknownTopicId,
		},
		{
			Slug:     "cm4",
			TestFunc: testFetchNoMessages,
		},
		{
			Slug:     "eg2",
			TestFunc: testFetchWithSingleMessage,
		},
		{
			Slug:     "fd8",
			TestFunc: testFetchMultipleMessages,
		},
		// Produce
		{
			Slug:     "xz1",
			TestFunc: testAPIVersionWithProduceKey,
		},
		{
			Slug:     "zf2",
			TestFunc: testProduceWithInvalidRequest,
		},
		{
			Slug:     "gg1",
			TestFunc: testProduceResponse,
		},
		{
			Slug:     "ls8",
			TestFunc: testProduceSingleRecord,
		},
		{
			Slug:     "yd8",
			TestFunc: testProduceMultipleRecords,
		},
		{
			Slug:     "ct4",
			TestFunc: testProduceForMultiplePartitions,
		},
		{
			Slug:     "ov0",
			TestFunc: testProduceForMultipleTopics,
		},
	},
}
