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
		{
			Slug:     "nh4",
			TestFunc: testSequentialRequests,
		},
		{
			Slug:     "sk0",
			TestFunc: testConcurrentRequests,
		},
		{
			Slug:     "gs0",
			TestFunc: testAPIVersionwFetchKey,
		},
		{
			Slug:     "dh6",
			TestFunc: testFetchWithNoTopics,
		},
		{
			Slug:     "hn6",
			TestFunc: testFetchWithUnkownTopicID,
		},
		{
			Slug:     "cm4",
			TestFunc: testFetch,
		},
		{
			Slug:     "eg2",
			TestFunc: testSingleFetchFromDisk,
		},
		{
			Slug:     "fd8",
			TestFunc: testMultiFetchFromDisk,
		},
		{
			Slug:     "yk1",
			TestFunc: testAPIVersionwDescribeTopicPartitions,
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
			Slug:     "lk1",
			TestFunc: testDTPartitionWithTopicAndMultiplePartitions1,
		},
		{
			Slug:     "ku4",
			TestFunc: testDTPartitionWithTopicAndMultiplePartitions2,
		},
		{
			Slug:     "wq2",
			TestFunc: testDTPartitionWithTopics,
		},
	},
}
