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
			Timeout:  30 * time.Second,
		},
		{
			Slug:     "nv3",
			TestFunc: testHardcodedCorrelationId,
			Timeout:  30 * time.Second,
		},
		{
			Slug:     "wa6",
			TestFunc: testCorrelationId,
			Timeout:  30 * time.Second,
		},
		{
			Slug:     "nc5",
			TestFunc: testAPIVersionErrorCase,
			Timeout:  30 * time.Second,
		},
		{
			Slug:     "pv1",
			TestFunc: testAPIVersion,
			Timeout:  30 * time.Second,
		},
		{
			Slug:     "nh4",
			TestFunc: testSequentialRequests,
			Timeout:  30 * time.Second,
		},
		{
			Slug:     "sk0",
			TestFunc: testConcurrentRequests,
			Timeout:  30 * time.Second,
		},
		{
			Slug:     "gs0",
			TestFunc: testAPIVersionwFetchKey,
			Timeout:  30 * time.Second,
		},
		{
			Slug:     "dh6",
			TestFunc: testFetchWithNoTopics,
			Timeout:  30 * time.Second,
		},
		{
			Slug:     "hn6",
			TestFunc: testFetchWithUnkownTopicID,
			Timeout:  30 * time.Second,
		},
		{
			Slug:     "cm4",
			TestFunc: testFetch,
			Timeout:  30 * time.Second,
		},
		{
			Slug:     "eg2",
			TestFunc: testSingleFetchFromDisk,
			Timeout:  30 * time.Second,
		},
		{
			Slug:     "fd8",
			TestFunc: testMultiFetchFromDisk,
			Timeout:  30 * time.Second,
		},
		{
			Slug:     "yk1",
			TestFunc: testAPIVersionwDescribeTopicPartitions,
			Timeout:  30 * time.Second,
		},
		{
			Slug:     "vt6",
			TestFunc: testDTPartitionWithUnknownTopic,
			Timeout:  30 * time.Second,
		},
		{
			Slug:     "ea7",
			TestFunc: testDTPartitionWithTopicAndSinglePartition,
			Timeout:  30 * time.Second,
		},
		{
			Slug:     "ku4",
			TestFunc: testDTPartitionWithTopicAndMultiplePartitions2,
			Timeout:  30 * time.Second,
		},
		{
			Slug:     "wq2",
			TestFunc: testDTPartitionWithTopics,
			Timeout:  30 * time.Second,
		},
	},
}
