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
			Slug:     "st1",
			TestFunc: testBindToPort,
			Timeout:  15 * time.Second,
		},
		{
			Slug:     "st2",
			TestFunc: testHardcodedCorrelationId,
		},
		{
			Slug:     "st3",
			TestFunc: testCorrelationId,
		},
		{
			Slug:     "st4",
			TestFunc: testAPIVersionErrorCase,
		},
		{
			Slug:     "st5",
			TestFunc: testAPIVersionMessageLength,
		},
		{
			Slug:     "st6",
			TestFunc: testAPIVersion,
		},
		{
			Slug:     "st7",
			TestFunc: testAPIVersionwFetchKey,
		},
		{
			Slug:     "st8",
			TestFunc: testFetchError,
		},
		{
			Slug:     "st8_2",
			TestFunc: testFetchError2,
		},
		{
			Slug:     "st9",
			TestFunc: testEmptyFetch,
		},
		{
			Slug:     "st10",
			TestFunc: testFetch,
		},
	},
}
