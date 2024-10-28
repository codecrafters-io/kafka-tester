package internal

import (
	"github.com/codecrafters-io/kafka-tester/internal/kafka_executable"
	"github.com/codecrafters-io/kafka-tester/internal/test_cases"
	"github.com/codecrafters-io/kafka-tester/protocol/serializer"
	"github.com/codecrafters-io/tester-utils/logger"
	"github.com/codecrafters-io/tester-utils/test_case_harness"
)

func testBindToPort(stageHarness *test_case_harness.TestCaseHarness) error {
	b := kafka_executable.NewKafkaExecutable(stageHarness)
	if err := b.Run(); err != nil {
		return err
	}

	quietLogger := logger.GetQuietLogger("")
	logger := stageHarness.Logger
	err := serializer.GenerateLogDirs(quietLogger, true)
	if err != nil {
		return err
	}

	bindTestCase := test_cases.BindTestCase{
		Port:    9092,
		Retries: 15,
	}

	return bindTestCase.Run(b, logger)
}
