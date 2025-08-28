package kafka_executable

import (
	"fmt"
	"path"
	"strings"

	"github.com/codecrafters-io/kafka-tester/protocol/common"
	"github.com/codecrafters-io/tester-utils/executable"
	"github.com/codecrafters-io/tester-utils/logger"
	"github.com/codecrafters-io/tester-utils/test_case_harness"
)

type KafkaExecutable struct {
	executable *executable.Executable
	logger     *logger.Logger
	args       []string
}

func NewKafkaExecutable(stageHarness *test_case_harness.TestCaseHarness) *KafkaExecutable {
	b := &KafkaExecutable{
		executable: stageHarness.NewExecutable(),
		logger:     stageHarness.Logger,
	}
	b.executable.TimeoutInMilliseconds = 3600000
	stageHarness.RegisterTeardownFunc(func() { b.Kill() })

	return b
}

func (b *KafkaExecutable) Run(args ...string) error {
	b.args = args
	b.args = append(b.args, common.SERVER_PROPERTIES_FILE_PATH)
	if len(b.args) == 0 {
		b.logger.Infof("$ ./%s", path.Base(b.executable.Path))
	} else {
		var log string
		log += fmt.Sprintf("$ ./%s", path.Base(b.executable.Path))
		for _, arg := range b.args {
			if strings.Contains(arg, " ") {
				log += " \"" + arg + "\""
			} else {
				log += " " + arg
			}
		}
		b.logger.Infof(log)
	}

	if err := b.executable.Start(b.args...); err != nil {
		return err
	}

	return nil
}

func (b *KafkaExecutable) HasExited() bool {
	return b.executable.HasExited()
}

func (b *KafkaExecutable) Kill() error {
	b.logger.Debugf("Terminating program")
	if err := b.executable.Kill(); err != nil {
		b.logger.Debugf("Error terminating program: '%v'", err)
		return err
	}

	b.logger.Debugf("Program terminated successfully")
	return nil // When does this happen?
}
