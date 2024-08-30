package test_cases

import (
	"fmt"
	"net"
	"strconv"
	"time"

	"github.com/codecrafters-io/kafka-tester/internal/kafka_executable"
	"github.com/codecrafters-io/tester-utils/logger"
)

type BindTestCase struct {
	Port    int
	Retries int
}

func (t BindTestCase) Run(executable *kafka_executable.KafkaExecutable, logger *logger.Logger) error {
	logger.Infof("Connecting to port %d...", t.Port)

	retries := 0
	var err error
	address := "localhost:" + strconv.Itoa(t.Port)
	for {
		_, err = net.Dial("tcp", address)
		if err != nil && retries > t.Retries {
			logger.Infof("All retries failed.")
			return err
		}

		if err != nil {
			if executable.HasExited() {
				return fmt.Errorf("Looks like your program has terminated. A kafka server is expected to be a long-running process.")
			}

			// Don't print errors in the first second
			if retries > 2 {
				logger.Infof("Failed to connect to port %d, retrying in 1s", t.Port)
			}

			retries += 1
			time.Sleep(1000 * time.Millisecond)
		} else {
			break
		}
	}
	logger.Debugln("Connection successful")
	return nil
}
