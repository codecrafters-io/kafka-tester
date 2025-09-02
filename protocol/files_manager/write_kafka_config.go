package files_manager

import (
	"fmt"
	"os"
	"path"

	"github.com/codecrafters-io/kafka-tester/protocol/common"
	"github.com/codecrafters-io/tester-utils/logger"
)

func writeKraftServerProperties(logger *logger.Logger) error {
	kraftServerPropertiesPath := common.SERVER_PROPERTIES_FILE_PATH

	kraftServerProperties := `process.roles=broker,controller
node.id=1
controller.quorum.voters=1@localhost:9093
listeners=PLAINTEXT://:9092,CONTROLLER://:9093
controller.listener.names=CONTROLLER
listener.security.protocol.map=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,SSL:SSL,SASL_PLAINTEXT:SASL_PLAINTEXT,SASL_SSL:SASL_SSL
log.dirs=/tmp/kraft-combined-logs`

	err := os.WriteFile(kraftServerPropertiesPath, []byte(kraftServerProperties), 0644)

	if err != nil {
		return fmt.Errorf("error writing file to %s: %w", kraftServerPropertiesPath, err)
	}

	logger.Debugf("Wrote file to: %s", kraftServerPropertiesPath)
	return nil
}

func writeKafkaCleanShutdown(logger *logger.Logger) error {
	kafkaCleanShutdownPath := path.Join(common.LOG_DIR, ".kafka_cleanshutdown")
	kafkaCleanShutdown := `{"version":0,"brokerEpoch":10}`

	err := os.WriteFile(kafkaCleanShutdownPath, []byte(kafkaCleanShutdown), 0644)

	if err != nil {
		return fmt.Errorf("error writing file to %s: %w", kafkaCleanShutdownPath, err)
	}

	logger.Debugf("Wrote file to: %s", kafkaCleanShutdownPath)
	return nil
}
