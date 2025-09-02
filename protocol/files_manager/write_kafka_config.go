package files_manager

import (
	"fmt"
	"os"
	"path"

	"github.com/codecrafters-io/kafka-tester/protocol/common"
)

func (f *FilesManager) writeKraftServerProperties() error {
	logger := f.logger
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

	logger.Debugf("%sWrote file to: %s", f.getIndentedPrefix(), kraftServerPropertiesPath)
	return nil
}

func (f *FilesManager) writeKafkaCleanShutdown() error {
	logger := f.logger
	kafkaCleanShutdownPath := path.Join(common.LOG_DIR, ".kafka_cleanshutdown")
	kafkaCleanShutdown := `{"version":0,"brokerEpoch":10}`
	err := os.WriteFile(kafkaCleanShutdownPath, []byte(kafkaCleanShutdown), 0644)

	if err != nil {
		return fmt.Errorf("error writing file to %s: %w", kafkaCleanShutdownPath, err)
	}

	logger.Debugf("%sWrote file to: %s", f.getIndentedPrefix(), kafkaCleanShutdownPath)
	return nil
}
