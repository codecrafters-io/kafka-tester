package serializer

import (
	"fmt"
	"os"
	"time"

	"github.com/codecrafters-io/tester-utils/logger"
)

// writeKraftServerProperties writes the hard-coded kraft.server.properties content to path
// which should be /tmp/kraft-combined-logs/kraft.server.properties
func writeKraftServerProperties(path string, logger *logger.Logger) error {
	kraftServerProperties := `process.roles=broker,controller
node.id=1
controller.quorum.voters=1@localhost:9093
listeners=PLAINTEXT://:9092,CONTROLLER://:9093
controller.listener.names=CONTROLLER
listener.security.protocol.map=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,SSL:SSL,SASL_PLAINTEXT:SASL_PLAINTEXT,SASL_SSL:SASL_SSL
log.dirs=/tmp/kraft-combined-logs`

	err := os.WriteFile(path, []byte(kraftServerProperties), 0644)
	if err != nil {
		return fmt.Errorf("error writing file to %s: %w", path, err)
	}

	logger.Debugf("    - Wrote file to: %s", path)
	return nil
}

func writeMetaProperties(path, clusterID, directoryID string, nodeID, version int, logger *logger.Logger) error {
	content := fmt.Sprintf("#\n#%s\ncluster.id=%s\ndirectory.id=%s\nnode.id=%d\nversion=%d\n",
		time.Now().Format("Mon Jan 02 15:04:05 MST 2006"), clusterID, directoryID, nodeID, version)

	err := os.WriteFile(path, []byte(content), 0644)
	if err != nil {
		return fmt.Errorf("error writing meta properties file: %w", err)
	}

	logger.Debugf("    - Wrote file to: %s", path)
	return nil
}

// writeKafkaCleanShutdown writes the hard-coded .kafka_cleanshutdown content to path
func writeKafkaCleanShutdown(path string, logger *logger.Logger) error {
	kafkaCleanShutdown := `{"version":0,"brokerEpoch":10}`

	err := os.WriteFile(path, []byte(kafkaCleanShutdown), 0644)
	if err != nil {
		return fmt.Errorf("error writing file to %s: %w", path, err)
	}

	logger.Debugf("    - Wrote file to: %s", path)
	return nil
}
