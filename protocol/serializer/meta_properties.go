package serializer

import (
	"fmt"
	"os"
	"time"

	"github.com/codecrafters-io/tester-utils/logger"
)

// writeKraftServerProperties writes the embedded kraft.server.properties content to /tmp/kraft-combined-logs/kraft.server.properties
func writeKraftServerProperties(path string, logger *logger.Logger) {
	kraftServerProperties := `process.roles=broker,controller
node.id=1
controller.quorum.voters=1@localhost:9093
listeners=PLAINTEXT://:9092,CONTROLLER://:9093
controller.listener.names=CONTROLLER
listener.security.protocol.map=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,SSL:SSL,SASL_PLAINTEXT:SASL_PLAINTEXT,SASL_SSL:SASL_SSL
log.dirs=/tmp/kraft-combined-logs`

	err := os.WriteFile(path, []byte(kraftServerProperties), 0644)
	if err != nil {
		// ToDo error handling
		// All generate file methods need to handle errors properly
		fmt.Printf("Failed to write to file %s: %v", path, err)
	}

	logger.Debugf("Wrote file to: %s\n", path)
}

func writeMetaProperties(path, clusterID, directoryID string, nodeID, version int, logger *logger.Logger) error {
	content := fmt.Sprintf("#\n#%s\ncluster.id=%s\ndirectory.id=%s\nnode.id=%d\nversion=%d\n",
		time.Now().Format("Mon Jan 02 15:04:05 MST 2006"), clusterID, directoryID, nodeID, version)

	err := os.WriteFile(path, []byte(content), 0644)
	if err != nil {
		return fmt.Errorf("error writing meta properties file: %w", err)
	}
	logger.Debugf("Wrote file to: %s\n", path)
	return nil
}
