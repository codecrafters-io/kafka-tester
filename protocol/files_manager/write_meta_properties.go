package files_manager

import (
	"fmt"
	"os"
	"path"
	"time"

	"github.com/codecrafters-io/kafka-tester/protocol/common"
	"github.com/codecrafters-io/tester-utils/logger"
)

func writeMetaProperties(logger *logger.Logger) error {
	metaPropertiesPath := path.Join(common.LOG_DIR, "meta.properties")
	clusterID := common.CLUSTER_ID
	nodeID := common.NODE_ID
	version := common.VERSION
	directoryID, err := uuidToBase64(common.DIRECTORY_UUID)

	if err != nil {
		return fmt.Errorf("failed to convert directory uuid to base64: %s", err)
	}

	content := fmt.Sprintf("#\n#%s\ncluster.id=%s\ndirectory.id=%s\nnode.id=%d\nversion=%d\n",
		time.Now().Format("Mon Jan 02 15:04:05 MST 2006"), clusterID, directoryID, nodeID, version)

	err = os.WriteFile(metaPropertiesPath, []byte(content), 0644)

	if err != nil {
		return fmt.Errorf("error writing meta properties file: %w", err)
	}

	logger.Debugf("Wrote meta properties to: %s", metaPropertiesPath)
	return nil
}
