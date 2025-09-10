package kafka_files_handler

import (
	"fmt"
	"os"
	"path"
	"time"

	"github.com/codecrafters-io/kafka-tester/protocol/common"
	"github.com/codecrafters-io/tester-utils/logger"
)

type FilesHandler struct {
	logDirectoryGenerationConfig *LogDirectoryGenerationConfig
	logDirectoryGenerationData   *LogDirectoryGenerationData
}

func NewFilesHandler() *FilesHandler {
	return &FilesHandler{}
}

func (f *FilesHandler) AddLogDirectoryGenerationConfig(logDirectoryGenerationConfig LogDirectoryGenerationConfig) *FilesHandler {
	f.logDirectoryGenerationConfig = &logDirectoryGenerationConfig
	return f
}

func (f *FilesHandler) GenerateServerConfiguration(logger *logger.Logger) (err error) {
	logger.UpdateLastSecondaryPrefix("ServerConfig")
	defer logger.ResetSecondaryPrefixes()

	logger.Debugf("Generating server configuration files")

	// Write /tmp/server.properties
	err = f.writeKraftServerProperties()
	if err != nil {
		return fmt.Errorf("failed to write server properties: %w", err)
	}

	// TODO: Replace DIRECTORY_UUID with random value if it doesn't affect server's behavior

	directoryID, err := uuidToBase64(common.DIRECTORY_UUID)
	if err != nil {
		return fmt.Errorf("failed to convert directory UUID to base64: %w", err)
	}

	// (re)create /tmp/kraft-combined-logs
	if err := f.initializeLogDirecotry(common.LOG_DIR); err != nil {
		return err
	}

	// TODO: Replace CLUSTER_ID, NODE_ID, VERSION with random values

	// Write log_directory/meta.properties
	err = f.writeMetaProperties(common.CLUSTER_ID, directoryID, common.NODE_ID, common.VERSION)
	if err != nil {
		return fmt.Errorf("failed to write meta properties: %w", err)
	}

	// Write clean shutdown
	err = f.writeKafkaCleanShutdown()
	if err != nil {
		return fmt.Errorf("failed to write clean shutdown: %w", err)
	}

	logger.Debugf("Successfully generated server configuration files")
	return nil
}

func (f *FilesHandler) GenerateServerConfigAndLogDirectory(logger *logger.Logger) error {
	if err := f.GenerateServerConfiguration(logger); err != nil {
		return err
	}

	if f.logDirectoryGenerationConfig == nil {
		panic("Codecrafters Internal Error - GenerateServerConfigAndLogDirectory called without LogDirectoryGenerationConfig")
	}

	logDirectoryGenerationData, err := f.logDirectoryGenerationConfig.Generate()

	if err != nil {
		return err
	}

	f.logDirectoryGenerationData = logDirectoryGenerationData
	return nil
}

func (f *FilesHandler) writeKraftServerProperties() error {
	filePath := common.SERVER_PROPERTIES_FILE_PATH

	kraftServerProperties := `process.roles=broker,controller
node.id=1
controller.quorum.voters=1@localhost:9093
listeners=PLAINTEXT://:9092,CONTROLLER://:9093
controller.listener.names=CONTROLLER
listener.security.protocol.map=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,SSL:SSL,SASL_PLAINTEXT:SASL_PLAINTEXT,SASL_SSL:SASL_SSL
log.dirs=/tmp/kraft-combined-logs`

	err := os.WriteFile(filePath, []byte(kraftServerProperties), 0644)

	if err != nil {
		return fmt.Errorf("error writing file to %s: %w", filePath, err)
	}

	return nil
}

func (f *FilesHandler) writeMetaProperties(clusterID, directoryID string, nodeID, version int) error {
	content := fmt.Sprintf("#\n#%s\ncluster.id=%s\ndirectory.id=%s\nnode.id=%d\nversion=%d\n",
		time.Now().Format("Mon Jan 02 15:04:05 MST 2006"), clusterID, directoryID, nodeID, version)

	filePath := path.Join(common.LOG_DIR, "meta.properties")

	err := os.WriteFile(filePath, []byte(content), 0644)

	if err != nil {
		return fmt.Errorf("error writing meta properties file: %w", err)
	}

	return nil
}

func (f *FilesHandler) writeKafkaCleanShutdown() error {
	kafkaCleanShutdown := `{"version":0,"brokerEpoch":10}`
	filePath := path.Join(common.LOG_DIR, ".kafka_cleanshutdown")

	err := os.WriteFile(filePath, []byte(kafkaCleanShutdown), 0644)

	if err != nil {
		return fmt.Errorf("error writing file to %s: %w", filePath, err)
	}

	return nil
}

func (f *FilesHandler) initializeLogDirecotry(logDirectoryPath string) error {
	err := os.RemoveAll(logDirectoryPath)

	if err != nil {
		return fmt.Errorf("could not remove log directory at %s: %w", logDirectoryPath, err)
	}

	err = os.MkdirAll(logDirectoryPath, 0755)

	if err != nil {
		return fmt.Errorf("could not create log directory at %s: %w", logDirectoryPath, err)
	}

	return nil
}
