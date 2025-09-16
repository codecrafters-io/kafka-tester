package kafka_files_generator

import (
	"fmt"
	"os"
	"path"
	"time"

	"github.com/codecrafters-io/kafka-tester/protocol/kafkaapi"
	"github.com/codecrafters-io/tester-utils/logger"
)

// Structures to hold information about generated topics and partitions

type GeneratedRecordBatchesByPartition struct {
	PartitionId   int
	recordBatches kafkaapi.RecordBatches
}

type GeneratedTopicData struct {
	Name                              string
	UUID                              string
	GeneratedRecordBatchesByPartition []GeneratedRecordBatchesByPartition
}

type GeneratedLogDirectoryData struct {
	GeneratedTopicsData []*GeneratedTopicData
}

// FilesHandler allows creation of multiple topics/partitions at once
type FilesHandler struct {
	logDirectoryGenerationConfig *LogDirectoryGenerationConfig
	generatedLogDirectoryData    *GeneratedLogDirectoryData
	logger                       *logger.Logger
}

func NewFilesHandler(logger *logger.Logger) *FilesHandler {
	filesHandlerLogger := logger.Clone()
	filesHandlerLogger.UpdateLastSecondaryPrefix("Files Handler")
	return &FilesHandler{
		logger: filesHandlerLogger,
	}
}

func (f *FilesHandler) GetGeneratedLogDirectoryData() *GeneratedLogDirectoryData {
	return f.generatedLogDirectoryData
}

func (f *FilesHandler) AddLogDirectoryGenerationConfig(logDirectoryGenerationConfig LogDirectoryGenerationConfig) *FilesHandler {
	f.logDirectoryGenerationConfig = &logDirectoryGenerationConfig
	return f
}

func (f *FilesHandler) GenerateServerConfigAndLogDirs() error {
	if err := f.GenerateServerConfiguration(); err != nil {
		return err
	}

	return f.generateLogDirectories()
}

func (f *FilesHandler) GenerateServerConfiguration() (err error) {
	f.logger.Debugf("Generating server configuration files")

	// Write /tmp/server.properties
	err = f.writeKraftServerProperties()
	if err != nil {
		return fmt.Errorf("failed to write server properties: %w", err)
	}

	// (re)create /tmp/kraft-combined-logs
	if err := f.initializeLogDirecotry(KRAFT_LOG_DIRECTORY); err != nil {
		return err
	}

	// Write log_directory/meta.properties
	directoryID, err := uuidToBase64(DIRECTORY_UUID)

	if err != nil {
		return fmt.Errorf("failed to convert directory UUID to base64: %w", err)
	}

	err = f.writeMetaProperties(CLUSTER_ID, directoryID, NODE_ID, META_VERSION)
	if err != nil {
		return fmt.Errorf("failed to write meta properties: %w", err)
	}

	// Write clean shutdown
	err = f.writeKafkaCleanShutdown()
	if err != nil {
		return fmt.Errorf("failed to write clean shutdown: %w", err)
	}

	f.logger.Debugf("Successfully generated server configuration files")
	return nil
}

func (f *FilesHandler) generateLogDirectories() error {
	if f.logDirectoryGenerationConfig == nil {
		panic("Codecrafters Internal Error - GenerateServerConfigAndLogDirectory called without LogDirectoryGenerationConfig")
	}

	generatedLogDirectoryData, err := f.logDirectoryGenerationConfig.Generate(f.logger)

	if err != nil {
		return err
	}

	f.logger.Debugf("Created all log directories")
	f.generatedLogDirectoryData = generatedLogDirectoryData
	return nil
}

func (f *FilesHandler) writeKraftServerProperties() error {
	filePath := SERVER_PROPERTIES_FILE_PATH

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

	f.logger.Debugf("Wrote server properties to %s", filePath)
	return nil
}

func (f *FilesHandler) writeMetaProperties(clusterID, directoryID string, nodeID, version int) error {
	content := fmt.Sprintf("#\n#%s\ncluster.id=%s\ndirectory.id=%s\nnode.id=%d\nversion=%d\n",
		time.Now().Format("Mon Jan 02 15:04:05 MST 2006"), clusterID, directoryID, nodeID, version)

	filePath := path.Join(KRAFT_LOG_DIRECTORY, META_PROPERTIES_FILE_NAME)

	err := os.WriteFile(filePath, []byte(content), 0644)

	if err != nil {
		return fmt.Errorf("error writing meta properties file: %w", err)
	}

	f.logger.Debugf("Wrote meta properties to %s", filePath)
	return nil
}

func (f *FilesHandler) writeKafkaCleanShutdown() error {
	contents := `{"version":0,"brokerEpoch":10}`
	filePath := path.Join(KRAFT_LOG_DIRECTORY, KAFKA_CLEAN_SHUTDOWN_FILE_NAME)

	err := os.WriteFile(filePath, []byte(contents), 0644)

	if err != nil {
		return fmt.Errorf("error writing file to %s: %w", filePath, err)
	}

	f.logger.Debugf("Wrote kafka cleanshutdown to %s", filePath)
	return nil
}

func (f *FilesHandler) initializeLogDirecotry(path string) error {
	err := os.RemoveAll(path)

	if err != nil {
		return fmt.Errorf("could not remove log directory at %s: %w", path, err)
	}

	err = os.MkdirAll(path, 0755)

	if err != nil {
		return fmt.Errorf("could not create log directory at %s: %w", path, err)
	}

	f.logger.Debugf("Created %s", path)
	return nil
}
