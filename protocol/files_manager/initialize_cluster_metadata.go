package files_manager

import (
	"fmt"
	"os"

	"github.com/codecrafters-io/kafka-tester/protocol/common"
)

func (f *FilesManager) InitializeClusterMetadata() (writeError error) {
	logger := f.logger

	logger.UpdateLastSecondaryPrefix("Files Manager")

	// Cleanup on error
	defer func() {
		logger.ResetSecondaryPrefixes()
		f.ResetIndentationLevel()

		if writeError != nil {
			// Only log the cleanup error, write error will be returned automatically and logged later by test runner
			if cleanupError := cleanUp(); cleanupError != nil {
				logger.Errorf("failed to cleanup: %s", cleanupError)
			}
		}
	}()

	logDir := common.LOG_DIR

	// Clean up any existing files/directories first
	if cleanUpError := cleanUp(); cleanUpError != nil {
		return fmt.Errorf("failed to cleanup: %s", cleanUpError)
	}

	logger.Debugf("Writing cluster metadata files")
	f.IndentLogs()

	// Write kraft server properties
	if writeError = f.writeKraftServerProperties(); writeError != nil {
		return fmt.Errorf("failed to write kraft server properties: %w", writeError)
	}

	// Create log directory
	if err := os.MkdirAll(logDir, 0755); err != nil {
		return fmt.Errorf("failed to create log directory %s: %w", logDir, err)
	}

	logger.Debugf("%sCreated log directory: %s", f.getIndentedPrefix(), logDir)

	// Write kafka clean shutdown
	if err := f.writeKafkaCleanShutdown(); err != nil {
		return fmt.Errorf("failed to write kafka clean shutdown: %w", err)
	}

	// Write meta properties
	if err := f.writeMetaProperties(logger); err != nil {
		return fmt.Errorf("failed to write meta properties: %w", err)
	}

	f.DeIndentLogs()
	logger.Debugf("Successfully initialized all cluster metadata files")
	return nil
}

func cleanUp() error {
	logdir := common.LOG_DIR

	// rm -rf /tmp/kraft-combined-logs/)
	if err := os.RemoveAll(logdir); err != nil {
		return err
	}

	// rm /tmp/server.properties
	return os.RemoveAll(common.SERVER_PROPERTIES_FILE_PATH)
}
