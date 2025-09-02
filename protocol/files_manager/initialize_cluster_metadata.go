package files_manager

import (
	"fmt"
	"os"

	"github.com/codecrafters-io/kafka-tester/protocol/common"
	"github.com/codecrafters-io/tester-utils/logger"
)

func InitializeClusterMetadata(logger *logger.Logger) (writeError error) {
	logDir := common.LOG_DIR

	// Clean up any existing files/directories first
	if cleanUpError := cleanUp(); cleanUpError != nil {
		return fmt.Errorf("failed to cleanup: %s", cleanUpError)
	}

	// Cleanup on error
	defer func() {
		if writeError != nil {
			// Only log the cleanup error, write error will be returned automatically and logged later by test runner
			if cleanupError := cleanUp(); cleanupError != nil {
				logger.Errorf("failed to cleanup: %s", cleanupError)
			}
		}
	}()

	// Write kraft server properties
	if writeError = writeKraftServerProperties(logger); writeError != nil {
		return fmt.Errorf("failed to write kraft server properties: %w", writeError)
	}

	// Create log directory
	if err := os.MkdirAll(logDir, 0755); err != nil {
		return fmt.Errorf("failed to create log directory %s: %w", logDir, err)
	}

	logger.Debugf("Created log directory: %s", logDir)

	// Write kafka clean shutdown
	if err := writeKafkaCleanShutdown(logger); err != nil {
		return fmt.Errorf("failed to write kafka clean shutdown: %w", err)
	}

	// Write meta properties
	if err := writeMetaProperties(logger); err != nil {
		return fmt.Errorf("failed to write meta properties: %w", err)
	}

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
