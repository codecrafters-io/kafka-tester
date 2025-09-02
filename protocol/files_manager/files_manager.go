package files_manager

import (
	"strings"

	"github.com/codecrafters-io/tester-utils/logger"
)

type FilesManager struct {
	logger           *logger.Logger
	indentationLevel int
}

func NewFilesManager(logger *logger.Logger) *FilesManager {
	return &FilesManager{
		logger:           logger,
		indentationLevel: 0,
	}
}

func (f *FilesManager) getIndentedPrefix() string {
	return strings.Repeat("  - ", f.indentationLevel)
}

func (f *FilesManager) IndentLogs() {
	f.indentationLevel += 1
}

func (f *FilesManager) DeIndentLogs() {
	f.indentationLevel = max(f.indentationLevel-1, 0)
}

func (f *FilesManager) ResetIndentationLevel() {
	f.indentationLevel = 0
}
