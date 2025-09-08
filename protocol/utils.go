package protocol

import (
	"fmt"
	"strings"

	"github.com/codecrafters-io/tester-utils/logger"
)

func PrintHexdump(data []byte) {
	fmt.Printf("Hexdump of data:\n")
	for i, b := range data {
		if i%16 == 0 {
			fmt.Printf("\n%04x  ", i)
		}
		fmt.Printf("%02x ", b)
	}
	fmt.Println()
}

func LogWithIndentation(logger *logger.Logger, indentation int, message string, args ...interface{}) {
	logger.Debugf(fmt.Sprintf("%s%s", strings.Repeat(" ", indentation*2), message), args...)
}

func SuccessLogWithIndentation(logger *logger.Logger, indentation int, message string, args ...interface{}) {
	logger.Successf(fmt.Sprintf("%s%s", strings.Repeat(" ", indentation*2), message), args...)
}
