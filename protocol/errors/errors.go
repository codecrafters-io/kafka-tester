package errors

import (
	"fmt"
)

type PacketDecodingError struct {
	Message string
	Context []string
}

func (e *PacketDecodingError) Error() string {
	return fmt.Sprintf("Error: %s\nContext:\n%s", e.Message, getFormattedContext(e.Context))
}

func NewPacketDecodingError(message string) *PacketDecodingError {
	return &PacketDecodingError{
		Message: message,
	}
}

func (e *PacketDecodingError) AddContexts(contexts ...string) *PacketDecodingError {
	nonEmptyContexts := []string{}
	for _, context := range contexts {
		if context != "" {
			nonEmptyContexts = append(nonEmptyContexts, context)
		}
	}
	e.Context = append(nonEmptyContexts, e.Context...)
	return e
}

func getFormattedContext(arr []string) string {
	outputString := ""
	prefix := ""
	for _, v := range arr {
		if v != "" {
			outputString += fmt.Sprintf("%s- %s\n", prefix, v)
			prefix += "  "
		}
	}
	return outputString
}
