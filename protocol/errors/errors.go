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

func (e *PacketDecodingError) AddContexts(context ...string) *PacketDecodingError {
	e.Context = append(context, e.Context...)
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
