package errors_legacy

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

func NewPacketDecodingError(message string, context ...string) *PacketDecodingError {
	return &PacketDecodingError{
		Message: message,
		Context: context,
	}
}

func (e *PacketDecodingError) WithAddedContext(context string) *PacketDecodingError {
	e.Context = append([]string{context}, e.Context...)
	return e
}

func getFormattedContext(arr []string) string {
	outputString := ""
	prefix := ""
	for _, v := range arr {
		outputString += fmt.Sprintf("%s- %s\n", prefix, v)
		prefix += "  "
	}
	return outputString
}
