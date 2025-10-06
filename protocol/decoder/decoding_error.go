package decoder

// decodingError is an error that occurs in the process of decoding
// For eg. no bytes remaining, invalid bytes in varint, etc.
// For this, the start and end offset is the same value because we only point to the
// byte from where the error was encountered while trying to decode a value
type decodingError struct {
	message string
	offset  int
}

func (e *decodingError) Error() string {
	return e.message
}

func (e *decodingError) StartOffset() int {
	return e.offset
}

func (e *decodingError) EndOffset() int {
	return e.offset
}
