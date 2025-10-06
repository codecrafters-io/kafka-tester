package decoder

// DecoderError is any type of error that occurs while decoding
type DecoderError interface {
	error
	StartOffset() int
	EndOffset() int
}
