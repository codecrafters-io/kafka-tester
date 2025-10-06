package decoder

// DecoderError is a type of error that occurs in the process of decoding
type DecoderError interface {
	error
	StartOffset() int
	EndOffset() int
}
