package decoder

type DecoderError interface {
	error
	Offset() int
}

type decoderErrorImpl struct {
	message string
	offset  int
}

func (e *decoderErrorImpl) Error() string {
	return e.message
}

func (e *decoderErrorImpl) Offset() int {
	return int(e.offset)
}
