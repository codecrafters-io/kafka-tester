package decoder

type DecoderError interface {
	error
	StartOffset() int
	EndOffset() int
}

type decoderErrorImpl struct {
	message     string
	startOffset int
	endOffset   int
}

func (e *decoderErrorImpl) Error() string {
	return e.message
}

func (e *decoderErrorImpl) StartOffset() int {
	return e.startOffset
}

func (e *decoderErrorImpl) EndOffset() int {
	return e.endOffset
}

func NewDecoderErrorForOffset(err error, offset int) *decoderErrorImpl {
	return &decoderErrorImpl{
		message:     err.Error(),
		startOffset: offset,
		endOffset:   offset,
	}
}

func NewDecoderErrorForRange(err error, startOffset, endOffset int) *decoderErrorImpl {
	return &decoderErrorImpl{
		message:     err.Error(),
		startOffset: startOffset,
		endOffset:   endOffset,
	}
}
