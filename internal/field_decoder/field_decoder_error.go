package field_decoder

import (
	"github.com/codecrafters-io/kafka-tester/internal/field_path"
)

type FieldDecoderError interface {
	error
	Path() field_path.FieldPath
	StartOffset() int
	EndOffset() int
}

type fieldDecoderErrorImpl struct {
	message     string
	startOffset int
	endOffset   int
	path        field_path.FieldPath
}

func (e *fieldDecoderErrorImpl) Error() string {
	return e.message
}

func (e *fieldDecoderErrorImpl) StartOffset() int {
	return int(e.startOffset)
}

func (e *fieldDecoderErrorImpl) EndOffset() int {
	return int(e.endOffset)
}

func (e *fieldDecoderErrorImpl) Path() field_path.FieldPath {
	return e.path
}
