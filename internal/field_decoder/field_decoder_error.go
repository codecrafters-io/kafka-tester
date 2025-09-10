package field_decoder

import (
	"github.com/codecrafters-io/kafka-tester/internal/field_path"
)

type FieldDecoderError interface {
	error
	Path() field_path.FieldPath
	Offset() int
}

type fieldDecoderErrorImpl struct {
	message string
	offset  int
	path    field_path.FieldPath
}

func (e *fieldDecoderErrorImpl) Error() string {
	return e.message
}

func (e *fieldDecoderErrorImpl) Offset() int {
	return int(e.offset)
}

func (e *fieldDecoderErrorImpl) Path() field_path.FieldPath {
	return e.path
}
