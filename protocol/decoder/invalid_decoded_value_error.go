package decoder

// InvalidDecodedValueError is used where the value of a kafka protocol value has already been decoded
// but the value of the decoded type is not valid. For example,
// While decoding CompactString, it is stored as (<length_uvarint>-<string_contents>)
// Even if the length is decoded successfully, if it is 0, it is invalid.
// This error has start and end offset so it can be later pointed out in inspectable hex dump
type invalidDecodedValueError struct {
	message     string
	startOffset int
	endOffset   int
}

func (e *invalidDecodedValueError) Error() string {
	return e.message
}

func (e *invalidDecodedValueError) StartOffset() int {
	return e.startOffset
}

func (e *invalidDecodedValueError) EndOffset() int {
	return e.endOffset
}
