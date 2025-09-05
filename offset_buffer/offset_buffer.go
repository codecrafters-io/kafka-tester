package offset_buffer

// TODO[PaulRefactor]: See if there's a stdlib equivalent for this, or another package we can use -- feels super generic and unlikely it's an "unsolved" problem?
type Buffer struct {
	data   []byte
	offset uint64
}

func NewBuffer(bytes []byte) *Buffer {
	return &Buffer{data: bytes}
}

func (b *Buffer) Offset() uint64 {
	return b.offset
}

func (b *Buffer) RemainingBytesCount() uint64 {
	return uint64(len(b.data)) - b.offset
}

func (b *Buffer) AllBytes() []byte {
	return b.data
}

func (b *Buffer) RemainingBytes() []byte {
	return b.data[b.offset:]
}

// ReadRawBytes always returns `numberOfBytesToRead` bytes from b.data
// If the remaining bytes is less than the specified, it will panic
// It is the caller's responsibility to ensure they have enough bytes by calling RemainingBytesCount()
func (b *Buffer) ReadRawBytes(length uint64) []byte {
	if b.RemainingBytesCount() < length {
		panic("Codecrafters Internal Error - Remaining bytes is less than `numberOfBytesToRead`")
	}
	result := make([]byte, length)
	copy(result, b.data[b.offset:])
	b.offset += length
	return result
}
