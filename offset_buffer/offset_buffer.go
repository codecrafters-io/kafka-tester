package offset_buffer

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

func (b *Buffer) MustReadNBytes(length uint64) []byte {
	if b.RemainingBytesCount() < length {
		panic("Codecrafters Internal Error - Remaining bytes is less than length")
	}

	result := make([]byte, length)
	copy(result, b.data[b.offset:])
	b.offset += length
	return result
}
