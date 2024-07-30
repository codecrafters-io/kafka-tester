package errors

type PacketDecodingError struct {
	message string
}

func (err PacketDecodingError) Error() string {
	return err.message
}

var (
	ErrInsufficientData = PacketDecodingError{"insufficient data"}
)

var (
	ErrInvalidArrayLength     = PacketDecodingError{"invalid array length"}
	ErrInvalidByteSliceLength = PacketDecodingError{"invalid byteslice length"}
	ErrInvalidStringLength    = PacketDecodingError{"invalid string length"}
	ErrVarintOverflow         = PacketDecodingError{"varint overflow"}
	ErrUVarintOverflow        = PacketDecodingError{"uvarint overflow"}
	ErrInvalidBool            = PacketDecodingError{"invalid bool"}
)
