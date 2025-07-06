package builder

type RequestI interface {
	Encode() []byte
}
