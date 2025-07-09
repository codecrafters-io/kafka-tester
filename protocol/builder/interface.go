package builder

type RequestI interface {
	Encode() []byte
}

type Assertion interface {
	Run() error
}
