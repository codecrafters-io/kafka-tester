package builder

type RequestI interface {
	Encode() []byte
}

// TODO
//type Assertion struct {
// actualResponse kafkaResponseI
// expectedResponse kafkaResponseI
//	logger *logger.Logger
//	err    error
//}

type Assertion interface {
	Run() error
}
