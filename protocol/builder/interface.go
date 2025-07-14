package builder

type RequestI interface {
	Encode() []byte
	GetApiType() string
	GetApiVersion() int16
	GetCorrelationId() int32
}
