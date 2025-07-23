package kafka_interface

type RequestI interface {
	Encode() []byte
}

type RequestHeaderI interface {
	Encode() []byte
}

type RequestBodyI interface {
	Encode() []byte
}
