package kafka_interface

type RequestHeaderI interface {
	Encode() []byte
}
