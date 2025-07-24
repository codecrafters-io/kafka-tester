package kafka_interface

type RequestI interface {
	Encode() []byte
	GetHeader() kafkaapi.RequestHeader
}
