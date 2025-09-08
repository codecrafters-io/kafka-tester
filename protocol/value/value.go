package value

type KafkaProtocolValue interface {
	String() string
	GetType() string
}
