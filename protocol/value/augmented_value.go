package value

type AugmentedValue interface {
	GetPath() string
	GetStartOffset() int
	GetEndOffset() int
	GetKafkaValue() KafkaProtocolValue
}
