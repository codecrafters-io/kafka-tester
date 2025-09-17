package value

// KafkaString is a type of data structure whose encoding is done as
// length -> 2 bytes, followed by string contents
// It is different from compact string and compact nullable string
type KafkaString struct {
	Value string
}

func (v KafkaString) String() string {
	return v.Value
}

func (v KafkaString) GetType() string {
	return "STRING"
}
