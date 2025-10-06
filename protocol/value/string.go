package value

// String is a type of data structure whose encoding is done as
// length -> 2 bytes, followed by string contents
// It is different from compact string and compact nullable string
type String struct {
	Value string
}

func (v String) String() string {
	if v.Value == "" {
		return `""`
	}
	return v.Value
}

func (v String) GetType() string {
	return "STRING"
}

type AugmentedString struct {
	KafkaValue  String
	Path        string
	StartOffset int
	EndOffset   int
}

func (a AugmentedString) GetKafkaValue() KafkaProtocolValue {
	return a.KafkaValue
}

func (a AugmentedString) GetPath() string {
	return a.Path
}

func (a AugmentedString) GetStartOffset() int {
	return a.StartOffset
}

func (a AugmentedString) GetEndOffset() int {
	return a.EndOffset
}
