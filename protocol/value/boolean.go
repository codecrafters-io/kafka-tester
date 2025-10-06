package value

type Boolean struct {
	Value bool
}

func (v Boolean) String() string {
	if !v.Value {
		return "False"
	}
	return "True"
}

func (v Boolean) GetType() string {
	return "BOOLEAN"
}

type AugmentedBoolean struct {
	KafkaValue  Boolean
	Path        string
	StartOffset int
	EndOffset   int
}

func (a AugmentedBoolean) GetKafkaValue() KafkaProtocolValue {
	return a.KafkaValue
}

func (a AugmentedBoolean) GetPath() string {
	return a.Path
}

func (a AugmentedBoolean) GetStartOffset() int {
	return a.StartOffset
}

func (a AugmentedBoolean) GetEndOffset() int {
	return a.EndOffset
}
