package value

// UUID represents uuid of the format given by utils.DecodeUUID
type UUID struct {
	Value string
}

func (v UUID) String() string {
	return v.Value
}

func (v UUID) GetType() string {
	return "UUID"
}

type AugmentedUUID struct {
	KafkaValue  UUID
	Path        string
	StartOffset int
	EndOffset   int
}

func (a AugmentedUUID) GetKafkaValue() KafkaProtocolValue {
	return a.KafkaValue
}

func (a AugmentedUUID) GetPath() string {
	return a.Path
}

func (a AugmentedUUID) GetStartOffset() int {
	return a.StartOffset
}

func (a AugmentedUUID) GetEndOffset() int {
	return a.EndOffset
}
