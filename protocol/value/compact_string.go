package value

type CompactString struct {
	Value string
}

func (v CompactString) String() string {
	return string(v.Value)
}

func (v CompactString) GetType() string {
	return "COMPACT_STRING"
}
