package value

type CompactString struct {
	Value string
}

func (v CompactString) String() string {
	// Print quotes in case of empty string
	if v.Value == "" {
		return `""`
	}
	return string(v.Value)
}

func (v CompactString) GetType() string {
	return "COMPACT_STRING"
}
