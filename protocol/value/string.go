package value

// String is a type of data structure whose encoding is done as
// length -> 2 bytes, followed by string contents
// It is different from compact string and compact nullable string
type String struct {
	Value string
}

func (v String) String() string {
	return v.Value
}

func (v String) GetType() string {
	return "STRING"
}
