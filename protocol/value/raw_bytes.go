package value

import "fmt"

type RawBytes struct {
	Value []byte
}

func (v RawBytes) String() string {
	if v.Value == nil {
		return ""
	}
	return fmt.Sprintf("%v -> UTF-8: (%s)", v.Value, string(v.Value))
}

func (v RawBytes) GetType() string {
	return "RAW_BYTES"
}
