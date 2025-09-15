package value

import "fmt"

type Varint struct {
	Value int64
}

func (v Varint) String() string {
	return fmt.Sprintf("%d", v.Value)
}

func (v Varint) GetType() string {
	return "VARINT"
}
