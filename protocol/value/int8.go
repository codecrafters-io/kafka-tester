package value

import "fmt"

type Int8 struct {
	Value int8
}

func (v Int8) String() string {
	return fmt.Sprintf("%d", v.Value)
}

func (v Int8) GetType() string {
	return "INT8"
}
