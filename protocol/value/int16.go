package value

import "fmt"

type Int16 struct {
	Value int16
}

func (v Int16) String() string {
	return fmt.Sprintf("%d", v.Value)
}

func (v Int16) GetType() string {
	return "INT16"
}
