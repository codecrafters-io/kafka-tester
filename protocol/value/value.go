package value

import (
	"fmt"
)

const (
	TAG_BUFFER = "TAG_BUFFER"
)

type KafkaProtocolValue interface {
	GetVariableName() string
	GetValueString() string
	GetType() string
}

// Primitive Types

type Int16 struct {
	VariableName string
	Value        int16
}

func (v *Int16) GetVariableName() string {
	return v.VariableName
}

func (v *Int16) GetValueString() string {
	return fmt.Sprintf("%d", v.Value)
}

func (v *Int16) GetType() string {
	return "INT16"
}

type Int32 struct {
	VariableName string
	Value        int32
}

func (v *Int32) GetVariableName() string {
	return v.VariableName
}

func (v *Int32) GetValueString() string {
	return fmt.Sprintf("%d", v.Value)
}

func (v *Int32) GetType() string {
	return "INT32"
}

type UnsignedVarint struct {
	VariableName string
	Value        uint64
}

func (v *UnsignedVarint) GetVariableName() string {
	return v.VariableName
}

func (v *UnsignedVarint) GetValueString() string {
	return fmt.Sprintf("%d", v.Value)
}

func (v *UnsignedVarint) GetType() string {
	return "UNSIGNED_VARINT"
}

type CompactArrayLength struct {
	VariableName string
	Value        uint64
}

func (v *CompactArrayLength) GetVariableName() string {
	return v.VariableName
}

func (v *CompactArrayLength) GetValueString() string {
	switch v.Value {
	case 0:
		return "0 (NULL ARRAY)"
	case 1:
		return "1 (EMPTY ARRAY)"
	}
	return fmt.Sprintf("%d", v.Value-1)
}

func (v *CompactArrayLength) ActualLength() uint64 {
	if v.Value == 0 {
		return v.Value
	}

	if v.Value == 1 {
		return 0
	}

	return v.Value - 1
}

func (v *CompactArrayLength) GetType() string {
	return "UNSIGNED_VARINT"
}
