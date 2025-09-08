package field_path

import (
	"fmt"
	"strings"
)

type FieldPath struct {
	Segments []string
}

func NewFieldPath(pathString string) FieldPath {
	return FieldPath{Segments: strings.Split(pathString, ".")}
}

func (p FieldPath) String() string {
	return strings.Join(p.Segments, ".")
}

func (p FieldPath) Is(other FieldPath) bool {
	return p.String() == other.String()
}

func (p FieldPath) IsDescendantOf(other FieldPath) bool {
	if other.String() == "" {
		return true
	}
	return strings.HasPrefix(p.String(), other.String()+".")
}

func (p FieldPath) CommonAncestor(other FieldPath) FieldPath {
	commonSegments := []string{}

	minLength := len(p.Segments)
	if len(other.Segments) < minLength {
		minLength = len(other.Segments)
	}

	for i := 0; i < minLength; i++ {
		if p.Segments[i] == other.Segments[i] {
			commonSegments = append(commonSegments, p.Segments[i])
		} else {
			break
		}
	}

	return FieldPath{Segments: commonSegments}
}

func (p FieldPath) AncestorsUntil(other FieldPath) []FieldPath {
	if !p.IsDescendantOf(other) {
		panic(fmt.Sprintf("FieldPath %s is not a descendant of %s", p.String(), other.String()))
	}

	ancestors := []FieldPath{}
	current := p.Parent()

	for !current.Is(other) {
		ancestors = append(ancestors, current)
		current = current.Parent()
	}

	return ancestors
}

func (p FieldPath) LastSegment() string {
	if len(p.Segments) == 0 {
		panic("CodeCrafters Internal Error: LastSegment called on FieldPath with no segments")
	}

	return p.Segments[len(p.Segments)-1]
}

func (p FieldPath) DescendantsUntil(other FieldPath) []FieldPath {
	descendants := other.AncestorsUntil(p)

	// Reverse the slice
	for i := 0; i < len(descendants)/2; i++ {
		j := len(descendants) - 1 - i
		descendants[i], descendants[j] = descendants[j], descendants[i]
	}

	return descendants
}

func (p FieldPath) Parent() FieldPath {
	if len(p.Segments) == 0 {
		return FieldPath{Segments: []string{}}
	}

	return FieldPath{Segments: p.Segments[:len(p.Segments)-1]}
}

func (p FieldPath) IsSiblingOf(other FieldPath) bool {
	if len(p.Segments) == 0 || len(other.Segments) == 0 {
		return false
	}

	return p.Parent().Is(other.Parent())
}
