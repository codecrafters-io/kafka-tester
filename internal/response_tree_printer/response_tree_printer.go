package response_tree_printer

import (
	"fmt"
	"strings"

	"github.com/codecrafters-io/kafka-tester/protocol/value_storing_decoder"
	"github.com/codecrafters-io/tester-utils/logger"
)

type ResponseTreePrinter struct {
	AssertionError        error
	AssertionErrorLocator string
	DecodeError           error
	DecodeErrorLocator    string // See if we can include this in DecodeError?
	Decoder               *value_storing_decoder.ValueStoringDecoder
	Logger                *logger.Logger
}

func (r *ResponseTreePrinter) Print() {
	lastPrintedLocator := NewLocator("")
	currentIndentationLevel := 0

	buildIndentString := func() string {
		return strings.Repeat(" ", currentIndentationLevel*2)
	}

	for _, locatorString := range r.Decoder.DecodedValuesWithLocators() {
		locator := NewLocator(locatorString)

		if locator.IsSiblingOf(lastPrintedLocator) {
			// We don't need to adjust indentation level
		} else if locator.IsDescendantOf(lastPrintedLocator) {
			// If it's a descendant, we indent and print each ancestor between
			for _, ancestorLocator := range locator.AncestorsFrom(lastPrintedLocator) {
				currentIndentationLevel++
				r.Logger.Infof("%s- %s", buildIndentString(), ancestorLocator.LastSegment())
				lastPrintedLocator = ancestorLocator
			}

			currentIndentationLevel++
		} else {
			// If it's neither a sibling or a descendant, we reset indentation level to 0
			lastPrintedLocator = NewLocator("")
			currentIndentationLevel = 0
		}

		if r.AssertionError != nil && locator.String() == r.AssertionErrorLocator {
			r.Logger.Infof("- %s (Assertion Error)", locator)
			lastPrintedLocator = locator
		}

		if r.DecodeError != nil && locator.String() == r.DecodeErrorLocator {
			r.Logger.Infof("- %s (Decode Error)", locator)
			lastPrintedLocator = locator
		}
	}
}

type Locator struct {
	segments []string
}

func NewLocator(locatorString string) Locator {
	return Locator{segments: strings.Split(locatorString, ".")}
}

func (l Locator) String() string {
	return strings.Join(l.segments, ".")
}

func (l Locator) IsEqual(other Locator) bool {
	return l.String() == other.String()
}

func (l Locator) Is(other Locator) bool {
	return l.String() == other.String()
}

func (l Locator) IsDescendantOf(other Locator) bool {
	return strings.HasPrefix(l.String(), other.String())
}

func (l Locator) AncestorsUntil(other Locator) []Locator {
	if !l.IsDescendantOf(other) {
		panic(fmt.Sprintf("Locator %s is not a descendant of %s", l.String(), other.String()))
	}

	ancestors := []Locator{}

	for l.IsDescendantOf(other) {
		ancestors = append(ancestors, l.Parent())
		l = l.Parent()
	}

	return ancestors
}

func (l Locator) LastSegment() string {
	if len(l.segments) == 0 {
		panic("CodeCrafters Internal Error: LastSegment called on Locator with no segments")
	}

	return l.segments[len(l.segments)-1]
}

func (l Locator) AncestorsFrom(other Locator) []Locator {
	ancestors := l.AncestorsUntil(other)

	// Reverse the slice
	for i := 0; i < len(ancestors)/2; i++ {
		j := len(ancestors) - 1 - i
		ancestors[i], ancestors[j] = ancestors[j], ancestors[i]
	}

	return ancestors
}

func (l Locator) IsChildOf(other Locator) bool {
	return l.Parent().IsEqual(other)
}

func (l Locator) Parent() Locator {
	return Locator{segments: l.segments[:len(l.segments)-1]}
}

func (l Locator) IsSiblingOf(other Locator) bool {
	return l.Parent().Is(other.Parent())
}
