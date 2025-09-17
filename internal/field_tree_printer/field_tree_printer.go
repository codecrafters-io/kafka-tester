package field_tree_printer

import (
	"strings"

	"github.com/codecrafters-io/kafka-tester/internal/field_path"
	"github.com/codecrafters-io/kafka-tester/protocol/value"
	"github.com/codecrafters-io/tester-utils/logger"
)

type Field interface {
	GetPath() field_path.FieldPath
	GetValue() value.KafkaProtocolValue
}

type FieldTreePrinter struct {
	Fields []Field
	Logger *logger.Logger

	currentIndentationLevel int
	lastPrintedFieldPath    field_path.FieldPath
}

func (p FieldTreePrinter) PrintForErrorLogs(errorPath field_path.FieldPath, errorMessage string) {
	p.currentIndentationLevel = 0
	p.lastPrintedFieldPath = field_path.NewFieldPath("")

	for _, field := range p.Fields {
		p.printNodesLeadingTo(field.GetPath(), p.Logger.Infof)

		if errorPath.Is(field.GetPath()) {
			p.Logger.Infof("%s❌ %s (%s)", p.buildIndentPrefix(), field.GetPath().LastSegment(), field.GetValue().String())
			return
		} else {
			p.Logger.Infof("%s- %s (%s)", p.buildIndentPrefix(), field.GetPath().LastSegment(), field.GetValue().String())
			p.lastPrintedFieldPath = field.GetPath()
		}
	}

	// If ErrorPath didn't match any decoded fields, must be an error
	p.printNodesLeadingTo(errorPath, p.Logger.Infof)
	p.Logger.Errorf("%sX %s (%s)", p.buildIndentPrefix(), errorPath.LastSegment(), errorMessage)
}

func (p FieldTreePrinter) PrintForDebugLogs() {
	p.currentIndentationLevel = 0
	p.lastPrintedFieldPath = field_path.NewFieldPath("")

	for _, field := range p.Fields {
		p.printNodesLeadingTo(field.GetPath(), p.Logger.Debugf)
		p.Logger.Debugf("%s- %s (%s)", p.buildIndentPrefix(), field.GetPath().LastSegment(), field.GetValue().String())
		p.lastPrintedFieldPath = field.GetPath()
	}
}

func (p FieldTreePrinter) buildIndentPrefix() string {
	return strings.Repeat(" ", p.currentIndentationLevel*2)
}

func (p *FieldTreePrinter) printNodesLeadingTo(nextPath field_path.FieldPath, logFn func(string, ...interface{})) {
	if nextPath.IsSiblingOf(p.lastPrintedFieldPath) {
		// If the next path is a sibling, we don't need to print nodes or adjust indentation level
	} else if nextPath.IsDescendantOf(p.lastPrintedFieldPath) {
		// If the next path is a descendant, we indent and print each ancestor between
		for _, descendantPath := range p.lastPrintedFieldPath.DescendantsUntil(nextPath) {
			logFn("%s- %s", p.buildIndentPrefix(), descendantPath.LastSegment())
			p.currentIndentationLevel++
		}
	} else {
		// If it's neither a sibling or a descendant, we reset indentation level until the common ancestor
		commonAncestor := nextPath.CommonAncestor(p.lastPrintedFieldPath)
		p.currentIndentationLevel = len(commonAncestor.Segments)
		p.lastPrintedFieldPath = commonAncestor
		p.printNodesLeadingTo(nextPath, logFn)
	}
}
