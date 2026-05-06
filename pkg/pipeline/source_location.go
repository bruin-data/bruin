package pipeline

import "fmt"

// SourceLocation describes the position of a code element within a source file.
type SourceLocation struct {
	File   string `json:"file,omitempty"   yaml:"-" mapstructure:"-"`
	Line   int    `json:"line,omitempty"   yaml:"-" mapstructure:"-"`
	Column int    `json:"column,omitempty" yaml:"-" mapstructure:"-"`
}

// IsZero reports whether the location carries no meaningful position.
func (l *SourceLocation) IsZero() bool {
	return l == nil || l.Line <= 0
}

// String formats the location as "file:line:col".
func (l *SourceLocation) String() string {
	if l.IsZero() {
		return ""
	}
	if l.Column > 0 {
		return fmt.Sprintf("%s:%d:%d", l.File, l.Line, l.Column)
	}
	return fmt.Sprintf("%s:%d", l.File, l.Line)
}
