package flightsql

import (
	"fmt"
	"strings"
)

// sqlDialect captures the engine specific pieces of SQL generation. Flight SQL
// itself is a transport protocol and says nothing about SQL syntax, so
// materialization has to be dialect specific. New Flight SQL engines can be
// supported by implementing this interface and registering them in
// dialectByName.
type sqlDialect interface {
	// quoteIdentifier quotes a possibly dotted identifier (schema.table).
	quoteIdentifier(identifier string) string
}

// ansiDialect implements the dialect-agnostic baseline shared across Flight SQL
// engines. It double-quotes identifiers, which is the ANSI standard.
type ansiDialect struct{}

func (ansiDialect) quoteIdentifier(identifier string) string {
	parts := strings.Split(identifier, ".")
	quoted := make([]string, len(parts))
	for i, part := range parts {
		quoted[i] = fmt.Sprintf(`"%s"`, part)
	}
	return strings.Join(quoted, ".")
}

// dremioDialect builds on the ANSI baseline. Dremio's SQL is largely ANSI
// compatible, so it currently inherits the baseline behaviour wholesale.
// Engine specific overrides can be added here as methods that shadow the
// embedded ansiDialect.
type dremioDialect struct {
	ansiDialect
}

// dialectByName resolves a connection's configured dialect string to a
// concrete sqlDialect. An empty value defaults to Dremio, which is the first
// supported Flight SQL engine.
//
//nolint:ireturn
func dialectByName(name string) sqlDialect {
	switch strings.ToLower(strings.TrimSpace(name)) {
	case "", "dremio":
		return dremioDialect{}
	default:
		return ansiDialect{}
	}
}
