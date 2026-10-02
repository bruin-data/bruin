package sqlparser

import (
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
)

// The *_contract_test.go suites characterize the public Go calls against the
// embedded SQLGlot runtime. Run them independently with:
//
//   go test -tags=no_duckdb_arrow ./pkg/sqlparser -run Contract -count=1
//
// Expectations are literal SQL/results, not SQL normalized by the parser under
// test. Keep types, casing, errors and omissions intact. Comments call out known
// bugs: recording one here does not endorse it or make emitted SQL safe to run.
// Each operation has its own test entry point so a replacement can be compared
// operation by operation without routing expected values through SQLGlot.

// These are Bruin's distinct parser dialects, not every dialect SQLGlot accepts.
// Keep this list explicit: adding a platform must also extend the contract suite.
// Platform aliases (e.g. MotherDuck -> DuckDB) are tested by the mapping tests.
var contractDialects = []string{
	"athena", "bigquery", "clickhouse", "databricks", "doris", "duckdb",
	"fabric", "mysql", "oracle", "postgres", "redshift", "snowflake",
	"spark", "starrocks", "trino", "tsql",
}

// SQLGlot's error location and ANSI highlighting are observable in Go errors.
// These literals deliberately do not ask the parser to format expectations.
const (
	contractSelectFromError     = "Expected table name but got <Token token_type: TokenType.SENTINEL, text: SENTINEL, line: 1, col: 1, start: 0, end: 0, comments: []>. Line 1, Col: 11.\n  SELECT \x1b[4mFROM\x1b[0m"
	contractSelectStarFromError = "Expected table name but got <Token token_type: TokenType.SENTINEL, text: SENTINEL, line: 1, col: 1, start: 0, end: 0, comments: []>. Line 1, Col: 13.\n  SELECT * \x1b[4mFROM\x1b[0m"
)

func TestSQLParserContractDialectCoverage(t *testing.T) {
	t.Parallel()

	registered := map[string]bool{}
	for _, dialect := range assetTypeDialectMap {
		registered[dialect] = true
	}
	for _, dialect := range connectionTypeDialectMap {
		registered[dialect] = true
	}
	dialects := make([]string, 0, len(registered))
	for dialect := range registered {
		dialects = append(dialects, dialect)
	}
	sort.Strings(dialects)
	require.Equal(t, dialects, contractDialects, "extend the Go characterization tests when registering a dialect")
}
