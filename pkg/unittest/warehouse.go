// Package unittest builds the SQL that runs a Bruin unit test on the asset's
// configured connection. The tables the asset reads are replaced by inline
// fixture CTEs, so the test runs as a single read-only SELECT that issues no
// DDL and leaves no artifacts on the target.
package unittest

import (
	"errors"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/bruin-data/bruin/pkg/sqlparser"
)

// Rewriter is the subset of the SQL parser the warehouse builder needs.
// *sqlparser.SQLParser satisfies it.
type Rewriter interface {
	ExtractSelect(sql string, dialect string) (string, error)
	UsedTables(sql string, dialect string) ([]string, error)
	RenameTables(sql string, dialect string, mapping map[string]string) (string, error)
	PrependCTEs(sql string, dialect string, ctes []sqlparser.CTE) (string, error)
	SelectFromCTE(sql string, dialect string, cteName string) (string, error)
	FreezeTime(sql string, dialect string, executionTime string) (string, error)
}

// fixtureCTE is the CTE that replaces one table the query reads: ref is the
// table name as written in the query (the rewrite target), body is the SELECT
// the CTE evaluates to (fixture rows, or a typed empty stub).
type fixtureCTE struct {
	ref  string
	body string
}

// BuildWarehouseQuery rewrites an asset's rendered SQL into a single read-only
// SELECT that reads from inline fixture CTEs instead of real tables, so a unit
// test can run on the configured warehouse with no DDL and no artifacts.
//
// Every table the query reads must be accounted for, or it would hit real data:
// a mocked input becomes a fixture CTE built from its rows; an unmocked read
// whose asset declares columns becomes an empty (zero-row) typed CTE; an
// unmocked read with no declared columns is an error.
//
// To assert an intermediate CTE instead of the final output, derive its query
// from this result with Rewriter.SelectFromCTE (WITH … SELECT * FROM <cte>) —
// the fixtures are already injected, so it needs no separate build.
func BuildWarehouseQuery(p Rewriter, dialect, renderedSQL string, test pipeline.UnitTest, schemas map[string][]pipeline.Column) (string, error) {
	injected, err := buildInjected(p, dialect, renderedSQL, test, schemas)
	if err != nil {
		return "", err
	}
	return freezeIfRequested(p, dialect, injected, test)
}

// freezeIfRequested pins CURRENT_TIMESTAMP/DATE/TIME to the test's
// execution_time when set, so a time-dependent assertion is deterministic.
func freezeIfRequested(p Rewriter, dialect, sql string, test pipeline.UnitTest) (string, error) {
	if test.ExecutionTime == "" {
		return sql, nil
	}
	frozen, err := p.FreezeTime(sql, dialect, test.ExecutionTime)
	if err != nil {
		return "", fmt.Errorf("failed to freeze execution_time: %w", err)
	}
	return frozen, nil
}

// buildInjected produces the single read-only SELECT that reads from inline
// fixture CTEs instead of real tables (no DDL, no artifacts), shared by the
// final-output and CTE-level builders.
func buildInjected(p Rewriter, dialect, renderedSQL string, test pipeline.UnitTest, schemas map[string][]pipeline.Column) (string, error) {
	// Reduce the asset to the SELECT that produces its rows: a materialization:
	// none asset can be full DDL (CREATE OR REPLACE VIEW … AS SELECT, CTAS,
	// INSERT … SELECT), and a unit test exercises only the inner SELECT. This
	// also keeps the test read-only: the CREATE/INSERT never reaches the target.
	renderedSQL, err := p.ExtractSelect(renderedSQL, dialect)
	if err != nil {
		return "", fmt.Errorf("cannot unit test this asset: %w", err)
	}

	used, err := p.UsedTables(renderedSQL, dialect)
	if err != nil {
		return "", fmt.Errorf("failed to determine the tables the query reads: %w", err)
	}

	normalizedSchemas := make(map[string][]pipeline.Column, len(schemas))
	for name, cols := range schemas {
		normalizedSchemas[normalizeName(name)] = cols
	}

	// Each table the query reads maps to the fixture CTE that replaces it, keyed
	// by the normalized read name so mocked inputs and discovered reads share one
	// key space. ref keeps the name as written in the query, for the rewrite.
	fixtures := make(map[string]fixtureCTE)

	for _, in := range test.Inputs {
		body, err := fixtureSelect(in, normalizedSchemas[normalizeName(in.Asset)])
		if err != nil {
			return "", err
		}
		fixtures[normalizeName(in.Asset)] = fixtureCTE{ref: in.Asset, body: body}
	}
	for _, t := range used {
		key := normalizeName(t)
		if _, ok := fixtures[key]; ok {
			continue // already mocked by an input above
		}
		cols, ok := normalizedSchemas[key]
		if !ok {
			return "", fmt.Errorf("the query reads %q which is not mocked and declares no columns; connection mode must substitute every read, so mock it in the test or add columns: to its asset", t)
		}
		body, err := emptySelect(cols)
		if err != nil {
			return "", fmt.Errorf("cannot stub %q: %w", t, err)
		}
		fixtures[key] = fixtureCTE{ref: t, body: body}
	}

	if len(fixtures) == 0 {
		return renderedSQL, nil // nothing the query reads needs substituting
	}

	// Build the rename mapping and CTE list in a deterministic order.
	keys := make([]string, 0, len(fixtures))
	for key := range fixtures {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	mapping := make(map[string]string, len(fixtures))
	ctes := make([]sqlparser.CTE, 0, len(fixtures))
	for _, key := range keys {
		f := fixtures[key]
		name := cteName(f.ref)
		mapping[f.ref] = name
		ctes = append(ctes, sqlparser.CTE{Name: name, Query: f.body})
	}

	rewritten, err := p.RenameTables(renderedSQL, dialect, mapping)
	if err != nil {
		return "", fmt.Errorf("failed to point the query at the fixture CTEs: %w", err)
	}
	return p.PrependCTEs(rewritten, dialect, ctes)
}

// fixtureSelect builds the CTE body for a mocked input: each row is a SELECT and
// they are combined with UNION ALL. The first SELECT carries the column names
// (and, for declared columns, a CAST that pins the type rather than letting the
// database infer it); a UNION ALL takes its column names and types from that
// first branch, so later rows are positional. This portable shape avoids the
// `VALUES … AS t(cols)` table-alias form, which BigQuery rejects.
func fixtureSelect(input pipeline.UnitTestInput, schema []pipeline.Column) (string, error) {
	if len(input.Rows) == 0 {
		return "", fmt.Errorf("input %q has no rows", input.Asset)
	}
	cols := collectColumns(input.Rows)
	declared := declaredTypes(schema)

	// Column identifiers are emitted unquoted so they case-fold the same way the
	// asset query's (conventionally unquoted) references do. Quoting would pin a
	// case and break, for example, Snowflake folding unquoted refs to upper case.
	selects := make([]string, len(input.Rows))
	for i, row := range input.Rows {
		exprs := make([]string, len(cols))
		for j, c := range cols {
			lit := sqlLiteral(row[c])
			switch {
			case i != 0:
				exprs[j] = lit // names/types come from the first SELECT
			case declared[c] != "":
				exprs[j] = fmt.Sprintf("CAST(%s AS %s) AS %s", lit, declared[c], c)
			default:
				exprs[j] = fmt.Sprintf("%s AS %s", lit, c)
			}
		}
		selects[i] = "SELECT " + strings.Join(exprs, ", ")
	}

	return strings.Join(selects, " UNION ALL "), nil
}

// emptySelect builds the CTE body for an unmocked read: a typed, zero-row
// SELECT, so the query binds but the upstream contributes nothing.
func emptySelect(schema []pipeline.Column) (string, error) {
	proj := make([]string, 0, len(schema))
	seen := make(map[string]struct{}, len(schema))
	for _, c := range schema {
		if c.Name == "" {
			continue
		}
		if _, dup := seen[c.Name]; dup {
			continue
		}
		seen[c.Name] = struct{}{}
		typ := strings.TrimSpace(c.Type)
		if typ == "" {
			typ = "VARCHAR"
		}
		proj = append(proj, fmt.Sprintf("CAST(NULL AS %s) AS %s", typ, c.Name))
	}
	if len(proj) == 0 {
		return "", errors.New("no usable columns")
	}
	// Typed columns, zero rows: wrap the typed projection in a subquery so the
	// WHERE has a FROM source on every dialect (a bare SELECT … WHERE is not
	// portable), and alias it since Postgres requires a subquery alias.
	return fmt.Sprintf("SELECT * FROM (SELECT %s) AS t WHERE 1 = 0", strings.Join(proj, ", ")), nil
}

// cteName derives a single, valid CTE identifier from a (possibly qualified)
// asset name. The prefix avoids collisions with real identifiers.
func cteName(asset string) string {
	var b strings.Builder
	b.WriteString("__bruin_ut_")
	for _, r := range asset {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9':
			b.WriteRune(r)
		default:
			b.WriteByte('_')
		}
	}
	return b.String()
}

func declaredTypes(schema []pipeline.Column) map[string]string {
	out := make(map[string]string, len(schema))
	for _, c := range schema {
		if c.Name == "" {
			continue
		}
		if typ := strings.TrimSpace(c.Type); typ != "" {
			out[c.Name] = typ
		}
	}
	return out
}

func collectColumns(rows []map[string]interface{}) []string {
	set := make(map[string]struct{})
	for _, row := range rows {
		for k := range row {
			set[k] = struct{}{}
		}
	}
	cols := make([]string, 0, len(set))
	for k := range set {
		cols = append(cols, k)
	}
	sort.Strings(cols)
	return cols
}

func normalizeName(s string) string {
	return strings.ToLower(strings.ReplaceAll(s, `"`, ""))
}

// sqlLiteral renders a fixture value as a portable SQL literal.
func sqlLiteral(v interface{}) string {
	switch val := v.(type) {
	case nil:
		return "NULL"
	case bool:
		if val {
			return "TRUE"
		}
		return "FALSE"
	case int:
		return strconv.Itoa(val)
	case int32:
		return strconv.FormatInt(int64(val), 10)
	case int64:
		return strconv.FormatInt(val, 10)
	case float32:
		return floatLiteral(float64(val))
	case float64:
		return floatLiteral(val)
	case time.Time:
		return quoteStringLiteral(formatTimeLiteral(val))
	case string:
		return quoteStringLiteral(val)
	default:
		return quoteStringLiteral(fmt.Sprintf("%v", val))
	}
}

func floatLiteral(f float64) string {
	if math.IsNaN(f) || math.IsInf(f, 0) {
		return quoteStringLiteral(strconv.FormatFloat(f, 'g', -1, 64))
	}
	return strconv.FormatFloat(f, 'f', -1, 64)
}

func formatTimeLiteral(t time.Time) string {
	if t.Hour() == 0 && t.Minute() == 0 && t.Second() == 0 && t.Nanosecond() == 0 {
		return t.Format("2006-01-02")
	}
	return t.Format("2006-01-02 15:04:05")
}

func quoteStringLiteral(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "''") + "'"
}
