package sqlparser

// In-process implementation of the former Python parser server (pythonsrc/parser/main.py and
// rename.py) on top of pkg/sqlengine.
// Each handler mirrors the Python function it replaces and returns the same JSON-shaped payload.

import (
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/bruin-data/bruin/pkg/sqlengine"
)

// pyErr is an error whose message mirrors str(exception) in the Python implementation.
type pyErr struct{ msg string }

func (e *pyErr) Error() string { return e.msg }

func errMsg(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

// normalizeDialect mirrors normalize_sqlglot_dialect.
func normalizeDialect(dialect string) string {
	if dialect == "vertica" {
		return "postgres"
	}
	return dialect
}

func getDialect(name string) (*sqlengine.Dialect, error) {
	return sqlengine.GetDialect(name)
}

// dispatch mirrors the command loop of pythonsrc/main.py. It returns the JSON response line.
// It is safe for concurrent use.
func dispatch(pc *parserCommand) (resp string) {
	defer func() {
		if r := recover(); r != nil {
			b, _ := json.Marshal(map[string]any{"error": fmt.Sprint(r)})
			resp = string(b)
		}
	}()
	var result any
	var err error
	c := pc.Contents
	switch pc.Command {
	case "init":
		result = map[string]any{}
	case "lineage":
		result, err = cmdLineage(c)
	case "get-tables":
		result = getTables(str(c["query"]), str(c["dialect"]))
	case "replace-table-references":
		result, err = replaceTableReferences(str(c["query"]), str(c["dialect"]), c["table_mapping"])
	case "add-limit":
		result, err = addLimit(str(c["query"]), toInt(c["limit"]), str(c["dialect"]))
	case "is-read-only":
		result = isReadOnlyQuery(str(c["query"]), str(c["dialect"]))
	case "is-single-select":
		result = isSingleSelectQuery(str(c["query"]), str(c["dialect"]))
	case "add-ctes":
		result = addCTEs(str(c["query"]), str(c["dialect"]), c["ctes"])
	case "extract-select":
		result = extractSelect(str(c["query"]), str(c["dialect"]))
	case "select-cte":
		result = selectCTE(str(c["query"]), str(c["dialect"]), str(c["cte_name"]))
	case "freeze-time":
		result = freezeTime(str(c["query"]), str(c["dialect"]), str(c["execution_time"]))
	case "hoist-declares":
		result = hoistDeclares(str(c["query"]), str(c["dialect"]))
	case "hoist-declares-list":
		result = hoistDeclaresList(toStrings(c["queries"]), str(c["dialect"]))
	default:
		err = errors.New("invalid cmd")
	}
	if err != nil {
		b, _ := json.Marshal(map[string]any{"error": err.Error()})
		return string(b)
	}
	b, merr := json.Marshal(result)
	if merr != nil {
		b, _ = json.Marshal(map[string]any{"error": merr.Error()})
	}
	return string(b)
}

func str(v any) string {
	s, _ := v.(string)
	return s
}

func toInt(v any) int {
	switch x := v.(type) {
	case int:
		return x
	case float64:
		return int(x)
	case json.Number:
		i, _ := x.Int64()
		return int(i)
	}
	return 0
}

func toStrings(v any) []string {
	switch x := v.(type) {
	case []string:
		return x
	case []any:
		out := make([]string, 0, len(x))
		for _, s := range x {
			out = append(out, str(s))
		}
		return out
	}
	return nil
}

// ---------------------------------------------------------------------------
// DECLARE hoisting
// ---------------------------------------------------------------------------

// topLevelSemicolons mirrors _top_level_semicolons: rune offsets of statement separators
// outside procedural blocks.
func topLevelSemicolons(query string, d *sqlengine.Dialect) ([]int, error) {
	tokens, err := d.TokenizeScript(query)
	if err != nil {
		return nil, err
	}
	parenDepth, beginDepth, caseDepth := 0, 0, 0
	var positions []int
	for i, tok := range tokens {
		switch {
		case tok.Type == sqlengine.TK_L_PAREN:
			parenDepth++
		case tok.Type == sqlengine.TK_R_PAREN:
			if parenDepth > 0 {
				parenDepth--
			}
		case tok.Type == sqlengine.TK_CASE:
			caseDepth++
		case (tok.Type == sqlengine.TK_BEGIN || tok.Type == sqlengine.TK_COMMAND) && firstWord(tok.Text) == "BEGIN":
			words := strings.Fields(strings.ToUpper(tok.Text))
			nextIsTransaction := len(words) > 1 && words[1] == "TRANSACTION"
			if !nextIsTransaction && i+1 < len(tokens) {
				nextIsTransaction = strings.ToUpper(tokens[i+1].Text) == "TRANSACTION"
			}
			if !nextIsTransaction {
				beginDepth++
			}
		case tok.Type == sqlengine.TK_END:
			if caseDepth > 0 {
				caseDepth--
			} else if beginDepth > 0 {
				beginDepth--
			}
		case tok.Type == sqlengine.TK_SEMICOLON && parenDepth == 0 && beginDepth == 0:
			positions = append(positions, tok.Start)
		}
	}
	return positions, nil
}

func firstWord(s string) string {
	f := strings.Fields(strings.ToUpper(s))
	if len(f) == 0 {
		// Python: "".split()[0] raises IndexError; tokens always have text here.
		return ""
	}
	return f[0]
}

// isDeclareStatement mirrors _is_declare_statement.
func isDeclareStatement(query, dialect string) bool {
	tried := map[string]bool{}
	for _, rd := range []string{dialect, "bigquery"} {
		if tried[rd] {
			continue
		}
		tried[rd] = true
		d, err := getDialect(rd)
		if err != nil {
			continue
		}
		e, err := d.ParseOne(query, nil)
		if err == nil && e.IsA(sqlengine.KDeclare) {
			return true
		}
	}
	return false
}

func pyStrip(s string) string { return strings.TrimFunc(s, isPySpace) }

func isPySpace(r rune) bool {
	switch r {
	case '\t', '\n', '\x0b', '\x0c', '\r', '\x1c', '\x1d', '\x1e', '\x1f', ' ', '\x85', '\xa0',
		' ', ' ', ' ', ' ', ' ', '　':
		return true
	}
	return r >= ' ' && r <= ' '
}

// hoistDeclares mirrors hoist_declares.
func hoistDeclares(query, dialect string) map[string]any {
	if pyStrip(query) == "" {
		return map[string]any{"query": query, "error": ""}
	}
	dialect = normalizeDialect(dialect)
	d, err := getDialect(dialect)
	if err != nil {
		return map[string]any{"query": query, "error": err.Error()}
	}
	positions, err := topLevelSemicolons(query, d)
	if err != nil {
		return map[string]any{"query": query, "error": err.Error()}
	}
	runes := []rune(query)
	var slices []string
	previous := 0
	for _, pos := range positions {
		slices = append(slices, string(runes[previous:pos]))
		previous = pos + 1
	}
	if previous < len(runes) {
		slices = append(slices, string(runes[previous:]))
	}
	var declares, rest []string
	sawNonDeclare, needsReorder := false, false
	for _, s := range slices {
		stmt := pyStrip(s)
		if stmt == "" {
			continue
		}
		if isDeclareStatement(stmt, dialect) {
			declares = append(declares, stmt)
			needsReorder = needsReorder || sawNonDeclare
		} else {
			rest = append(rest, stmt)
			sawNonDeclare = true
		}
	}
	if len(declares) == 0 || !needsReorder {
		return map[string]any{"query": query, "error": ""}
	}
	return map[string]any{"query": strings.Join(append(declares, rest...), ";\n") + ";", "error": ""}
}

// hoistDeclaresList mirrors hoist_declares_list.
func hoistDeclaresList(queries []string, dialect string) map[string]any {
	dialect = normalizeDialect(dialect)
	var declares, rest []string
	sawNonDeclare, needsReorder := false, false
	for _, q := range queries {
		if isDeclareStatement(pyStrip(q), dialect) {
			declares = append(declares, q)
			needsReorder = needsReorder || sawNonDeclare
		} else {
			rest = append(rest, q)
			sawNonDeclare = true
		}
	}
	if len(declares) == 0 || !needsReorder {
		return map[string]any{"queries": queries, "error": ""}
	}
	return map[string]any{"queries": append(declares, rest...), "error": ""}
}

// ---------------------------------------------------------------------------
// Tables
// ---------------------------------------------------------------------------

// extractTables mirrors extract_tables.
func extractTables(parsed *sqlengine.Expr) []*sqlengine.Expr {
	if parsed == nil {
		return nil
	}
	cteNames := map[string]bool{}
	if parsed.IsA(sqlengine.KCreate) {
		if ex := parsed.Expression(); ex != nil {
			for cte := range ex.FindAll(sqlengine.KCTE) {
				cteNames[cte.AliasOrName()] = true
			}
		}
	} else {
		for cte := range parsed.FindAll(sqlengine.KCTE) {
			cteNames[cte.AliasOrName()] = true
		}
	}
	var refs []*sqlengine.Expr
	for table := range parsed.FindAll(sqlengine.KTable) {
		if cteNames[table.Name()] && table.DbName() == "" && table.CatalogName() == "" {
			continue
		}
		refs = append(refs, table)
	}
	return refs
}

// tableNamePart mirrors the Anonymous-aware table name logic of get_table_name.
func tableNamePart(table *sqlengine.Expr) string {
	name := table.Name()
	if name == "" {
		this := table.This()
		if this.IsA(sqlengine.KAnonymous) {
			inner := this.Arg("this")
			if ie, ok := inner.(*sqlengine.Expr); ok && ie.IsA(sqlengine.KIdentifier) {
				name = ie.Name()
			} else if s, ok := inner.(string); ok {
				name = s
			} else if ie != nil {
				name = sqlengine.ExprString(ie)
			}
		} else if this.IsA(sqlengine.KIdentifier) {
			name = this.ThisS()
		}
	}
	return name
}

// getTableName mirrors get_table_name.
func getTableName(table *sqlengine.Expr) string {
	db := ""
	if c := table.CatalogName(); c != "" {
		db = c + "."
	}
	schema := ""
	if s := table.DbName(); s != "" {
		schema = s + "."
	}
	return db + schema + tableNamePart(table)
}

// getTableNameWithContext mirrors get_table_name_with_context.
func getTableNameWithContext(table *sqlengine.Expr, currentDatabase string) string {
	var db, schema string
	if c := table.CatalogName(); c != "" {
		db = c + "."
		if s := table.DbName(); s != "" {
			schema = s + "."
		} else {
			schema = "dbo."
		}
	} else {
		db = currentDatabase + "."
		if s := table.DbName(); s != "" {
			schema = s + "."
		} else {
			schema = "dbo."
		}
	}
	return db + schema + tableNamePart(table)
}

func sortedUnique(names []string) []string {
	set := map[string]bool{}
	out := []string{}
	for _, n := range names {
		if !set[n] {
			set[n] = true
			out = append(out, n)
		}
	}
	sort.Strings(out)
	return out
}

func getTablesTSQL(query string) map[string]any {
	d, err := getDialect("tsql")
	if err != nil {
		return map[string]any{"tables": []string{}, "error": err.Error()}
	}
	parsed, err := d.Parse(query, nil)
	if err != nil {
		return map[string]any{"tables": []string{}, "error": err.Error()}
	}
	var tables []*sqlengine.Expr
	currentDatabase := ""
	for _, stmt := range parsed {
		if stmt == nil {
			continue
		}
		if stmt.IsA(sqlengine.KUse) {
			if this := stmt.This(); this != nil {
				currentDatabase = this.Name()
			}
		} else {
			tables = append(tables, extractTables(stmt)...)
		}
	}
	var names []string
	for _, t := range tables {
		if currentDatabase != "" {
			names = append(names, getTableNameWithContext(t, currentDatabase))
		} else {
			names = append(names, getTableName(t))
		}
	}
	return map[string]any{"tables": sortedUnique(names)}
}

// getTables mirrors get_tables.
func getTables(query, dialect string) map[string]any {
	dialect = normalizeDialect(dialect)
	if dialect == "tsql" {
		return getTablesTSQL(query)
	}
	d, err := getDialect(dialect)
	if err != nil {
		return map[string]any{"tables": []string{}, "error": err.Error()}
	}
	parsed, err := d.Parse(query, nil)
	if err != nil {
		return map[string]any{"tables": []string{}, "error": err.Error()}
	}
	var names []string
	for _, stmt := range parsed {
		if stmt == nil {
			continue
		}
		for _, t := range extractTables(stmt) {
			names = append(names, getTableName(t))
		}
	}
	return map[string]any{"tables": sortedUnique(names)}
}

// mergeParts mirrors merge_parts.
func mergeParts(table *sqlengine.Expr) string {
	var parts []string
	for _, part := range table.Parts() {
		if part.IsA(sqlengine.KIdentifier) {
			parts = append(parts, part.Name())
		} else if part.IsA(sqlengine.KAnonymous) {
			inner := part.Arg("this")
			if ie, ok := inner.(*sqlengine.Expr); ok && ie.IsA(sqlengine.KIdentifier) {
				parts = append(parts, ie.Name())
			} else if s, ok := inner.(string); ok {
				parts = append(parts, s)
			} else if ie != nil {
				parts = append(parts, sqlengine.ExprString(ie))
			}
		}
	}
	return strings.Join(parts, ".")
}
