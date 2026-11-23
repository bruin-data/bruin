package schemaevolution

import (
	"strings"

	"github.com/bruin-data/bruin/pkg/sqlengine"
)

func dialectOrDefault(dialectName string) *sqlengine.Dialect {
	if d, err := sqlengine.GetDialect(dialectName); err == nil && d != nil {
		return d
	}
	return sqlengine.MustDialect("")
}

// IsConstantLiteralExpr reports whether expr is empty or a single SQL constant literal
// (NULL, TRUE, FALSE, a single string literal, or an optionally signed numeric literal)
// using pkg/sqlengine's dialect tokenizer.
func IsConstantLiteralExpr(dialectName, expr string) bool {
	trimmed := strings.TrimSpace(expr)
	if trimmed == "" {
		return true
	}

	tokens, err := dialectOrDefault(dialectName).Tokenize(trimmed)
	if err != nil || len(tokens) == 0 {
		return false
	}

	switch len(tokens) {
	case 1:
		switch tokens[0].Type { //nolint:exhaustive
		case sqlengine.TK_NULL, sqlengine.TK_TRUE, sqlengine.TK_FALSE, sqlengine.TK_NUMBER, sqlengine.TK_STRING:
			return true
		default:
			return false
		}
	case 2:
		if (tokens[0].Type == sqlengine.TK_PLUS || tokens[0].Type == sqlengine.TK_DASH) &&
			tokens[1].Type == sqlengine.TK_NUMBER &&
			tokens[0].End+1 == tokens[1].Start {
			return true
		}
		return isAdjacentLeadingDotDecimal(tokens[0], tokens[1])
	case 3:
		return (tokens[0].Type == sqlengine.TK_PLUS || tokens[0].Type == sqlengine.TK_DASH) &&
			tokens[0].End+1 == tokens[1].Start &&
			isAdjacentLeadingDotDecimal(tokens[1], tokens[2])
	default:
		return false
	}
}

func isAdjacentLeadingDotDecimal(dotTok, numTok *sqlengine.Token) bool {
	return dotTok.Type == sqlengine.TK_DOT &&
		numTok.Type == sqlengine.TK_NUMBER &&
		dotTok.End+1 == numTok.Start &&
		!strings.Contains(numTok.Text, ".")
}

// IsBareValuesQuery reports whether queryStr starts with a bare `VALUES ...` clause
// (ignoring leading SQL comments and parentheses) using pkg/sqlengine's tokenizer.
func IsBareValuesQuery(dialectName, queryStr string) bool {
	tokens, err := dialectOrDefault(dialectName).Tokenize(queryStr)
	if err != nil {
		return false
	}
	for _, tok := range tokens {
		if tok.Type == sqlengine.TK_L_PAREN {
			continue
		}
		return tok.Type == sqlengine.TK_VALUES
	}
	return false
}

// StripTrailingSemicolonsAndComments removes trailing SQL comments and any trailing
// statement-terminating semicolons using pkg/sqlengine token offsets so queryStr can
// safely be embedded as a subquery or terminated with `;`.
func StripTrailingSemicolonsAndComments(dialectName, queryStr string) string {
	trimmed := strings.TrimSpace(queryStr)
	if trimmed == "" {
		return ""
	}
	tokens, err := dialectOrDefault(dialectName).Tokenize(trimmed)
	if err != nil || len(tokens) == 0 {
		return strings.TrimSpace(strings.TrimRight(trimmed, ";"))
	}
	i := len(tokens) - 1
	for i >= 0 && tokens[i].Type == sqlengine.TK_SEMICOLON {
		i--
	}
	if i < 0 {
		return ""
	}
	runes := []rune(trimmed)
	end := int(tokens[i].End) + 1
	if end >= 0 && end <= len(runes) {
		return strings.TrimSpace(string(runes[:end]))
	}
	return trimmed
}

func findLeadingBeginTransactionEnd(dialectName, sqlStr string) (int, bool) {
	tokens, err := dialectOrDefault(dialectName).Tokenize(sqlStr)
	if err != nil || len(tokens) < 2 {
		return 0, false
	}

	first := tokens[0]
	if first.Type != sqlengine.TK_BEGIN && !strings.EqualFold(first.Text, "START") {
		return 0, false
	}

	idx := 1
	if idx < len(tokens) && (strings.EqualFold(tokens[idx].Text, "TRANSACTION") || strings.EqualFold(tokens[idx].Text, "WORK")) {
		idx++
	}
	if idx >= len(tokens) || tokens[idx].Type != sqlengine.TK_SEMICOLON {
		return 0, false
	}

	runes := []rune(sqlStr)
	endRune := int(tokens[idx].End) + 1
	if endRune <= 0 || endRune > len(runes) {
		return 0, false
	}
	return len(string(runes[:endRune])), true
}

// SpliceStatementsInTransaction injects stmts into mainSQL so schema evolution DDL
// and the write query execute atomically inside one SQL transaction.
func SpliceStatementsInTransaction(dialectName, mainSQL string, stmts []string) string {
	if len(stmts) == 0 {
		return mainSQL
	}

	alterBlock := strings.Join(stmts, ";\n") + ";"
	trimmed := strings.TrimSpace(mainSQL)

	if endByte, ok := findLeadingBeginTransactionEnd(dialectName, trimmed); ok {
		prefix := trimmed[:endByte]
		rest := strings.TrimLeft(trimmed[endByte:], " \t\r\n")
		return prefix + "\n" + alterBlock + "\n" + rest
	}

	body := StripTrailingSemicolonsAndComments(dialectName, trimmed)
	return "BEGIN TRANSACTION;\n" + alterBlock + "\n" + body + ";\nCOMMIT;"
}

// SplitTableIdentifier parses a 1-, 2-, or 3-part SQL table reference using pkg/sqlengine's
// dialect tokenizer, unquoting quoted identifiers while preserving dots, spaces, and escaped
// quotes inside quoted parts.
func SplitTableIdentifier(dialectName, tableName, defaultSchema string) (catalog, schema, table string, partCount int) {
	trimmed := strings.TrimSpace(tableName)
	if trimmed == "" {
		return "", defaultSchema, "", 0
	}

	if parts, ok := tokenizeTableParts(dialectName, trimmed); ok && len(parts) > 0 {
		switch len(parts) {
		case 1:
			return "", defaultSchema, parts[0], 1
		case 2:
			return "", parts[0], parts[1], 2
		default:
			return parts[0], parts[1], parts[2], len(parts)
		}
	}

	rawParts := strings.Split(trimmed, ".")
	switch len(rawParts) {
	case 1:
		return "", defaultSchema, strings.TrimSpace(rawParts[0]), 1
	case 2:
		return "", strings.TrimSpace(rawParts[0]), strings.TrimSpace(rawParts[1]), 2
	default:
		return strings.TrimSpace(rawParts[0]), strings.TrimSpace(rawParts[1]), strings.TrimSpace(rawParts[2]), len(rawParts)
	}
}

func tokenizeTableParts(dialectName, tableName string) ([]string, bool) {
	tokens, err := dialectOrDefault(dialectName).Tokenize(tableName)
	if err != nil || len(tokens) == 0 {
		return nil, false
	}

	parts, nextIdx, ok := collectDotSeparatedParts(dialectName, tokens, 0)
	if !ok || nextIdx != len(tokens) {
		return nil, false
	}
	return parts, true
}

func collectDotSeparatedParts(dialectName string, tokens []*sqlengine.Token, start int) ([]string, int, bool) {
	if start >= len(tokens) || isNonIdentifierPunctuation(tokens[start].Type) {
		return nil, start, false
	}
	var parts []string
	idx := start
	for {
		if idx >= len(tokens) || isNonIdentifierPunctuation(tokens[idx].Type) {
			return nil, idx, false
		}
		part := tokens[idx].Text
		tokStart := int(tokens[idx].Start)
		lastEnd := int(tokens[idx].End)
		idx++

		// Unquoted BigQuery table segments may contain hyphens (`my-proj`, `proj-1.`),
		// which tokenize as adjacent tokens connected by `-`.
		if lastEnd-tokStart+1 == len([]rune(part)) {
			for idx < len(tokens) && tokens[idx].Type == sqlengine.TK_DASH && int(tokens[idx].Start) == lastEnd+1 {
				for idx < len(tokens) &&
					tokens[idx].Type != sqlengine.TK_DOT &&
					!isNonIdentifierPunctuation(tokens[idx].Type) &&
					!strings.HasSuffix(part, ".") &&
					int(tokens[idx].Start) == lastEnd+1 {
					part += tokens[idx].Text
					lastEnd = int(tokens[idx].End)
					idx++
				}
			}
		}

		if strings.HasSuffix(part, ".") && len(part) > 1 {
			parts = append(parts, strings.TrimSuffix(part, "."))
			continue
		}
		parts = append(parts, part)
		if idx < len(tokens) && tokens[idx].Type == sqlengine.TK_DOT {
			idx++
			continue
		}
		break
	}

	if len(parts) == 1 && strings.EqualFold(dialectName, "bigquery") && strings.Contains(parts[0], ".") {
		parts = strings.Split(parts[0], ".")
	}
	return parts, idx, len(parts) > 0
}

func isNonIdentifierPunctuation(tt sqlengine.TokenType) bool {
	switch tt { //nolint:exhaustive
	case sqlengine.TK_DOT,
		sqlengine.TK_COMMA,
		sqlengine.TK_SEMICOLON,
		sqlengine.TK_L_PAREN,
		sqlengine.TK_R_PAREN,
		sqlengine.TK_L_BRACKET,
		sqlengine.TK_R_BRACKET,
		sqlengine.TK_L_BRACE,
		sqlengine.TK_R_BRACE,
		sqlengine.TK_STRING:
		return true
	default:
		return false
	}
}

// QuoteDoubleQuotedIdentifier quotes a single SQL identifier (such as a column name)
// in ANSI double quotes without splitting on dots, escaping embedded double quotes.
func QuoteDoubleQuotedIdentifier(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}

// QuoteBacktickIdentifier quotes a single SQL identifier (such as a BigQuery column name)
// in backticks without splitting on dots, escaping embedded backticks.
func QuoteBacktickIdentifier(name string) string {
	return "`" + strings.ReplaceAll(name, "`", "``") + "`"
}

// QuoteQualifiedTableName splits a 1-, 2-, or 3-part SQL table reference using dialectName's
// tokenizer and quotes each individual segment with quoteIdent.
func QuoteQualifiedTableName(dialectName, tableName string, quoteIdent func(string) string) string {
	cat, sch, tbl, n := SplitTableIdentifier(dialectName, tableName, "")
	switch n {
	case 1:
		return quoteIdent(tbl)
	case 2:
		return quoteIdent(sch) + "." + quoteIdent(tbl)
	default:
		if cat != "" {
			return quoteIdent(cat) + "." + quoteIdent(sch) + "." + quoteIdent(tbl)
		}
		return tableName
	}
}

// SelectLiteralHints summarizes pure-Go AST inspection of top-level SELECT projections.
type SelectLiteralHints struct {
	UntypedNulls    map[string]bool
	EmptyArrays     map[string]int
	ConcreteScalars map[string]bool
}

// InspectSelectLiterals inspects the top-level SELECT projection list of queryStr using
// pkg/sqlengine and identifies bare untyped NULL projections, bare untyped empty array
// literals (`[]`, `[[]]`), and guaranteed non-NULL concrete scalar literals/CASTs.
func InspectSelectLiterals(dialectName, queryStr string) SelectLiteralHints {
	var hints SelectLiteralHints
	cleaned := StripTrailingSemicolonsAndComments(dialectName, queryStr)
	if cleaned == "" {
		return hints
	}
	expr, ok := safeParseQueryExpr(dialectName, cleaned)
	if !ok || expr == nil {
		return hints
	}
	sel := expr.Unnest()
	if sel == nil || !sel.IsA(sqlengine.KSelect) {
		return hints
	}

	for _, proj := range sel.Expressions() {
		if proj == nil {
			continue
		}
		colKey := strings.ToLower(strings.TrimSpace(proj.AliasOrName()))
		if colKey == "" {
			continue
		}
		inner := proj.Unalias().Unnest()
		if inner == nil {
			continue
		}
		if inner.IsA(sqlengine.KNull) {
			if hints.UntypedNulls == nil {
				hints.UntypedNulls = make(map[string]bool)
			}
			hints.UntypedNulls[colKey] = true
			continue
		}
		if depth, isEmptyArr := untypedEmptyArrayLiteralDepth(inner); isEmptyArr {
			if hints.EmptyArrays == nil {
				hints.EmptyArrays = make(map[string]int)
			}
			hints.EmptyArrays[colKey] = depth
			continue
		}
		if inner.IsA(sqlengine.KLiteral) || inner.IsA(sqlengine.KBoolean) {
			if hints.ConcreteScalars == nil {
				hints.ConcreteScalars = make(map[string]bool)
			}
			hints.ConcreteScalars[colKey] = true
		}
	}
	return hints
}

// DetectUntypedSelectLiterals inspects the top-level SELECT projection list of queryStr using
// pkg/sqlengine and returns lowercased aliases of bare untyped NULL projections and the nesting
// depth of bare untyped empty array literals (`[]`, `[[]]`).
func DetectUntypedSelectLiterals(dialectName, queryStr string) (map[string]bool, map[string]int) {
	hints := InspectSelectLiterals(dialectName, queryStr)
	return hints.UntypedNulls, hints.EmptyArrays
}

// ApplyUntypedSelectLiterals enriches cols in place with pure-Go AST detection of bare untyped
// NULL projections (`UntypedNull = true`) and bare empty array literals (`[]`, `[[]]`), formatting
// untyped array placeholders via cfg (e.g. `ARRAY<UNKNOWN>` in BigQuery, `SQLNULL[]` in DuckDB).
// It returns the inspected SelectLiteralHints so engines with runtime probes can skip already-resolved columns.
func ApplyUntypedSelectLiterals(cfg TypeFormatConfig, queryStr string, cols []Column) SelectLiteralHints {
	hints := InspectSelectLiterals(cfg.SQLDialect, queryStr)
	if len(hints.UntypedNulls) == 0 && len(hints.EmptyArrays) == 0 {
		return hints
	}
	for i := range cols {
		key := strings.ToLower(strings.TrimSpace(cols[i].Name))
		if hints.UntypedNulls[key] {
			cols[i].UntypedNull = true
			continue
		}
		if depth, ok := hints.EmptyArrays[key]; ok && depth > 0 {
			pt := ParsedType{DType: sqlengine.DT_NULL}
			for range depth {
				elem := pt
				pt = ParsedType{DType: sqlengine.DT_ARRAY, Elem: &elem}
			}
			cols[i].Type = FormatType(cfg, pt)
		}
	}
	return hints
}

func safeParseQueryExpr(dialectName, sqlStr string) (out *sqlengine.Expr, ok bool) {
	defer func() {
		if recover() != nil {
			out = nil
			ok = false
		}
	}()
	expr, err := dialectOrDefault(dialectName).ParseOne(sqlStr, nil)
	if err != nil || expr == nil {
		return nil, false
	}
	return expr, true
}

func untypedEmptyArrayLiteralDepth(expr *sqlengine.Expr) (int, bool) {
	if expr == nil || !expr.IsA(sqlengine.KArray) {
		return 0, false
	}
	elems := expr.Expressions()
	if len(elems) == 0 {
		return 1, true
	}
	if len(elems) == 1 && elems[0] != nil {
		if innerDepth, ok := untypedEmptyArrayLiteralDepth(elems[0].Unnest()); ok {
			return innerDepth + 1, true
		}
	}
	return 0, false
}

// InjectInsertColumnList rewrites positional `INSERT INTO <tableName> ...` statements in sqlStr
// (for engines without DuckDB's `INSERT BY NAME`) so they explicitly target cols by name,
// allowing target tables to retain omitted historical columns or differing column orders.
func InjectInsertColumnList(dialectName, sqlStr, tableName string, quoteIdent func(string) string, cols []Column) string {
	if len(cols) == 0 || strings.TrimSpace(sqlStr) == "" {
		return sqlStr
	}

	wantCat, wantSch, wantTbl, _ := SplitTableIdentifier(dialectName, tableName, "")
	if wantTbl == "" {
		return sqlStr
	}
	var wantParts []string
	for _, p := range []string{wantCat, wantSch, wantTbl} {
		if p != "" {
			wantParts = append(wantParts, p)
		}
	}

	tokens, err := dialectOrDefault(dialectName).Tokenize(sqlStr)
	if err != nil || len(tokens) < 4 {
		return sqlStr
	}

	quoted := make([]string, 0, len(cols))
	for _, col := range cols {
		name := strings.TrimSpace(col.Name)
		if name != "" {
			quoted = append(quoted, quoteIdent(name))
		}
	}
	if len(quoted) == 0 {
		return sqlStr
	}
	colList := strings.Join(quoted, ", ")

	type edit struct {
		start int
		end   int
		text  string
	}
	var edits []edit

	for i := 0; i+2 < len(tokens); i++ {
		if tokens[i].Type != sqlengine.TK_INSERT || tokens[i+1].Type != sqlengine.TK_INTO {
			continue
		}
		parts, nextIdx, ok := collectDotSeparatedParts(dialectName, tokens, i+2)
		if !ok || len(parts) != len(wantParts) || nextIdx >= len(tokens) {
			continue
		}
		matched := true
		for k := range wantParts {
			if !strings.EqualFold(parts[k], wantParts[k]) {
				matched = false
				break
			}
		}
		if !matched {
			continue
		}

		tableEnd := int(tokens[nextIdx-1].End) + 1
		nextTok := tokens[nextIdx]

		// Already has an explicit column list `INSERT INTO tbl (c1, c2) ...`.
		if nextTok.Type == sqlengine.TK_L_PAREN {
			if nextIdx+1 >= len(tokens) ||
				(tokens[nextIdx+1].Type != sqlengine.TK_SELECT && tokens[nextIdx+1].Type != sqlengine.TK_WITH) {
				continue
			}
		}

		// `INSERT INTO tbl SELECT * FROM __bruin_tmp_...` -> `INSERT INTO tbl (c1, c2) SELECT c1, c2 FROM ...`
		if nextTok.Type == sqlengine.TK_SELECT &&
			nextIdx+2 < len(tokens) &&
			tokens[nextIdx+1].Type == sqlengine.TK_STAR &&
			tokens[nextIdx+2].Type == sqlengine.TK_FROM {
			starEnd := int(tokens[nextIdx+1].End) + 1
			edits = append(edits, edit{
				start: tableEnd,
				end:   starEnd,
				text:  " (" + colList + ") SELECT " + colList,
			})
			i = nextIdx + 1
			continue
		}

		edits = append(edits, edit{
			start: tableEnd,
			end:   tableEnd,
			text:  " (" + colList + ")",
		})
		i = nextIdx - 1
	}

	if len(edits) == 0 {
		return sqlStr
	}

	runes := []rune(sqlStr)
	var b strings.Builder
	cursor := 0
	for _, e := range edits {
		if e.start < cursor || e.end > len(runes) {
			continue
		}
		b.WriteString(string(runes[cursor:e.start]))
		b.WriteString(e.text)
		cursor = e.end
	}
	b.WriteString(string(runes[cursor:]))
	return b.String()
}
