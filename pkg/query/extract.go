package query

import (
	"context"
	"regexp"
	"strings"

	"github.com/bruin-data/bruin/pkg/jinja"
	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/pkg/errors"
	"github.com/spf13/afero"
)

type Query struct {
	VariableDefinitions []string
	Query               string
	Args                []any
}

type QueryResult struct {
	Columns     []string
	Rows        [][]interface{}
	ColumnTypes []string
	Execution   *QueryExecutionSummary
}

type QueryExtractor interface {
	ExtractQueriesFromString(filepath string) ([]*Query, error)
	CloneForAsset(ctx context.Context, pipeline *pipeline.Pipeline, asset *pipeline.Asset) (QueryExtractor, error)
	ReextractQueriesFromSlice(content []string) ([]string, error)
}

func (q Query) ToExplainQuery() string {
	// take only the first 10 characters since the actual queries may be pretty large
	lowerQuery := strings.ToLower(q.Query[:min(10, len(q.Query))])
	if strings.HasPrefix(lowerQuery, "explain ") {
		return ensureSemicolon(q.Query)
	}

	if strings.HasPrefix(lowerQuery, "use ") {
		return ensureSemicolon(q.Query)
	}

	eq := ""
	if len(q.VariableDefinitions) > 0 {
		eq += strings.Join(q.VariableDefinitions, ";\n") + ";\n"
	}

	eq += "EXPLAIN " + q.Query
	eq = ensureSemicolon(eq)

	return eq
}

func ensureSemicolon(query string) string {
	if !strings.HasSuffix(query, ";") {
		return query + ";"
	}

	return query
}

func (q Query) ToDryRunQuery() string {
	eq := ""
	if len(q.VariableDefinitions) > 0 {
		eq += strings.Join(q.VariableDefinitions, ";\n") + ";\n"
	}

	eq += q.Query
	if !strings.HasSuffix(eq, ";") {
		eq += ";"
	}

	return eq
}

func (q Query) String() string {
	return q.Query
}

var (
	queryCommentRegex        = regexp.MustCompile(`(?m)(?s)\/\*.*?\*\/|(^|\s)--.*?\n`)
	oraclePLSQLDDLStartRegex = regexp.MustCompile(`(?is)^CREATE\s+(?:OR\s+REPLACE\s+)?(?:(?:NON)?EDITIONABLE\s+)?(?:PROCEDURE|FUNCTION|PACKAGE(?:\s+BODY)?|TRIGGER|TYPE\s+BODY)\b`)
	oraclePLSQLDDLBodyRegex  = regexp.MustCompile(`(?is)^CREATE\s+(?:OR\s+REPLACE\s+)?(?:(?:NON)?EDITIONABLE\s+)?(?:PACKAGE(?:\s+BODY)?|TYPE\s+BODY)\b`)
	oraclePLSQLDDLNameRegex  = regexp.MustCompile(`(?is)^CREATE\s+(?:OR\s+REPLACE\s+)?(?:(?:NON)?EDITIONABLE\s+)?(?:PACKAGE(?:\s+BODY)?|TYPE\s+BODY)\s+([A-Z0-9_$#".]+)\b`)
	oraclePLSQLBeginRegex    = regexp.MustCompile(`\bBEGIN\b`)
	oraclePLSQLEndRegex      = regexp.MustCompile(`\bEND(?:\s+([A-Z0-9_$#".]+))?\b`)
	oraclePLSQLBlockEndRegex = regexp.MustCompile(`(?s)\bEND(?:\s+([A-Z0-9_$#".]+))?\s*$`)
)

// FileQuerySplitterExtractor is a regular file extractor, but it splits the queries in the given file into multiple
// instances. For usecases that require EXPLAIN statements, such as validating Snowflake queries, it is not possible
// to use a single query with multiple statements, so we need to split them into multiple queries.
type FileQuerySplitterExtractor struct {
	Fs       afero.Fs
	Renderer jinja.RendererInterface
}

func (f FileQuerySplitterExtractor) ExtractQueriesFromString(content string) ([]*Query, error) {
	cleanedUpQueries := queryCommentRegex.ReplaceAllLiteralString(content, "\n")
	cleanedUpQueries, err := f.Renderer.Render(cleanedUpQueries)
	if err != nil {
		return nil, errors.Wrap(err, "could not render file while extracting the queries with the split query extractor")
	}

	return splitQueries(cleanedUpQueries), nil
}

func splitQueries(fileContent string) []*Query {
	queries := make([]*Query, 0)
	var sqlVariablesSeenSoFar []string

	for _, query := range strings.Split(fileContent, ";") {
		query = strings.TrimSpace(query)
		if len(query) == 0 {
			continue
		}

		queryLines := strings.Split(query, "\n")
		cleanQueryRows := make([]string, 0, len(queryLines))
		for _, line := range queryLines {
			emptyLine := strings.TrimSpace(line)
			if len(emptyLine) == 0 {
				continue
			}

			cleanQueryRows = append(cleanQueryRows, line)
		}

		cleanQuery := strings.TrimSpace(strings.Join(cleanQueryRows, "\n"))
		lowerCaseVersion := strings.ToLower(cleanQuery)
		if strings.HasPrefix(lowerCaseVersion, "set") || strings.HasPrefix(lowerCaseVersion, "declare") {
			sqlVariablesSeenSoFar = append(sqlVariablesSeenSoFar, cleanQuery)
			continue
		}

		if strings.HasPrefix(lowerCaseVersion, "use") {
			continue
		}

		queries = append(queries, &Query{
			VariableDefinitions: sqlVariablesSeenSoFar,
			Query:               strings.TrimSpace(cleanQuery),
		})
	}

	return queries
}

func (f FileQuerySplitterExtractor) CloneForAsset(ctx context.Context, p *pipeline.Pipeline, t *pipeline.Asset) (QueryExtractor, error) {
	renderer, err := f.Renderer.CloneForAsset(ctx, p, t)
	if err != nil {
		return nil, err
	}

	return &FileQuerySplitterExtractor{
		Renderer: renderer,
		Fs:       f.Fs,
	}, nil
}

func (f FileQuerySplitterExtractor) ReextractQueriesFromSlice(content []string) ([]string, error) {
	return nil, errors.New("not implemented")
}

// OracleScriptExtractor splits Oracle SQL scripts into executable statements.
// Oracle drivers execute one SQL or PL/SQL unit per call, unlike SQL*Plus-style
// clients that understand semicolon-delimited scripts and standalone slash
// block terminators.
type OracleScriptExtractor struct {
	Fs       afero.Fs
	Renderer jinja.RendererInterface
}

func (f OracleScriptExtractor) ExtractQueriesFromString(content string) ([]*Query, error) {
	rendered, err := f.Renderer.Render(strings.TrimSpace(content))
	if err != nil {
		return nil, errors.Wrap(err, "could not render file while extracting Oracle script queries")
	}

	return splitOracleQueries(rendered), nil
}

func (f OracleScriptExtractor) CloneForAsset(ctx context.Context, p *pipeline.Pipeline, t *pipeline.Asset) (QueryExtractor, error) {
	renderer, err := f.Renderer.CloneForAsset(ctx, p, t)
	if err != nil {
		return nil, err
	}

	return &OracleScriptExtractor{
		Renderer: renderer,
		Fs:       f.Fs,
	}, nil
}

func (f OracleScriptExtractor) ReextractQueriesFromSlice(content []string) ([]string, error) {
	allQueries := make([]string, 0, len(content))
	for _, query := range content {
		rendered, err := f.Renderer.Render(strings.TrimSpace(query))
		if err != nil {
			return nil, errors.Wrap(err, "could not render file while re-extracting Oracle script queries")
		}
		for _, extracted := range splitOracleQueries(rendered) {
			allQueries = append(allQueries, extracted.Query)
		}
	}

	return allQueries, nil
}

func splitOracleQueries(fileContent string) []*Query {
	statements := splitOracleStatements(fileContent)
	queries := make([]*Query, 0, len(statements))
	for _, statement := range statements {
		statement = strings.TrimSpace(removeStandaloneOracleSlash(statement))
		if statement == "" || strings.TrimSpace(queryCommentRegex.ReplaceAllLiteralString(statement, "\n")) == "" {
			continue
		}
		queries = append(queries, &Query{Query: statement})
	}

	return queries
}

func splitOracleStatements(fileContent string) []string {
	statements := make([]string, 0)
	var current strings.Builder
	inSingleQuote := false
	inDoubleQuote := false
	inLineComment := false
	inBlockComment := false

	for i := 0; i < len(fileContent); i++ {
		ch := fileContent[i]
		var next byte
		if i+1 < len(fileContent) {
			next = fileContent[i+1]
		}

		switch {
		case inLineComment:
			current.WriteByte(ch)
			if ch == '\n' || ch == '\r' {
				inLineComment = false
			}
			continue
		case inBlockComment:
			current.WriteByte(ch)
			if ch == '*' && next == '/' {
				current.WriteByte(next)
				i++
				inBlockComment = false
			}
			continue
		case inSingleQuote:
			current.WriteByte(ch)
			if ch == '\'' {
				if next == '\'' {
					current.WriteByte(next)
					i++
					continue
				}
				inSingleQuote = false
			}
			continue
		case inDoubleQuote:
			current.WriteByte(ch)
			if ch == '"' {
				if next == '"' {
					current.WriteByte(next)
					i++
					continue
				}
				inDoubleQuote = false
			}
			continue
		}

		switch {
		case ch == '-' && next == '-':
			current.WriteByte(ch)
			current.WriteByte(next)
			i++
			inLineComment = true
			continue
		case ch == '/' && next == '*':
			current.WriteByte(ch)
			current.WriteByte(next)
			i++
			inBlockComment = true
			continue
		case ch == '\'':
			current.WriteByte(ch)
			inSingleQuote = true
			continue
		case ch == '"':
			current.WriteByte(ch)
			inDoubleQuote = true
			continue
		case ch == ';':
			statementBeforeTerminator := strings.TrimSpace(current.String())
			if oracleStatementStartsPLSQLBlock(statementBeforeTerminator) && !oraclePLSQLBlockComplete(statementBeforeTerminator) {
				current.WriteByte(ch)
				continue
			}
			current.WriteByte(ch)
			statements = append(statements, current.String())
			current.Reset()
			continue
		}

		current.WriteByte(ch)
	}

	if strings.TrimSpace(current.String()) != "" {
		statements = append(statements, current.String())
	}

	return statements
}

func oracleStatementStartsPLSQLBlock(statement string) bool {
	normalized := oraclePLSQLComparableText(statement)
	return oracleHasAnonymousPLSQLPrefix(normalized, "BEGIN") || oracleHasAnonymousPLSQLPrefix(normalized, "DECLARE") || oraclePLSQLDDLStartRegex.MatchString(normalized)
}

func oraclePLSQLBlockComplete(statement string) bool {
	normalized := oraclePLSQLComparableText(statement)
	normalized = strings.TrimSuffix(normalized, ";")
	matches := oraclePLSQLBlockEndRegex.FindStringSubmatch(normalized)
	if len(matches) == 0 {
		return false
	}
	if len(matches) > 1 && oracleEndLabelIsControlKeyword(matches[1]) {
		return false
	}
	ddlBodyName := oraclePLSQLDDLBodyName(normalized)

	depth := 0
	if ddlBodyName != "" || oraclePLSQLDDLBodyRegex.MatchString(normalized) {
		depth++
	}
	depth += len(oraclePLSQLBeginRegex.FindAllString(normalized, -1))
	for _, match := range oraclePLSQLEndRegex.FindAllStringSubmatch(normalized, -1) {
		if len(match) > 1 && oracleEndLabelIsControlKeyword(match[1]) {
			continue
		}
		depth--
	}

	if depth <= 0 {
		return true
	}
	if depth == 1 && ddlBodyName != "" && len(matches) > 1 {
		return oracleEndLabelMatchesObjectName(matches[1], ddlBodyName)
	}
	return false
}

func oraclePLSQLComparableText(statement string) string {
	withoutComments := queryCommentRegex.ReplaceAllLiteralString(statement, "\n")
	withoutStrings := oracleMaskSingleQuotedStrings(withoutComments)
	return strings.ToUpper(strings.TrimSpace(withoutStrings))
}

func oracleHasAnonymousPLSQLPrefix(statement, prefix string) bool {
	if !strings.HasPrefix(statement, prefix) {
		return false
	}
	if len(statement) == len(prefix) {
		return true
	}
	switch statement[len(prefix)] {
	case ' ', '\n', '\r', '\t':
		return true
	default:
		return false
	}
}

func oracleMaskSingleQuotedStrings(statement string) string {
	var masked strings.Builder
	masked.Grow(len(statement))

	inString := false
	for i := 0; i < len(statement); i++ {
		ch := statement[i]
		if !inString {
			if ch == '\'' {
				inString = true
				masked.WriteByte(' ')
				continue
			}
			masked.WriteByte(ch)
			continue
		}

		switch {
		case ch == '\'' && i+1 < len(statement) && statement[i+1] == '\'':
			masked.WriteString("  ")
			i++
		case ch == '\'':
			inString = false
			masked.WriteByte(' ')
		case ch == '\n' || ch == '\r':
			masked.WriteByte(ch)
		default:
			masked.WriteByte(' ')
		}
	}

	return masked.String()
}

func oraclePLSQLDDLBodyName(statement string) string {
	match := oraclePLSQLDDLNameRegex.FindStringSubmatch(statement)
	if len(match) < 2 {
		return ""
	}
	return match[1]
}

func oracleEndLabelMatchesObjectName(label, objectName string) bool {
	label = strings.Trim(label, `"`)
	if label == "" {
		return false
	}
	nameParts := strings.Split(objectName, ".")
	objectName = strings.Trim(nameParts[len(nameParts)-1], `"`)
	return label == objectName
}

func oracleEndLabelIsControlKeyword(label string) bool {
	switch label {
	case "IF", "LOOP", "CASE":
		return true
	default:
		return false
	}
}

func removeStandaloneOracleSlash(statement string) string {
	lines := strings.Split(strings.TrimSpace(statement), "\n")
	filtered := make([]string, 0, len(lines))
	for _, line := range lines {
		if strings.TrimSpace(line) == "/" {
			continue
		}
		filtered = append(filtered, line)
	}
	return strings.Join(filtered, "\n")
}

// WholeFileExtractor is a regular file extractor that returns the whole file content as the query string. It is useful
// for cases where the whole file content can be treated as a single query, such as validating GoogleCloudPlatform queries via dry-run.
type WholeFileExtractor struct {
	Fs       afero.Fs
	Renderer jinja.RendererInterface
}

func (f *WholeFileExtractor) ExtractQueriesFromString(content string) ([]*Query, error) {
	render, err := f.Renderer.Render(strings.TrimSpace(content))
	if err != nil {
		return nil, errors.Wrap(err, "could not render file while extracting the queries from the whole file")
	}

	return []*Query{
		{
			Query: render,
		},
	}, nil
}

func (f *WholeFileExtractor) ReextractQueriesFromSlice(content []string) ([]string, error) {
	allQueries := make([]string, 0, len(content))
	for _, query := range content {
		q, err := f.Renderer.Render(strings.TrimSpace(query))
		if err != nil {
			return nil, errors.Wrap(err, "could not render file while re-extracting the queries from the whole file")
		}
		allQueries = append(allQueries, q)
	}

	return allQueries, nil
}

func (f *WholeFileExtractor) CloneForAsset(ctx context.Context, p *pipeline.Pipeline, t *pipeline.Asset) (QueryExtractor, error) {
	renderer, err := f.Renderer.CloneForAsset(ctx, p, t)
	if err != nil {
		return nil, err
	}

	return &WholeFileExtractor{
		Renderer: renderer,
		Fs:       f.Fs,
	}, nil
}
