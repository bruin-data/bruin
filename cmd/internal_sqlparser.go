package cmd

import (
	"context"
	"encoding/json"
	"os"
	"sort"
	"strings"
	"unicode"

	"github.com/bruin-data/bruin/pkg/sqlparser"
	errors2 "github.com/pkg/errors"
	"github.com/urfave/cli/v3"
)

type internalSQLParseResult struct {
	Columns []string `json:"columns"`
	Tables  []string `json:"tables"`
}

func InternalSQLGlot() *cli.Command {
	return internalSQLParserCommand("sqlglot", func() (sqlparser.Parser, error) {
		return sqlparser.NewSQLParser(true)
	})
}

func InternalPolyglot() *cli.Command {
	return internalSQLParserCommand("polyglot", func() (sqlparser.Parser, error) {
		return sqlparser.NewRustSQLParser(true)
	})
}

func internalSQLParserCommand(
	name string,
	newParser func() (sqlparser.Parser, error),
) *cli.Command {
	return &cli.Command{
		Name:      name,
		Usage:     "extract output columns and referenced tables from a SQL file",
		ArgsUsage: "[path to a SQL file]",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:  "dialect",
				Usage: "SQL dialect to parse",
				Value: "bigquery",
			},
		},
		Action: func(ctx context.Context, c *cli.Command) error {
			filePath := c.Args().Get(0)
			if filePath == "" {
				printErrorJSON(errors2.New("sql file path is required"))
				return cli.Exit("", 1)
			}

			queryBytes, err := os.ReadFile(filePath)
			if err != nil {
				printErrorJSON(errors2.Wrap(err, "failed to read sql file"))
				return cli.Exit("", 1)
			}

			parser, err := newParser()
			if err != nil {
				printErrorJSON(err)
				return cli.Exit("", 1)
			}
			defer func() {
				_ = parser.Close()
			}()

			if err := parser.Start(); err != nil {
				printErrorJSON(err)
				return cli.Exit("", 1)
			}

			dialect := c.String("dialect")
			normalizedQuery, err := parser.RenameTables(string(queryBytes), dialect, map[string]string{})
			if err != nil {
				printErrorJSON(errors2.Wrap(err, "failed to normalize sql query"))
				return cli.Exit("", 1)
			}

			tables, err := parser.UsedTables(normalizedQuery, dialect)
			if err != nil {
				printErrorJSON(errors2.Wrap(err, "failed to get used tables"))
				return cli.Exit("", 1)
			}

			columns := extractTopLevelSelectColumns(normalizedQuery)
			sort.Strings(columns)
			sort.Strings(tables)

			payload, err := json.MarshalIndent(internalSQLParseResult{
				Columns: columns,
				Tables:  tables,
			}, "", "  ")
			if err != nil {
				printErrorJSON(errors2.Wrap(err, "failed to marshal parser result"))
				return cli.Exit("", 1)
			}

			_, _ = os.Stdout.Write(payload)
			_, _ = os.Stdout.WriteString("\n")
			return nil
		},
	}
}

func extractTopLevelSelectColumns(query string) []string {
	selectStart, fromStart := topLevelSelectBounds(query)
	if selectStart == -1 || fromStart == -1 || selectStart >= fromStart {
		return []string{}
	}

	columns := make([]string, 0)
	seen := make(map[string]struct{})
	for _, expression := range splitTopLevelCSV(query[selectStart:fromStart]) {
		name := outputNameFromExpression(expression)
		if name == "" {
			continue
		}

		name = strings.ToLower(name)
		if _, ok := seen[name]; ok {
			continue
		}
		seen[name] = struct{}{}
		columns = append(columns, name)
	}

	return columns
}

func topLevelSelectBounds(query string) (int, int) {
	selectStart := -1
	depth := 0
	inSingleQuote := false
	inDoubleQuote := false
	inBacktick := false

	for i := 0; i < len(query); i++ {
		switch {
		case inSingleQuote:
			if query[i] == '\'' && !isEscaped(query, i) {
				inSingleQuote = false
			}
			continue
		case inDoubleQuote:
			if query[i] == '"' && !isEscaped(query, i) {
				inDoubleQuote = false
			}
			continue
		case inBacktick:
			if query[i] == '`' && !isEscaped(query, i) {
				inBacktick = false
			}
			continue
		case hasPrefix(query, i, "--"):
			i = skipUntilNewline(query, i+2)
			continue
		case hasPrefix(query, i, "/*"):
			i = skipUntilBlockCommentEnd(query, i+2)
			continue
		}

		switch query[i] {
		case '\'':
			inSingleQuote = true
			continue
		case '"':
			inDoubleQuote = true
			continue
		case '`':
			inBacktick = true
			continue
		case '(':
			depth++
			continue
		case ')':
			if depth > 0 {
				depth--
			}
			continue
		}

		if depth != 0 {
			continue
		}

		if selectStart == -1 && matchesKeyword(query, i, "select") {
			selectStart = i + len("select")
			i += len("select") - 1
			continue
		}

		if selectStart != -1 && matchesKeyword(query, i, "from") {
			return selectStart, i
		}
	}

	return -1, -1
}

func splitTopLevelCSV(selectList string) []string {
	parts := make([]string, 0)
	start := 0
	depth := 0
	inSingleQuote := false
	inDoubleQuote := false
	inBacktick := false

	for i := range len(selectList) {
		switch {
		case inSingleQuote:
			if selectList[i] == '\'' && !isEscaped(selectList, i) {
				inSingleQuote = false
			}
			continue
		case inDoubleQuote:
			if selectList[i] == '"' && !isEscaped(selectList, i) {
				inDoubleQuote = false
			}
			continue
		case inBacktick:
			if selectList[i] == '`' && !isEscaped(selectList, i) {
				inBacktick = false
			}
			continue
		}

		switch selectList[i] {
		case '\'':
			inSingleQuote = true
		case '"':
			inDoubleQuote = true
		case '`':
			inBacktick = true
		case '(':
			depth++
		case ')':
			if depth > 0 {
				depth--
			}
		case ',':
			if depth == 0 {
				parts = append(parts, strings.TrimSpace(selectList[start:i]))
				start = i + 1
			}
		}
	}

	if tail := strings.TrimSpace(selectList[start:]); tail != "" {
		parts = append(parts, tail)
	}

	return parts
}

func outputNameFromExpression(expression string) string {
	if expression == "" {
		return ""
	}

	if position := lastTopLevelKeyword(expression, "as"); position != -1 {
		return normalizeIdentifier(expression[position+len("as"):])
	}

	return normalizeIdentifier(lastTopLevelToken(expression))
}

func lastTopLevelKeyword(expression string, keyword string) int {
	depth := 0
	inSingleQuote := false
	inDoubleQuote := false
	inBacktick := false
	last := -1

	for i := 0; i < len(expression); i++ {
		switch {
		case inSingleQuote:
			if expression[i] == '\'' && !isEscaped(expression, i) {
				inSingleQuote = false
			}
			continue
		case inDoubleQuote:
			if expression[i] == '"' && !isEscaped(expression, i) {
				inDoubleQuote = false
			}
			continue
		case inBacktick:
			if expression[i] == '`' && !isEscaped(expression, i) {
				inBacktick = false
			}
			continue
		}

		switch expression[i] {
		case '\'':
			inSingleQuote = true
		case '"':
			inDoubleQuote = true
		case '`':
			inBacktick = true
		case '(':
			depth++
		case ')':
			if depth > 0 {
				depth--
			}
		}

		if depth == 0 && matchesKeyword(expression, i, keyword) {
			last = i
			i += len(keyword) - 1
		}
	}

	return last
}

func lastTopLevelToken(expression string) string {
	expression = strings.TrimSpace(expression)
	if expression == "" {
		return ""
	}

	depth := 0
	inSingleQuote := false
	inDoubleQuote := false
	inBacktick := false

	for i := len(expression) - 1; i >= 0; i-- {
		switch {
		case inSingleQuote:
			if expression[i] == '\'' && !isEscaped(expression, i) {
				inSingleQuote = false
			}
			continue
		case inDoubleQuote:
			if expression[i] == '"' && !isEscaped(expression, i) {
				inDoubleQuote = false
			}
			continue
		case inBacktick:
			if expression[i] == '`' && !isEscaped(expression, i) {
				inBacktick = false
			}
			continue
		}

		switch expression[i] {
		case '\'':
			inSingleQuote = true
		case '"':
			inDoubleQuote = true
		case '`':
			inBacktick = true
		case ')':
			depth++
		case '(':
			if depth > 0 {
				depth--
			}
		}

		if depth == 0 && unicode.IsSpace(rune(expression[i])) {
			return strings.TrimSpace(expression[i+1:])
		}
	}

	return expression
}

func normalizeIdentifier(value string) string {
	value = strings.TrimSpace(value)
	value = strings.Trim(value, "`\"")
	value = strings.TrimSuffix(value, ".*")
	if index := strings.LastIndex(value, "."); index != -1 {
		value = value[index+1:]
	}
	return strings.Trim(value, "`\"")
}

func matchesKeyword(input string, position int, keyword string) bool {
	if position < 0 || position+len(keyword) > len(input) {
		return false
	}
	if !strings.EqualFold(input[position:position+len(keyword)], keyword) {
		return false
	}

	if position > 0 {
		if previous := rune(input[position-1]); unicode.IsLetter(previous) || unicode.IsDigit(previous) || previous == '_' {
			return false
		}
	}
	if position+len(keyword) < len(input) {
		if next := rune(input[position+len(keyword)]); unicode.IsLetter(next) || unicode.IsDigit(next) || next == '_' {
			return false
		}
	}

	return true
}

func hasPrefix(input string, position int, prefix string) bool {
	return position >= 0 && position+len(prefix) <= len(input) && input[position:position+len(prefix)] == prefix
}

func isEscaped(input string, position int) bool {
	backslashes := 0
	for i := position - 1; i >= 0 && input[i] == '\\'; i-- {
		backslashes++
	}
	return backslashes%2 == 1
}

func skipUntilNewline(input string, position int) int {
	for i := position; i < len(input); i++ {
		if input[i] == '\n' {
			return i
		}
	}
	return len(input)
}

func skipUntilBlockCommentEnd(input string, position int) int {
	for i := position; i < len(input)-1; i++ {
		if input[i] == '*' && input[i+1] == '/' {
			return i + 1
		}
	}
	return len(input)
}
