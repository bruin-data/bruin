package query

import (
	"strconv"
	"strings"
	"unicode"
)

func IsLikelyResultQuery(sql string) bool {
	sql = StripLeadingSQLComments(sql)
	upperSQL := strings.ToUpper(sql)
	switch FirstSQLKeyword(sql) {
	case "SELECT", "WITH", "SHOW", "DESCRIBE", "DESC", "EXPLAIN", "PRAGMA":
		return true
	default:
		return containsSQLKeyword(upperSQL, "RETURNING")
	}
}

func StripLeadingSQLComments(sql string) string {
	sql = strings.TrimSpace(sql)
	for {
		if strings.HasPrefix(sql, "--") {
			if newline := strings.IndexByte(sql, '\n'); newline >= 0 {
				sql = strings.TrimSpace(sql[newline+1:])
				continue
			}
			return ""
		}
		if strings.HasPrefix(sql, "/*") {
			end := strings.Index(sql, "*/")
			if end >= 0 {
				sql = strings.TrimSpace(sql[end+2:])
				continue
			}
			return ""
		}
		return sql
	}
}

func FirstSQLKeyword(sql string) string {
	sql = StripLeadingSQLComments(sql)
	for i, r := range sql {
		if !unicode.IsLetter(r) && r != '_' {
			return strings.ToUpper(sql[:i])
		}
	}

	return strings.ToUpper(sql)
}

func SQLStatementType(sql string) string {
	sql = StripLeadingSQLComments(sql)
	fields := strings.Fields(sql)
	if len(fields) == 0 {
		return ""
	}

	first := strings.ToUpper(fields[0])
	if len(fields) == 1 {
		return first
	}

	switch first {
	case "ALTER", "CREATE", "DROP":
		return first + " " + strings.ToUpper(fields[1])
	default:
		return first
	}
}

func StatementTypeFromCommandTag(commandTag string) string {
	fields := strings.Fields(commandTag)
	for len(fields) > 0 && isInteger(fields[len(fields)-1]) {
		fields = fields[:len(fields)-1]
	}
	if len(fields) == 0 {
		return ""
	}

	return strings.ToUpper(strings.Join(fields, " "))
}

func IsDMLStatement(statementType string) bool {
	fields := strings.Fields(strings.ToUpper(statementType))
	if len(fields) == 0 {
		return false
	}

	switch fields[0] {
	case "INSERT", "UPDATE", "DELETE", "MERGE", "TRUNCATE", "TRUNCATE_TABLE":
		return true
	default:
		return false
	}
}

func IsNonResultStatement(summary *QueryExecutionSummary) bool {
	if summary == nil {
		return false
	}
	if summary.DMLAffectedRows != nil || summary.DMLStats != nil || summary.DDLOperationPerformed != "" {
		return true
	}

	statementType := strings.ToUpper(summary.StatementType)
	return statementType != "" && statementType != "SELECT"
}

func NewExecutionSummaryFromStatement(connectionType, statementType string, rowsAffected *int64) *QueryExecutionSummary {
	statementType = strings.ToUpper(strings.TrimSpace(statementType))
	summary := &QueryExecutionSummary{
		ConnectionType: connectionType,
		StatementType:  statementType,
	}
	if rowsAffected != nil && IsDMLStatement(statementType) {
		summary.DMLAffectedRows = rowsAffected
	}

	return summary
}

func containsSQLKeyword(sql, keyword string) bool {
	for i := 0; i < len(sql); i++ {
		switch {
		case sql[i] == '\'':
			i = skipQuotedString(sql, i, '\'')
			continue
		case sql[i] == '"':
			i = skipQuotedString(sql, i, '"')
			continue
		case strings.HasPrefix(sql[i:], "--"):
			if newline := strings.IndexByte(sql[i:], '\n'); newline >= 0 {
				i += newline
				continue
			}
			return false
		case strings.HasPrefix(sql[i:], "/*"):
			if end := strings.Index(sql[i+2:], "*/"); end >= 0 {
				i += end + 3
				continue
			}
			return false
		}

		if !strings.HasPrefix(sql[i:], keyword) {
			continue
		}

		start := i == 0 || !isSQLIdentifierRune(rune(sql[i-1]))
		endIndex := i + len(keyword)
		end := endIndex == len(sql) || !isSQLIdentifierRune(rune(sql[endIndex]))
		if start && end {
			return true
		}
	}

	return false
}

func skipQuotedString(sql string, start int, quote byte) int {
	for i := start + 1; i < len(sql); i++ {
		if sql[i] != quote {
			continue
		}
		if i+1 < len(sql) && sql[i+1] == quote {
			i++
			continue
		}
		return i
	}

	return len(sql) - 1
}

func isSQLIdentifierRune(r rune) bool {
	return unicode.IsLetter(r) || unicode.IsDigit(r) || r == '_'
}

func isInteger(value string) bool {
	_, err := strconv.ParseInt(value, 10, 64)
	return err == nil
}
