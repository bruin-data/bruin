package diff

import (
	"fmt"
	"strings"
)

// DatabaseDialect represents the SQL dialect for generating ALTER TABLE statements.
type DatabaseDialect string

const (
	DialectPostgreSQL DatabaseDialect = "postgresql"
	DialectSnowflake  DatabaseDialect = "snowflake"
	DialectBigQuery   DatabaseDialect = "bigquery"
	DialectDuckDB     DatabaseDialect = "duckdb"
	DialectGeneric    DatabaseDialect = "generic"
)

// AlterStatementGenerator generates ALTER TABLE statements based on schema comparison results.
type AlterStatementGenerator struct {
	dialect DatabaseDialect
	reverse bool // If true, generates statements to transform Table1 to match Table2
}

// NewAlterStatementGenerator creates a new ALTER statement generator with the specified dialect.
func NewAlterStatementGenerator(dialect DatabaseDialect, reverse bool) *AlterStatementGenerator {
	return &AlterStatementGenerator{
		dialect: dialect,
		reverse: reverse,
	}
}

// GenerateAlterStatements generates ALTER TABLE SQL statements to transform one table to match another.
// By default (reverse=false), it generates statements to transform Table2 to match Table1.
// If reverse=true, it generates statements to transform Table1 to match Table2.
func (g *AlterStatementGenerator) GenerateAlterStatements(result *SchemaComparisonResult) []string {
	if result == nil || !result.HasSchemaDifferences {
		return []string{}
	}

	var sourceTableName, targetTableName string

	if g.reverse {
		// Transform Table1 to match Table2
		sourceTableName = result.Table1.Table.Name
		targetTableName = result.Table2.Table.Name
	} else {
		// Transform Table2 to match Table1 (default)
		sourceTableName = result.Table2.Table.Name
		targetTableName = result.Table1.Table.Name
	}

	var statements []string
	totalClauses := len(result.MissingColumns) + len(result.ColumnDifferences)*2
	alterClauses := make([]string, 0, totalClauses)

	// Collect all ALTER TABLE clauses
	for _, missingCol := range result.MissingColumns {
		clause := g.generateMissingColumnClause(missingCol, sourceTableName, targetTableName)
		if clause == "" {
			continue
		}
		alterClauses = append(alterClauses, clause)
	}

	for _, colDiff := range result.ColumnDifferences {
		// Generate ALTER COLUMN clauses for type and nullability changes
		clauses := g.generateAlterColumnClauses(colDiff)
		alterClauses = append(alterClauses, clauses...)
	}

	if len(alterClauses) == 0 {
		return statements
	}

	// Try to combine clauses into a single statement if the dialect supports it
	if g.canCombineAlterClauses() {
		stmt := fmt.Sprintf("ALTER TABLE %s\n  %s;",
			g.quoteIdentifier(sourceTableName),
			strings.Join(alterClauses, ",\n  "))
		statements = append(statements, stmt)
	} else {
		// Generate separate ALTER TABLE statements
		for _, clause := range alterClauses {
			stmt := fmt.Sprintf("ALTER TABLE %s %s;",
				g.quoteIdentifier(sourceTableName),
				clause)
			statements = append(statements, stmt)
		}
	}

	return statements
}

// generateMissingColumnClause translates a MissingColumn into the appropriate ALTER TABLE clause.
func (g *AlterStatementGenerator) generateMissingColumnClause(missingCol MissingColumn, sourceTableName, targetTableName string) string {
	switch {
	// Add columns that are missing from the source table (the one we're modifying)
	// and exist in the target table (the one we're matching to)
	case missingCol.MissingFrom == sourceTableName && missingCol.TableName == targetTableName:
		return g.generateAddColumnClause(missingCol.ColumnName, missingCol.Type, missingCol.Nullable, missingCol.PrimaryKey, missingCol.Unique)
	// Drop columns that only exist on the source table and should not be present
	case missingCol.TableName == sourceTableName && missingCol.MissingFrom == targetTableName:
		return g.generateDropColumnClause(missingCol.ColumnName)
	default:
		return ""
	}
}

// generateAddColumnClause generates an ADD COLUMN clause.
func (g *AlterStatementGenerator) generateAddColumnClause(columnName, dataType string, nullable, primaryKey, unique bool) string {
	clause := fmt.Sprintf("ADD COLUMN %s %s", g.quoteIdentifier(columnName), dataType)

	// DuckDB doesn't support adding columns with constraints directly
	if g.dialect != DialectDuckDB {
		if !nullable {
			clause += " NOT NULL"
		}

		if unique {
			clause += " UNIQUE"
		}
	} else {
		// For DuckDB, add constraints as comments
		if !nullable {
			clause += " /* NOT NULL constraint - apply separately if needed */"
		}
		if unique {
			clause += " /* UNIQUE constraint - apply separately if needed */"
		}
	}

	// Note: PRIMARY KEY constraints typically need to be added separately
	// and can't be part of ADD COLUMN in most databases when the table has data
	if primaryKey {
		// For now, we'll add it as a comment since adding PK to existing table is complex
		clause += " /* PRIMARY KEY - may need to be added separately */"
	}

	return clause
}

// generateDropColumnClause generates a DROP COLUMN clause.
func (g *AlterStatementGenerator) generateDropColumnClause(columnName string) string {
	return "DROP COLUMN " + g.quoteIdentifier(columnName)
}

// generateAlterColumnClauses generates ALTER COLUMN clauses for type and nullability changes.
func (g *AlterStatementGenerator) generateAlterColumnClauses(colDiff ColumnDifference) []string {
	var clauses []string

	// Handle type changes
	if colDiff.TypeDifference != nil {
		clause := g.generateAlterTypeClause(colDiff.ColumnName, colDiff.TypeDifference)
		if clause != "" {
			clauses = append(clauses, clause)
		}
	}

	// Handle nullability changes
	if colDiff.NullabilityDifference != nil {
		clause := g.generateAlterNullabilityClause(colDiff.ColumnName, colDiff.NullabilityDifference)
		if clause != "" {
			clauses = append(clauses, clause)
		}
	}

	// Note: UNIQUE constraint changes are complex and often require DROP/ADD CONSTRAINT
	// which we'll skip for now or handle separately

	return clauses
}

// generateAlterTypeClause generates a clause to change a column's data type.
func (g *AlterStatementGenerator) generateAlterTypeClause(columnName string, typeDiff *TypeDifference) string {
	var newType string
	if g.reverse {
		newType = typeDiff.Table2Type
	} else {
		newType = typeDiff.Table1Type
	}

	switch g.dialect {
	case DialectPostgreSQL, DialectDuckDB:
		return fmt.Sprintf("ALTER COLUMN %s TYPE %s", g.quoteIdentifier(columnName), newType)
	case DialectSnowflake, DialectBigQuery:
		return fmt.Sprintf("ALTER COLUMN %s SET DATA TYPE %s", g.quoteIdentifier(columnName), newType)
	case DialectGeneric:
		return fmt.Sprintf("ALTER COLUMN %s TYPE %s", g.quoteIdentifier(columnName), newType)
	default:
		// Generic SQL - use PostgreSQL syntax as it's more common
		return fmt.Sprintf("ALTER COLUMN %s TYPE %s", g.quoteIdentifier(columnName), newType)
	}
}

// generateAlterNullabilityClause generates a clause to change a column's nullability.
func (g *AlterStatementGenerator) generateAlterNullabilityClause(columnName string, nullDiff *NullabilityDifference) string {
	var shouldBeNullable bool
	if g.reverse {
		shouldBeNullable = nullDiff.Table2Nullable
	} else {
		shouldBeNullable = nullDiff.Table1Nullable
	}

	switch g.dialect {
	case DialectPostgreSQL, DialectDuckDB:
		if shouldBeNullable {
			return fmt.Sprintf("ALTER COLUMN %s DROP NOT NULL", g.quoteIdentifier(columnName))
		}
		return fmt.Sprintf("ALTER COLUMN %s SET NOT NULL", g.quoteIdentifier(columnName))
	case DialectSnowflake:
		if shouldBeNullable {
			return fmt.Sprintf("ALTER COLUMN %s DROP NOT NULL", g.quoteIdentifier(columnName))
		}
		return fmt.Sprintf("ALTER COLUMN %s SET NOT NULL", g.quoteIdentifier(columnName))
	case DialectBigQuery:
		// BigQuery doesn't support ALTER COLUMN for nullability changes on existing columns
		// This requires recreating the table
		return fmt.Sprintf("/* BigQuery does not support changing nullability - column %s would need table recreation */", g.quoteIdentifier(columnName))
	case DialectGeneric:
		fallthrough
	default:
		if shouldBeNullable {
			return fmt.Sprintf("ALTER COLUMN %s DROP NOT NULL", g.quoteIdentifier(columnName))
		}
		return fmt.Sprintf("ALTER COLUMN %s SET NOT NULL", g.quoteIdentifier(columnName))
	}
}

// canCombineAlterClauses returns true if the dialect supports combining multiple ALTER clauses in one statement.
func (g *AlterStatementGenerator) canCombineAlterClauses() bool {
	switch g.dialect {
	case DialectPostgreSQL:
		return true
	case DialectDuckDB:
		// DuckDB only supports one ALTER command per statement
		return false
	case DialectSnowflake:
		return true
	case DialectBigQuery:
		// BigQuery supports multiple ADD COLUMN but not mixing with ALTER COLUMN
		return false
	case DialectGeneric:
		return true
	default:
		return true
	}
}

// quoteIdentifier quotes an identifier based on the database dialect.
func (g *AlterStatementGenerator) quoteIdentifier(identifier string) string {
	// If already quoted, return as-is
	if strings.HasPrefix(identifier, "\"") && strings.HasSuffix(identifier, "\"") {
		return identifier
	}
	if strings.HasPrefix(identifier, "`") && strings.HasSuffix(identifier, "`") {
		return identifier
	}

	switch g.dialect {
	case DialectBigQuery:
		return fmt.Sprintf("`%s`", identifier)
	case DialectPostgreSQL:
		if strings.Contains(identifier, ".") {
			parts := strings.Split(identifier, ".")
			quotedParts := make([]string, len(parts))
			for i, part := range parts {
				quotedParts[i] = fmt.Sprintf("\"%s\"", part)
			}
			return strings.Join(quotedParts, ".")
		}
		return fmt.Sprintf("\"%s\"", identifier)
	case DialectSnowflake, DialectDuckDB:
		return fmt.Sprintf("\"%s\"", identifier)
	case DialectGeneric:
		return fmt.Sprintf("\"%s\"", identifier)
	default:
		return fmt.Sprintf("\"%s\"", identifier)
	}
}

// DetectDialectFromConnection detects the database dialect from connection type names.
func DetectDialectFromConnection(conn1Type, conn2Type string) DatabaseDialect {
	// Normalize connection types
	conn1Lower := strings.ToLower(conn1Type)
	conn2Lower := strings.ToLower(conn2Type)

	// If both connections are the same type, use that dialect
	if conn1Lower == conn2Lower {
		return dialectFromConnectionType(conn1Lower)
	}

	// If different, return the second connection's dialect (as per requirements)
	return dialectFromConnectionType(conn2Lower)
}

// dialectFromConnectionType maps a connection type string to a DatabaseDialect.
func dialectFromConnectionType(connType string) DatabaseDialect {
	connType = strings.ToLower(connType)

	switch {
	case strings.Contains(connType, "postgres"), strings.Contains(connType, "pg"):
		return DialectPostgreSQL
	case strings.Contains(connType, "snowflake"):
		return DialectSnowflake
	case strings.Contains(connType, "bigquery"), strings.Contains(connType, "bq"):
		return DialectBigQuery
	case strings.Contains(connType, "duckdb"), strings.Contains(connType, "duck"):
		return DialectDuckDB
	default:
		return DialectGeneric
	}
}
