package bigquery

import (
	"fmt"
	"strings"

	"cloud.google.com/go/bigquery"
	"github.com/bruin-data/bruin/pkg/schemaevolution"
	"github.com/bruin-data/bruin/pkg/sqlengine"
)

const bigQuerySQLDialect = "bigquery"

var bigQueryTypeFormatConfig = schemaevolution.TypeFormatConfig{
	SQLDialect: bigQuerySQLDialect,
	DTypeFolds: map[sqlengine.DType]sqlengine.DType{
		sqlengine.DT_TINYINT:   sqlengine.DT_BIGINT,
		sqlengine.DT_SMALLINT:  sqlengine.DT_BIGINT,
		sqlengine.DT_INT:       sqlengine.DT_BIGINT,
		sqlengine.DT_FLOAT:     sqlengine.DT_DOUBLE,
		sqlengine.DT_TIMESTAMP: sqlengine.DT_DATETIME,
	},
	TypeNames: map[sqlengine.DType]string{
		sqlengine.DT_BIGINT:      "INT64",
		sqlengine.DT_DOUBLE:      "FLOAT64",
		sqlengine.DT_DECIMAL:     "NUMERIC",
		sqlengine.DT_BIGDECIMAL:  "BIGNUMERIC",
		sqlengine.DT_VARCHAR:     "STRING",
		sqlengine.DT_BINARY:      "BYTES",
		sqlengine.DT_BOOLEAN:     "BOOL",
		sqlengine.DT_DATETIME:    "DATETIME",
		sqlengine.DT_TIMESTAMPTZ: "TIMESTAMP",
	},
	GenericArraySyntax:       true,
	KeepTypeLength:           true,
	OmitDefaultDecimalParams: true,
	DefaultDecimalPrecision:  38,
	DefaultDecimalScale:      9,
}

var bigQueryBasePolicy = schemaevolution.PolicyDialect{
	Policy: schemaevolution.TypePolicy{
		Name:                     "BigQuery",
		Format:                   bigQueryTypeFormatConfig,
		NotNullAddedColumnReason: "cannot add a new NOT NULL (REQUIRED) column via ALTER TABLE in BigQuery",
		MaxSignedIntDType:        sqlengine.DT_BIGINT,
		AllowDecimalWidening:     true,
		AllowIntToDecimal:        true,
		MaxDecimalPrecision:      38,
		MaxDecimalIntDigits:      29,
		MaxDecimalScale:          9,
		AllowDecimalToBigDecimal: true,
		AllowLengthWidening:      true,
	},
}

// BigQueryDialect implements schemaevolution.Dialect for Google BigQuery by embedding
// PolicyDialect and providing GoogleSQL ALTER TABLE statement rendering.
type BigQueryDialect struct {
	schemaevolution.PolicyDialect
}

func newBigQueryDialect() BigQueryDialect {
	return BigQueryDialect{PolicyDialect: bigQueryBasePolicy}
}

// RenderStatements renders all planned evolution actions into GoogleSQL ALTER TABLE statements.
// BigQuery allows batching multiple ADD COLUMN and ALTER COLUMN SET DATA TYPE clauses in a single
// ALTER TABLE statement, while DEFAULT values on new columns require a follow-up ALTER COLUMN SET DEFAULT.
func (d BigQueryDialect) RenderStatements(tableName string, plan *schemaevolution.Plan) []string {
	if plan.IsEmpty() {
		return nil
	}

	quotedTable := schemaevolution.QuoteQualifiedTableName(bigQuerySQLDialect, tableName, schemaevolution.QuoteBacktickIdentifier)
	clauses := make([]string, 0, len(plan.Actions))
	var defaultStmts []string

	for _, action := range plan.Actions {
		if action.Type == schemaevolution.ActionAlterColumnType {
			quotedCol := schemaevolution.QuoteBacktickIdentifier(action.ColumnName)
			clauses = append(clauses, fmt.Sprintf("ALTER COLUMN %s SET DATA TYPE %s", quotedCol, action.ToType))
		}
	}

	for _, action := range plan.Actions {
		if action.Type != schemaevolution.ActionAddColumn {
			continue
		}
		col := action.Column
		quotedCol := schemaevolution.QuoteBacktickIdentifier(col.Name)
		clause := fmt.Sprintf("ADD COLUMN %s %s", quotedCol, col.Type)
		if col.Collation != "" {
			clause += fmt.Sprintf(" COLLATE '%s'", strings.ReplaceAll(col.Collation, "'", "''"))
		}
		clauses = append(clauses, clause)
		if col.Default != "" {
			defaultStmts = append(defaultStmts, fmt.Sprintf(
				"ALTER TABLE %s ALTER COLUMN %s SET DEFAULT (%s)",
				quotedTable, quotedCol, col.Default,
			))
		}
	}

	stmts := make([]string, 0, 1+len(defaultStmts))
	if len(clauses) > 0 {
		stmts = append(stmts, fmt.Sprintf("ALTER TABLE %s %s", quotedTable, strings.Join(clauses, ", ")))
	}
	stmts = append(stmts, defaultStmts...)
	return stmts
}

// isBigQueryNonGroupableType reports whether sqlType is or contains a GoogleSQL type
// (ARRAY, JSON, or GEOGRAPHY) that BigQuery rejects in `=` / `!=` / `IS DISTINCT FROM`.
func isBigQueryNonGroupableType(sqlType string) bool {
	return hasNonGroupableBigQueryType(schemaevolution.ParseType(bigQueryTypeFormatConfig, sqlType))
}

func hasNonGroupableBigQueryType(pt schemaevolution.ParsedType) bool {
	switch pt.DType { //nolint:exhaustive
	case sqlengine.DT_ARRAY, sqlengine.DT_JSON, sqlengine.DT_JSONB, sqlengine.DT_GEOGRAPHY:
		return true
	case sqlengine.DT_STRUCT:
		for _, f := range pt.Fields {
			if hasNonGroupableBigQueryType(f.Type) {
				return true
			}
		}
		return false
	default:
		return false
	}
}

// bigQueryFieldSchemaToSQLType converts a cloud.google.com/go/bigquery FieldSchema into its
// canonical GoogleSQL DDL type string (preserving STRUCT fields, ARRAY wrappers, lengths, and precision/scale).
func bigQueryFieldSchemaToSQLType(fs *bigquery.FieldSchema) string {
	if fs == nil {
		return ""
	}
	base := bigQueryFieldBaseSQLType(fs)
	if base == "" {
		return ""
	}
	if fs.Repeated {
		return "ARRAY<" + base + ">"
	}
	return base
}

func bigQueryFieldBaseSQLType(fs *bigquery.FieldSchema) string {
	switch fs.Type { //nolint:exhaustive
	case bigquery.RecordFieldType:
		fields := make([]string, 0, len(fs.Schema))
		for _, child := range fs.Schema {
			if child == nil || child.Name == "" {
				continue
			}
			fields = append(fields, fmt.Sprintf("%s %s", schemaevolution.QuoteBacktickIdentifier(child.Name), bigQueryFieldSchemaToSQLType(child)))
		}
		return "STRUCT<" + strings.Join(fields, ", ") + ">"

	case bigquery.RangeFieldType:
		if fs.RangeElementType != nil && fs.RangeElementType.Type != "" {
			return fmt.Sprintf("RANGE<%s>", string(fs.RangeElementType.Type))
		}
		return "RANGE"

	case bigquery.IntegerFieldType:
		return "INT64"
	case bigquery.FloatFieldType:
		return "FLOAT64"
	case bigquery.BooleanFieldType:
		return "BOOL"
	case bigquery.StringFieldType:
		if fs.MaxLength > 0 {
			return fmt.Sprintf("STRING(%d)", fs.MaxLength)
		}
		return "STRING"
	case bigquery.BytesFieldType:
		if fs.MaxLength > 0 {
			return fmt.Sprintf("BYTES(%d)", fs.MaxLength)
		}
		return "BYTES"
	case bigquery.NumericFieldType:
		if fs.Precision > 0 {
			return fmt.Sprintf("NUMERIC(%d,%d)", fs.Precision, fs.Scale)
		}
		return "NUMERIC"
	case bigquery.BigNumericFieldType:
		if fs.Precision > 0 {
			return fmt.Sprintf("BIGNUMERIC(%d,%d)", fs.Precision, fs.Scale)
		}
		return "BIGNUMERIC"
	default:
		return strings.ToUpper(strings.TrimSpace(string(fs.Type)))
	}
}
