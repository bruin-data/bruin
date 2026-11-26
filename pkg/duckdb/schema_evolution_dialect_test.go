package duck

import (
	"testing"

	"github.com/bruin-data/bruin/pkg/schemaevolution"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func boolPtr(v bool) *bool { return &v }

func TestDuckDBDialect_ResolveTypeChange(t *testing.T) {
	t.Parallel()

	d := newDuckDBDialect(false, false, false)

	tests := []struct {
		name       string
		existing   schemaevolution.Column
		incoming   schemaevolution.Column
		wantRes    schemaevolution.TypeResolution
		wantTarget string
	}{
		// Equivalent / normalized
		{"same int alias", schemaevolution.Column{Type: "INT"}, schemaevolution.Column{Type: "INTEGER"}, schemaevolution.TypeChangeNone, ""},
		{"double precision alias", schemaevolution.Column{Type: "DOUBLE"}, schemaevolution.Column{Type: "DOUBLE PRECISION"}, schemaevolution.TypeChangeNone, ""},
		{"timestamp without tz alias", schemaevolution.Column{Type: "TIMESTAMP"}, schemaevolution.Column{Type: "TIMESTAMP WITHOUT TIME ZONE"}, schemaevolution.TypeChangeNone, ""},
		{"varchar aliases", schemaevolution.Column{Type: "VARCHAR(50)"}, schemaevolution.Column{Type: "TEXT"}, schemaevolution.TypeChangeNone, ""},
		{"boolean aliases", schemaevolution.Column{Type: "BOOL"}, schemaevolution.Column{Type: "BOOLEAN"}, schemaevolution.TypeChangeNone, ""},
		{"timestamptz alias", schemaevolution.Column{Type: "TIMESTAMPTZ"}, schemaevolution.Column{Type: "TIMESTAMP WITH TIME ZONE"}, schemaevolution.TypeChangeNone, ""},
		{"decimal spacing", schemaevolution.Column{Type: "DECIMAL(10, 2)"}, schemaevolution.Column{Type: "NUMERIC(10,2)"}, schemaevolution.TypeChangeNone, ""},
		{"incoming untyped null keeps existing", schemaevolution.Column{Type: "VARCHAR"}, schemaevolution.Column{Type: "INTEGER", UntypedNull: true}, schemaevolution.TypeChangeNone, ""},

		// Implicit DuckDB insert casts (no-op)
		{"existing varchar absorbs integer", schemaevolution.Column{Type: "VARCHAR"}, schemaevolution.Column{Type: "INTEGER"}, schemaevolution.TypeChangeNone, ""},
		{"existing varchar absorbs date", schemaevolution.Column{Type: "VARCHAR"}, schemaevolution.Column{Type: "DATE"}, schemaevolution.TypeChangeNone, ""},
		{"existing varchar absorbs list", schemaevolution.Column{Type: "VARCHAR"}, schemaevolution.Column{Type: "INTEGER[]"}, schemaevolution.TypeChangeNone, ""},
		{"existing varchar absorbs struct", schemaevolution.Column{Type: "VARCHAR"}, schemaevolution.Column{Type: "STRUCT(a INTEGER)"}, schemaevolution.TypeChangeNone, ""},
		{"existing date absorbs varchar literal", schemaevolution.Column{Type: "DATE"}, schemaevolution.Column{Type: "VARCHAR"}, schemaevolution.TypeChangeNone, ""},
		{"existing enum absorbs varchar literal", schemaevolution.Column{Type: "ENUM('sad', 'Happy')"}, schemaevolution.Column{Type: "VARCHAR"}, schemaevolution.TypeChangeNone, ""},
		{"existing uuid absorbs varchar literal", schemaevolution.Column{Type: "UUID"}, schemaevolution.Column{Type: "VARCHAR"}, schemaevolution.TypeChangeNone, ""},

		// Signed integer widening & narrowing (no-op)
		{"tinyint to smallint", schemaevolution.Column{Type: "TINYINT"}, schemaevolution.Column{Type: "SMALLINT"}, schemaevolution.TypeChangeWiden, "SMALLINT"},
		{"int to bigint", schemaevolution.Column{Type: "INTEGER"}, schemaevolution.Column{Type: "BIGINT"}, schemaevolution.TypeChangeWiden, "BIGINT"},
		{"bigint to hugeint", schemaevolution.Column{Type: "BIGINT"}, schemaevolution.Column{Type: "HUGEINT"}, schemaevolution.TypeChangeWiden, "HUGEINT"},
		{"bigint to int is no-op", schemaevolution.Column{Type: "BIGINT"}, schemaevolution.Column{Type: "INTEGER"}, schemaevolution.TypeChangeNone, ""},

		// Unsigned integer widening & mixed signed/unsigned
		{"utinyint to uinteger", schemaevolution.Column{Type: "UTINYINT"}, schemaevolution.Column{Type: "UINTEGER"}, schemaevolution.TypeChangeWiden, "UINTEGER"},
		{"ubigint to uinteger is no-op", schemaevolution.Column{Type: "UBIGINT"}, schemaevolution.Column{Type: "UINTEGER"}, schemaevolution.TypeChangeNone, ""},
		{"utinyint to smallint", schemaevolution.Column{Type: "UTINYINT"}, schemaevolution.Column{Type: "SMALLINT"}, schemaevolution.TypeChangeWiden, "SMALLINT"},
		{"uinteger to integer widens to bigint", schemaevolution.Column{Type: "UINTEGER"}, schemaevolution.Column{Type: "INTEGER"}, schemaevolution.TypeChangeWiden, "BIGINT"},
		{"bigint absorbs uinteger", schemaevolution.Column{Type: "BIGINT"}, schemaevolution.Column{Type: "UINTEGER"}, schemaevolution.TypeChangeNone, ""},
		{"integer plus uinteger widens to bigint", schemaevolution.Column{Type: "INTEGER"}, schemaevolution.Column{Type: "UINTEGER"}, schemaevolution.TypeChangeWiden, "BIGINT"},
		{"uhugeint to signed incompatible", schemaevolution.Column{Type: "UHUGEINT"}, schemaevolution.Column{Type: "BIGINT"}, schemaevolution.TypeChangeIncompatible, ""},

		// Floating point & exact-to-float rejections
		{"float to double", schemaevolution.Column{Type: "FLOAT"}, schemaevolution.Column{Type: "DOUBLE"}, schemaevolution.TypeChangeWiden, "DOUBLE"},
		{"double to float is no-op", schemaevolution.Column{Type: "DOUBLE"}, schemaevolution.Column{Type: "FLOAT"}, schemaevolution.TypeChangeNone, ""},
		{"int to double rejected (exact to approximate)", schemaevolution.Column{Type: "INTEGER"}, schemaevolution.Column{Type: "DOUBLE"}, schemaevolution.TypeChangeIncompatible, ""},
		{"smallint to float rejected (exact to approximate)", schemaevolution.Column{Type: "SMALLINT"}, schemaevolution.Column{Type: "FLOAT"}, schemaevolution.TypeChangeIncompatible, ""},
		{"int to float rejected (exact to approximate)", schemaevolution.Column{Type: "INTEGER"}, schemaevolution.Column{Type: "FLOAT"}, schemaevolution.TypeChangeIncompatible, ""},
		{"bigint to double rejected as lossy", schemaevolution.Column{Type: "BIGINT"}, schemaevolution.Column{Type: "DOUBLE"}, schemaevolution.TypeChangeIncompatible, ""},
		{"ubigint to double rejected as lossy", schemaevolution.Column{Type: "UBIGINT"}, schemaevolution.Column{Type: "DOUBLE"}, schemaevolution.TypeChangeIncompatible, ""},
		{"hugeint to double rejected as lossy", schemaevolution.Column{Type: "HUGEINT"}, schemaevolution.Column{Type: "DOUBLE"}, schemaevolution.TypeChangeIncompatible, ""},
		{"double absorbs bigint", schemaevolution.Column{Type: "DOUBLE"}, schemaevolution.Column{Type: "BIGINT"}, schemaevolution.TypeChangeNone, ""},

		// Decimals
		{"decimal precision increase", schemaevolution.Column{Type: "DECIMAL(10,2)"}, schemaevolution.Column{Type: "DECIMAL(14,2)"}, schemaevolution.TypeChangeWiden, "DECIMAL(14,2)"},
		{"decimal scale and precision reconciliation", schemaevolution.Column{Type: "DECIMAL(10,2)"}, schemaevolution.Column{Type: "DECIMAL(10,4)"}, schemaevolution.TypeChangeWiden, "DECIMAL(12,4)"},
		{"decimal already wider is no-op", schemaevolution.Column{Type: "DECIMAL(18,4)"}, schemaevolution.Column{Type: "DECIMAL(10,2)"}, schemaevolution.TypeChangeNone, ""},
		{"decimal overflow incompatible", schemaevolution.Column{Type: "DECIMAL(38,0)"}, schemaevolution.Column{Type: "DECIMAL(10,4)"}, schemaevolution.TypeChangeIncompatible, ""},
		{"int to decimal", schemaevolution.Column{Type: "INTEGER"}, schemaevolution.Column{Type: "DECIMAL(12,2)"}, schemaevolution.TypeChangeWiden, "DECIMAL(12,2)"},
		{"int to narrow decimal preserves int digits", schemaevolution.Column{Type: "INTEGER"}, schemaevolution.Column{Type: "DECIMAL(8,2)"}, schemaevolution.TypeChangeWiden, "DECIMAL(12,2)"},
		{"decimal absorbs smallint", schemaevolution.Column{Type: "DECIMAL(10,2)"}, schemaevolution.Column{Type: "SMALLINT"}, schemaevolution.TypeChangeNone, ""},
		{"decimal(10,2) to double rejected (exact to approximate)", schemaevolution.Column{Type: "DECIMAL(10,2)"}, schemaevolution.Column{Type: "DOUBLE"}, schemaevolution.TypeChangeIncompatible, ""},
		{"decimal(18,3) to double rejected as lossy", schemaevolution.Column{Type: "DECIMAL(18,3)"}, schemaevolution.Column{Type: "DOUBLE"}, schemaevolution.TypeChangeIncompatible, ""},
		{"double absorbs decimal", schemaevolution.Column{Type: "DOUBLE"}, schemaevolution.Column{Type: "DECIMAL(18,3)"}, schemaevolution.TypeChangeNone, ""},
		{"signed int absorbs small whole-number decimal(5,0)", schemaevolution.Column{Type: "INTEGER"}, schemaevolution.Column{Type: "DECIMAL(5,0)"}, schemaevolution.TypeChangeNone, ""},
		{"signed bigint absorbs whole-number decimal(18,0)", schemaevolution.Column{Type: "BIGINT"}, schemaevolution.Column{Type: "DECIMAL(18,0)"}, schemaevolution.TypeChangeNone, ""},
		{"signed int widens on decimal(10,0)", schemaevolution.Column{Type: "INTEGER"}, schemaevolution.Column{Type: "DECIMAL(10,0)"}, schemaevolution.TypeChangeWiden, "DECIMAL(10,0)"},
		{"unsigned utinyint widens on decimal(2,0) due to sign", schemaevolution.Column{Type: "UTINYINT"}, schemaevolution.Column{Type: "DECIMAL(2,0)"}, schemaevolution.TypeChangeWiden, "DECIMAL(3,0)"},

		// Temporal
		{"date to timestamp", schemaevolution.Column{Type: "DATE"}, schemaevolution.Column{Type: "TIMESTAMP"}, schemaevolution.TypeChangeWiden, "TIMESTAMP"},
		{"date to timestamptz", schemaevolution.Column{Type: "DATE"}, schemaevolution.Column{Type: "TIMESTAMPTZ"}, schemaevolution.TypeChangeWiden, "TIMESTAMP WITH TIME ZONE"},
		{"timestamp_s to timestamp", schemaevolution.Column{Type: "TIMESTAMP_S"}, schemaevolution.Column{Type: "TIMESTAMP"}, schemaevolution.TypeChangeWiden, "TIMESTAMP"},
		{"timestamp to timestamp_ns rejected (1677..2262 overflow)", schemaevolution.Column{Type: "TIMESTAMP"}, schemaevolution.Column{Type: "TIMESTAMP_NS"}, schemaevolution.TypeChangeIncompatible, ""},
		{"date to timestamp_ns rejected (1677..2262 overflow)", schemaevolution.Column{Type: "DATE"}, schemaevolution.Column{Type: "TIMESTAMP_NS"}, schemaevolution.TypeChangeIncompatible, ""},
		{"timestamp to timestamptz", schemaevolution.Column{Type: "TIMESTAMP"}, schemaevolution.Column{Type: "TIMESTAMPTZ"}, schemaevolution.TypeChangeWiden, "TIMESTAMP WITH TIME ZONE"},
		{"timestamp list to timestamptz list", schemaevolution.Column{Type: "TIMESTAMP[]"}, schemaevolution.Column{Type: "TIMESTAMP WITH TIME ZONE[]"}, schemaevolution.TypeChangeWiden, "TIMESTAMP WITH TIME ZONE[]"},
		{"timestamp absorbs date", schemaevolution.Column{Type: "TIMESTAMP"}, schemaevolution.Column{Type: "DATE"}, schemaevolution.TypeChangeNone, ""},
		{"timestamptz absorbs timestamp", schemaevolution.Column{Type: "TIMESTAMPTZ"}, schemaevolution.Column{Type: "TIMESTAMP"}, schemaevolution.TypeChangeNone, ""},
		{"timestamp_ns to timestamptz incompatible", schemaevolution.Column{Type: "TIMESTAMP_NS"}, schemaevolution.Column{Type: "TIMESTAMPTZ"}, schemaevolution.TypeChangeIncompatible, ""},
		{"time to timetz incompatible", schemaevolution.Column{Type: "TIME"}, schemaevolution.Column{Type: "TIMETZ"}, schemaevolution.TypeChangeIncompatible, ""},
		{"timetz absorbs time", schemaevolution.Column{Type: "TIMETZ"}, schemaevolution.Column{Type: "TIME"}, schemaevolution.TypeChangeNone, ""},

		// Lists and fixed-size arrays
		{"int list to bigint list", schemaevolution.Column{Type: "INTEGER[]"}, schemaevolution.Column{Type: "BIGINT[]"}, schemaevolution.TypeChangeWiden, "BIGINT[]"},
		{"bigint list absorbs int list", schemaevolution.Column{Type: "BIGINT[]"}, schemaevolution.Column{Type: "INTEGER[]"}, schemaevolution.TypeChangeNone, ""},
		{"int list to varchar list incompatible", schemaevolution.Column{Type: "INTEGER[]"}, schemaevolution.Column{Type: "VARCHAR[]"}, schemaevolution.TypeChangeIncompatible, ""},
		{"fixed array int[3] to bigint[3] preserves size", schemaevolution.Column{Type: "INTEGER[3]"}, schemaevolution.Column{Type: "BIGINT[3]"}, schemaevolution.TypeChangeWiden, "BIGINT[3]"},
		{"fixed array int[3] to int[4] incompatible", schemaevolution.Column{Type: "INTEGER[3]"}, schemaevolution.Column{Type: "INTEGER[4]"}, schemaevolution.TypeChangeIncompatible, ""},
		{"fixed array int[3] to variable int[] incompatible", schemaevolution.Column{Type: "INTEGER[3]"}, schemaevolution.Column{Type: "INTEGER[]"}, schemaevolution.TypeChangeIncompatible, ""},
		{"variable int[] absorbs fixed int[3]", schemaevolution.Column{Type: "INTEGER[]"}, schemaevolution.Column{Type: "INTEGER[3]"}, schemaevolution.TypeChangeNone, ""},
		{"variable int[] widens to bigint[] on fixed bigint[3]", schemaevolution.Column{Type: "INTEGER[]"}, schemaevolution.Column{Type: "BIGINT[3]"}, schemaevolution.TypeChangeWiden, "BIGINT[]"},
		{"untyped empty list absorbed by list", schemaevolution.Column{Type: "INTEGER[]"}, schemaevolution.Column{Type: `"NULL"[]`}, schemaevolution.TypeChangeNone, ""},
		{"untyped empty list absorbed by nested list", schemaevolution.Column{Type: "INTEGER[][]"}, schemaevolution.Column{Type: `"NULL"[]`}, schemaevolution.TypeChangeNone, ""},
		{"untyped empty list absorbed by varchar", schemaevolution.Column{Type: "VARCHAR"}, schemaevolution.Column{Type: `"NULL"[]`}, schemaevolution.TypeChangeNone, ""},
		{"untyped empty list rejected for scalar integer", schemaevolution.Column{Type: "INTEGER"}, schemaevolution.Column{Type: `"NULL"[]`}, schemaevolution.TypeChangeIncompatible, ""},
		{"untyped empty list rejected for fixed-size array", schemaevolution.Column{Type: "INTEGER[3]"}, schemaevolution.Column{Type: `"NULL"[]`}, schemaevolution.TypeChangeIncompatible, ""},
		{"deeper untyped empty list rejected for shallower list", schemaevolution.Column{Type: "INTEGER[]"}, schemaevolution.Column{Type: `"NULL"[][]`}, schemaevolution.TypeChangeIncompatible, ""},
		{"struct list field absorbs matching untyped empty list", schemaevolution.Column{Type: "STRUCT(tags INTEGER[])"}, schemaevolution.Column{Type: `STRUCT(tags "NULL"[])`}, schemaevolution.TypeChangeNone, ""},
		{"struct list field rejects deeper untyped empty list", schemaevolution.Column{Type: "STRUCT(tags INTEGER[])"}, schemaevolution.Column{Type: `STRUCT(tags "NULL"[][])`}, schemaevolution.TypeChangeIncompatible, ""},
		{"struct fixed-array list field rejects untyped empty inner list", schemaevolution.Column{Type: "STRUCT(arr INTEGER[3][])"}, schemaevolution.Column{Type: `STRUCT(arr "NULL"[][])`}, schemaevolution.TypeChangeIncompatible, ""},
		{"struct wider field absorbs narrower concrete field", schemaevolution.Column{Type: "STRUCT(a VARCHAR, b BIGINT)"}, schemaevolution.Column{Type: "STRUCT(a VARCHAR, b INTEGER)"}, schemaevolution.TypeChangeNone, ""},
		{"struct list absorbs narrower struct element", schemaevolution.Column{Type: "STRUCT(a BIGINT)[]"}, schemaevolution.Column{Type: "STRUCT(a INTEGER)[]"}, schemaevolution.TypeChangeNone, ""},
		{"map wider value absorbs narrower concrete value", schemaevolution.Column{Type: "MAP(VARCHAR, BIGINT)"}, schemaevolution.Column{Type: "MAP(VARCHAR, INTEGER)"}, schemaevolution.TypeChangeNone, ""},
		{"union absorbs subset member with untyped null", schemaevolution.Column{Type: "UNION(a VARCHAR, b BIGINT)"}, schemaevolution.Column{Type: `UNION(a "NULL")`}, schemaevolution.TypeChangeNone, ""},
		{"union absorbs subset member with narrower type", schemaevolution.Column{Type: "UNION(a VARCHAR, b BIGINT)"}, schemaevolution.Column{Type: "UNION(b INTEGER)"}, schemaevolution.TypeChangeNone, ""},
		{"union rejects unknown member tag", schemaevolution.Column{Type: "UNION(a VARCHAR)"}, schemaevolution.Column{Type: `UNION(z "NULL")`}, schemaevolution.TypeChangeIncompatible, ""},

		// Primary key / unique constraints
		{"primary key widening rejected", schemaevolution.Column{Type: "INTEGER", PrimaryKey: true}, schemaevolution.Column{Type: "BIGINT"}, schemaevolution.TypeChangeIncompatible, ""},
		{"unique column widening rejected", schemaevolution.Column{Type: "INTEGER", Unique: true}, schemaevolution.Column{Type: "BIGINT"}, schemaevolution.TypeChangeIncompatible, ""},
		{"primary key unchanged type allowed", schemaevolution.Column{Type: "INTEGER", PrimaryKey: true}, schemaevolution.Column{Type: "INTEGER"}, schemaevolution.TypeChangeNone, ""},

		// Incompatible cross-family changes
		{"int to varchar incompatible", schemaevolution.Column{Type: "INTEGER"}, schemaevolution.Column{Type: "VARCHAR"}, schemaevolution.TypeChangeIncompatible, ""},
		{"boolean to int incompatible", schemaevolution.Column{Type: "BOOLEAN"}, schemaevolution.Column{Type: "INTEGER"}, schemaevolution.TypeChangeIncompatible, ""},
		{"date to bigint incompatible", schemaevolution.Column{Type: "DATE"}, schemaevolution.Column{Type: "BIGINT"}, schemaevolution.TypeChangeIncompatible, ""},
		{"time to timestamp incompatible", schemaevolution.Column{Type: "TIME"}, schemaevolution.Column{Type: "TIMESTAMP"}, schemaevolution.TypeChangeIncompatible, ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			gotRes, gotTarget, _ := d.ResolveTypeChange(tt.existing, tt.incoming)
			assert.Equal(t, tt.wantRes, gotRes)
			assert.Equal(t, tt.wantTarget, gotTarget)
		})
	}
}

func TestDuckDBDialect_PreservesEnumCasingAndMetadata(t *testing.T) {
	t.Parallel()

	d := newDuckDBDialect(false, false, false)

	col, reason, ok := d.ValidateNewColumn(schemaevolution.Column{
		Name:      "mood",
		Type:      "ENUM('sad', 'Happy')",
		Default:   "'sad'",
		Collation: "en_US",
	})
	require.True(t, ok, reason)
	assert.Equal(t, "ENUM('sad', 'Happy')", col.Type)
	assert.Equal(t, []string{`ALTER TABLE demo.t ADD COLUMN "mood" ENUM('sad', 'Happy') COLLATE en_US DEFAULT 'sad'`}, d.AddColumnStatements("demo.t", col))

	volatileCol, reason, ok := d.ValidateNewColumn(schemaevolution.Column{
		Name:    "created_at",
		Type:    "TIMESTAMP",
		Default: "now()",
	})
	require.True(t, ok, reason)
	assert.Equal(t, []string{
		`ALTER TABLE demo.t ADD COLUMN "created_at" TIMESTAMP`,
		`ALTER TABLE demo.t ALTER COLUMN "created_at" SET DEFAULT now()`,
	}, d.AddColumnStatements("demo.t", volatileCol))

	_, emptyReason, ok := d.ValidateNewColumn(schemaevolution.Column{Name: "missing_type", Type: ""})
	assert.False(t, ok)
	assert.Contains(t, emptyReason, "non-empty SQL type")

	listCol, reason, ok := d.ValidateNewColumn(schemaevolution.Column{
		Name: "moods",
		Type: "ENUM('sad', 'Happy')[]",
	})
	require.True(t, ok, reason)
	assert.Equal(t, "ENUM('sad', 'Happy')[]", listCol.Type)

	dottedCol, reason, ok := d.ValidateNewColumn(schemaevolution.Column{Name: "user.id", Type: "BIGINT"})
	require.True(t, ok, reason)
	assert.Equal(t, []string{`ALTER TABLE demo.t ADD COLUMN "user.id" BIGINT`}, d.AddColumnStatements("demo.t", dottedCol))
	assert.Equal(t, `ALTER TABLE demo.t ALTER COLUMN "user.id" SET DATA TYPE BIGINT`, d.AlterColumnTypeSQL("demo.t", schemaevolution.Action{
		ColumnName: "user.id",
		ToType:     "BIGINT",
	}))
}

func TestDuckDBDialect_DuckLakeMode(t *testing.T) {
	t.Parallel()

	d := newDuckDBDialect(true, false, false)

	res, target, _ := d.ResolveTypeChange(schemaevolution.Column{Type: "INTEGER"}, schemaevolution.Column{Type: "BIGINT"})
	assert.Equal(t, schemaevolution.TypeChangeWiden, res)
	assert.Equal(t, "BIGINT", target)

	res, target, _ = d.ResolveTypeChange(schemaevolution.Column{Type: "INTEGER[3]"}, schemaevolution.Column{Type: "BIGINT[3]"})
	assert.Equal(t, schemaevolution.TypeChangeWiden, res)
	assert.Equal(t, "BIGINT[3]", target)

	res, _, _ = d.ResolveTypeChange(schemaevolution.Column{Type: "INTEGER"}, schemaevolution.Column{Type: "DECIMAL(5,0)"})
	assert.Equal(t, schemaevolution.TypeChangeNone, res)

	res, target, _ = d.ResolveTypeChange(schemaevolution.Column{Type: "FLOAT"}, schemaevolution.Column{Type: "DOUBLE"})
	assert.Equal(t, schemaevolution.TypeChangeWiden, res)
	assert.Equal(t, "DOUBLE", target)

	for _, tc := range []struct {
		cur, inc string
	}{
		{"DATE", "TIMESTAMP"},
		{"INTEGER", "DOUBLE"},
		{"DECIMAL(10,2)", "DECIMAL(12,2)"},
		{"UINTEGER", "BIGINT"},
		{"TIMESTAMP", "TIMESTAMPTZ"},
	} {
		res, _, reason := d.ResolveTypeChange(schemaevolution.Column{Type: tc.cur}, schemaevolution.Column{Type: tc.inc})
		assert.Equal(t, schemaevolution.TypeChangeIncompatible, res, "%s -> %s", tc.cur, tc.inc)
		assert.Contains(t, reason, "DuckLake")
	}

	_, reason, ok := d.ValidateNewColumn(schemaevolution.Column{Name: "ts", Type: "TIMESTAMP", Default: "now()"})
	assert.False(t, ok)
	assert.Contains(t, reason, "DuckLake")

	_, _, ok = d.ValidateNewColumn(schemaevolution.Column{Name: "status", Type: "VARCHAR", Default: "'active'"})
	assert.True(t, ok)
}

func TestSpliceDuckDBAlterStatements(t *testing.T) {
	t.Parallel()

	alters := []string{`ALTER TABLE t ADD COLUMN "x" BIGINT`}

	rewritten := "BEGIN; CREATE TEMP TABLE tmp AS SELECT 1; INSERT INTO t BY NAME SELECT * FROM tmp; COMMIT"
	got := schemaevolution.SpliceStatementsInTransaction(duckDBSQLDialect, rewritten, alters)
	assert.Equal(t, "BEGIN;\nALTER TABLE t ADD COLUMN \"x\" BIGINT;\nCREATE TEMP TABLE tmp AS SELECT 1; INSERT INTO t BY NAME SELECT * FROM tmp; COMMIT", got)

	startTx := "START TRANSACTION; INSERT INTO t VALUES (1); COMMIT;"
	got = schemaevolution.SpliceStatementsInTransaction(duckDBSQLDialect, startTx, alters)
	assert.Equal(t, "START TRANSACTION;\nALTER TABLE t ADD COLUMN \"x\" BIGINT;\nINSERT INTO t VALUES (1); COMMIT;", got)
}
