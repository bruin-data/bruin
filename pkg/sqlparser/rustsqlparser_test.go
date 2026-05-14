//go:build cgo && (darwin || linux)

package sqlparser

import (
	"testing"

	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/stretchr/testify/require"
)

func TestRustSQLParserSmoke(t *testing.T) {
	t.Parallel()

	parser, err := NewRustSQLParser(false)
	require.NoError(t, err)

	t.Cleanup(func() {
		require.NoError(t, parser.Close())
	})

	lineage, err := parser.ColumnLineage(
		"SELECT IF(col1 IS NOT NULL, 1, 0) AS x FROM t",
		"bigquery",
		Schema{"t": {"col1": "STRING"}},
	)
	require.NoError(t, err)
	require.Equal(
		t,
		[]ColumnLineage{
			{
				Name: "x",
				Upstream: []UpstreamColumn{
					{Column: "col1", Table: "t"},
				},
				Type: "INT",
			},
		},
		lineage.Columns,
	)
	require.Empty(t, lineage.Errors)

	tables, err := parser.UsedTables(
		"WITH base AS (SELECT * FROM raw.my_cte) SELECT * FROM base",
		"bigquery",
	)
	require.NoError(t, err)
	require.Equal(t, []string{"raw.my_cte"}, tables)
}

func TestRustSQLParser_HoistDeclares(t *testing.T) {
	t.Parallel()

	parser, err := NewRustSQLParser(false)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, parser.Close()) })
	require.NoError(t, parser.Start())

	t.Run("no declare is a no-op", func(t *testing.T) {
		t.Parallel()
		got, err := parser.HoistDeclares("SELECT 1; SELECT 2;", pipeline.AssetTypeBigqueryQuery)
		require.NoError(t, err)
		require.Equal(t, "SELECT 1; SELECT 2;", got)
	})

	t.Run("already-ordered declare returns input verbatim", func(t *testing.T) {
		t.Parallel()
		in := "DECLARE x INT64;\nSELECT 1;"
		got, err := parser.HoistDeclares(in, pipeline.AssetTypeBigqueryQuery)
		require.NoError(t, err)
		require.Equal(t, in, got)
	})

	t.Run("declare after non-declare gets hoisted with original text preserved", func(t *testing.T) {
		t.Parallel()
		in := "SET x = 1;\nDECLARE y INT64;\nSELECT 1;"
		got, err := parser.HoistDeclares(in, pipeline.AssetTypeBigqueryQuery)
		require.NoError(t, err)
		// Each statement's original text is preserved verbatim — only order
		// and the ';\n' separator are rewritten.
		require.Equal(t, "DECLARE y INT64;\nSET x = 1;\nSELECT 1;", got)
	})

	t.Run("declare keyword inside string literal does not trigger reordering", func(t *testing.T) {
		t.Parallel()
		in := "SELECT 'declare bankruptcy' AS msg;"
		got, err := parser.HoistDeclares(in, pipeline.AssetTypeBigqueryQuery)
		require.NoError(t, err)
		require.Equal(t, in, got)
	})

	t.Run("semicolon inside string literal does not split", func(t *testing.T) {
		t.Parallel()
		in := "SET separator = ';';\nDECLARE y INT64;"
		got, err := parser.HoistDeclares(in, pipeline.AssetTypeBigqueryQuery)
		require.NoError(t, err)
		require.Equal(t, "DECLARE y INT64;\nSET separator = ';';", got)
	})

	t.Run("declare inside BEGIN..END block is not hoisted", func(t *testing.T) {
		t.Parallel()
		in := "SET x = 1;\nBEGIN\n  DECLARE y INT64;\n  SELECT y;\nEND;"
		got, err := parser.HoistDeclares(in, pipeline.AssetTypeBigqueryQuery)
		require.NoError(t, err)
		require.Equal(t, in, got)
	})

	t.Run("CASE..END inside BEGIN body does not leak DECLAREs", func(t *testing.T) {
		t.Parallel()
		// CASE shares its closing END token with BEGIN. Without per-construct
		// depth tracking, the CASE's END would prematurely close the BEGIN
		// block and the inner DECLARE would be hoisted out, breaking the
		// stored-procedure body.
		in := "SET x = 1;\nBEGIN\n  SELECT CASE WHEN x>0 THEN 'a' ELSE 'b' END;\n  DECLARE y INT64;\nEND;"
		got, err := parser.HoistDeclares(in, pipeline.AssetTypeBigqueryQuery)
		require.NoError(t, err)
		require.Equal(t, in, got)
	})

	t.Run("leading comment is preserved with its statement", func(t *testing.T) {
		t.Parallel()
		in := "SET x = 1;\n-- setup\nDECLARE y INT64;"
		got, err := parser.HoistDeclares(in, pipeline.AssetTypeBigqueryQuery)
		require.NoError(t, err)
		require.Equal(t, "-- setup\nDECLARE y INT64;\nSET x = 1;", got)
	})

	t.Run("array type syntax preserved verbatim", func(t *testing.T) {
		t.Parallel()
		// `array<STRING>` lower-case casing must survive — we slice the
		// original text rather than regenerating from the AST.
		in := "SET x = 1;\nDECLARE distinct_keys array<STRING>;"
		got, err := parser.HoistDeclares(in, pipeline.AssetTypeBigqueryQuery)
		require.NoError(t, err)
		require.Equal(t, "DECLARE distinct_keys array<STRING>;\nSET x = 1;", got)
	})

	t.Run("unmapped asset type returns error and input unchanged", func(t *testing.T) {
		t.Parallel()
		in := "SET x = 1;\nDECLARE y INT64;"
		got, err := parser.HoistDeclares(in, pipeline.AssetTypePython)
		require.Error(t, err)
		require.Equal(t, in, got)
	})
}

func TestRustSQLParser_HoistDeclaresList(t *testing.T) {
	t.Parallel()

	parser, err := NewRustSQLParser(false)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, parser.Close()) })
	require.NoError(t, parser.Start())

	t.Run("no declare is a no-op", func(t *testing.T) {
		t.Parallel()
		in := []string{"SELECT 1", "SELECT 2"}
		got, err := parser.HoistDeclaresList(in, pipeline.AssetTypeBigqueryQuery)
		require.NoError(t, err)
		require.Equal(t, in, got)
	})

	t.Run("declare after non-declare gets hoisted while preserving text", func(t *testing.T) {
		t.Parallel()
		in := []string{"SET x = 1", "DECLARE y INT64", "SELECT 1"}
		got, err := parser.HoistDeclaresList(in, pipeline.AssetTypeBigqueryQuery)
		require.NoError(t, err)
		require.Equal(t, []string{"DECLARE y INT64", "SET x = 1", "SELECT 1"}, got)
	})
}
