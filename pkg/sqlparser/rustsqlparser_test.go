//go:build cgo && (darwin || linux)

package sqlparser

import (
	"testing"

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
