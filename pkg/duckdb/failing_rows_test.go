//go:build !bruin_no_duckdb

package duck

import (
	"context"
	"path/filepath"
	"strings"
	"testing"

	"github.com/bruin-data/bruin/pkg/ansisql"
	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/bruin-data/bruin/pkg/query"
	"github.com/bruin-data/bruin/pkg/scheduler"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type singleConnection struct{ conn any }

func (s singleConnection) GetConnection(string) any { return s.conn }

func TestChecksShowFailingRows(t *testing.T) {
	t.Parallel()

	client, err := NewClient(Config{Path: filepath.Join(t.TempDir(), "checks.duckdb")})
	require.NoError(t, err)
	defer client.Close()
	require.NoError(t, client.RunQueryWithoutResult(t.Context(), &query.Query{Query: `CREATE TABLE items AS SELECT * FROM (VALUES
		(1, 'ok', 2),
		(NULL, 'a description that is much longer than fifty characters', 3),
		(3, 'dup', 1),
		(3, 'dup again', 1)
	) AS t(id, name, quantity)`}))
	require.NoError(t, client.RunQueryWithoutResult(t.Context(), &query.Query{Query: "CREATE TABLE many AS SELECT range AS n FROM range(7)"}))

	conn := singleConnection{conn: client}
	p := &pipeline.Pipeline{Name: "p", DefaultConnections: map[string]string{"duckdb": "duckdb-default"}}
	columnCheck := func(assetName, column string) *scheduler.ColumnCheckInstance {
		return &scheduler.ColumnCheckInstance{
			AssetInstance: &scheduler.AssetInstance{Asset: &pipeline.Asset{Name: assetName, Type: pipeline.AssetTypeDuckDBQuery}, Pipeline: p},
			Column:        &pipeline.Column{Name: column},
			Check:         &pipeline.ColumnCheck{},
		}
	}
	samples := ansisql.WithFailingRowSamples(context.Background(), 5)

	t.Run("not_null shows the whole null row with long values cut", func(t *testing.T) {
		t.Parallel()
		err := ansisql.NewNotNullCheck(conn).Check(samples, columnCheck("items", "id"))
		assert.EqualError(t, err, `column 'id' has 1 null values
failing rows:
+------+----------------------------------------------------+----------+
| id   | name                                               | quantity |
+------+----------------------------------------------------+----------+
| NULL | a description that is much longer than fifty ch... | 3        |
+------+----------------------------------------------------+----------+`)
	})

	t.Run("without the option only the count is shown", func(t *testing.T) {
		t.Parallel()
		err := ansisql.NewNotNullCheck(conn).Check(context.Background(), columnCheck("items", "id"))
		assert.EqualError(t, err, "column 'id' has 1 null values")
	})

	t.Run("unique shows the repeated values", func(t *testing.T) {
		t.Parallel()
		err := ansisql.NewUniqueCheck(conn).Check(samples, columnCheck("items", "id"))
		assert.EqualError(t, err, `column 'id' has 1 non-unique values
failing rows:
+----+-----------+
| id | row_count |
+----+-----------+
| 3  | 2         |
+----+-----------+`)
	})

	t.Run("count custom check shows at most the limit of its rows", func(t *testing.T) {
		t.Parallel()
		count := int64(0)
		err := ansisql.NewCustomCheck(conn, nil).Check(samples, &scheduler.CustomCheckInstance{
			AssetInstance: &scheduler.AssetInstance{Asset: &pipeline.Asset{Name: "many", Type: pipeline.AssetTypeDuckDBQuery}, Pipeline: p},
			Check:         &pipeline.CustomCheck{Name: "no_rows", Query: "SELECT n FROM many", Count: &count},
		})
		require.Error(t, err)
		lines := strings.Split(err.Error(), "\n")
		assert.Equal(t, "custom check 'no_rows' has returned 7 instead of the expected 0", lines[0])
		assert.Equal(t, "first 5 failing rows:", lines[1])
		// Title, count line, three border lines, a header, and 5 rows.
		assert.Len(t, lines, 11)
	})

	t.Run("value custom check shows no rows", func(t *testing.T) {
		t.Parallel()
		err := ansisql.NewCustomCheck(conn, nil).Check(samples, &scheduler.CustomCheckInstance{
			AssetInstance: &scheduler.AssetInstance{Asset: &pipeline.Asset{Name: "items", Type: pipeline.AssetTypeDuckDBQuery}, Pipeline: p},
			Check:         &pipeline.CustomCheck{Name: "total", Query: "SELECT count(*) FROM items", Value: "0"},
		})
		assert.EqualError(t, err, "custom check 'total' has returned 4 instead of the expected 0")
	})
}
