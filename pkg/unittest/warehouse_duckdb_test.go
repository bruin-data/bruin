//go:build !bruin_no_duckdb

package unittest_test

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	duck "github.com/bruin-data/bruin/pkg/duckdb"
	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/bruin-data/bruin/pkg/query"
	"github.com/bruin-data/bruin/pkg/sqlparser"
	"github.com/bruin-data/bruin/pkg/unittest"
	"github.com/stretchr/testify/require"
)

// TestBuildWarehouseQuery_RunsOnDuckDB proves the CTE-injected, read-only query
// is semantically correct by executing it on DuckDB (standing in for a real
// warehouse): the asset's reads are satisfied entirely by inline fixtures, and
// nothing but a SELECT is run.
func TestBuildWarehouseQuery_RunsOnDuckDB(t *testing.T) {
	t.Parallel()

	parser, err := sqlparser.NewSQLParser(false)
	require.NoError(t, err)
	require.NoError(t, parser.Start())
	defer parser.Close()

	run := func(t *testing.T, sql string) *query.QueryResult {
		t.Helper()
		dir := t.TempDir()
		client, err := duck.NewClient(duck.Config{Path: filepath.Join(dir, "ut.duckdb")})
		require.NoError(t, err)
		res, err := client.SelectWithSchema(context.Background(), &query.Query{Query: sql})
		require.NoError(t, err)
		return res
	}

	orders := pipeline.UnitTestInput{
		Asset: "analytics.orders",
		Rows: []map[string]interface{}{
			{"id": 1, "status": "paid", "amount": 100},
			{"id": 2, "status": "refunded", "amount": 999},
		},
	}

	t.Run("fixture CTE substitutes the real table", func(t *testing.T) {
		t.Parallel()
		sql, err := unittest.BuildWarehouseQuery(parser, "duckdb",
			"SELECT CAST(SUM(amount) AS BIGINT) AS revenue FROM analytics.orders WHERE status = 'paid'",
			pipeline.UnitTest{Inputs: []pipeline.UnitTestInput{orders}}, nil)
		require.NoError(t, err)

		res := run(t, sql)
		require.Len(t, res.Rows, 1)
		require.EqualValues(t, 100, res.Rows[0][0])
	})

	t.Run("unmocked upstream is an empty typed CTE so a LEFT JOIN yields only mocked rows", func(t *testing.T) {
		t.Parallel()
		schemas := map[string][]pipeline.Column{
			"analytics.dim": {{Name: "id", Type: "BIGINT"}, {Name: "label", Type: "VARCHAR"}},
		}
		sql, err := unittest.BuildWarehouseQuery(parser, "duckdb",
			"SELECT o.id, d.label FROM analytics.orders o LEFT JOIN analytics.dim d ON d.id = o.id ORDER BY o.id",
			pipeline.UnitTest{Inputs: []pipeline.UnitTestInput{orders}}, schemas)
		require.NoError(t, err)

		res := run(t, sql)
		// Both orders survive the LEFT JOIN; label is NULL because dim is empty.
		require.Len(t, res.Rows, 2)
		require.Nil(t, res.Rows[0][1])
	})

	t.Run("a named fixture supplies a shared input that the asset joins against", func(t *testing.T) {
		t.Parallel()
		// The currency lookup is a shared fixture; the test supplies only its own
		// orders. Resolving the fixture turns it into a second input, so the join
		// finds a matching rate.
		fixtures := []pipeline.Fixture{{
			Name:  "base_currency",
			Asset: "analytics.currency",
			Rows:  []map[string]interface{}{{"code": "USD", "rate": 2}},
		}}
		test := pipeline.UnitTest{
			Inputs: []pipeline.UnitTestInput{{
				Asset: "analytics.orders",
				Rows:  []map[string]interface{}{{"id": 1, "amount": 50, "code": "USD"}},
			}},
			Fixtures: []string{"base_currency"},
		}
		inputs, err := unittest.ResolveFixtures(fixtures, test)
		require.NoError(t, err)
		test.Inputs = inputs

		sql, err := unittest.BuildWarehouseQuery(parser, "duckdb",
			"SELECT o.id, o.amount * c.rate AS converted FROM analytics.orders o "+
				"JOIN analytics.currency c ON c.code = o.code",
			test, nil)
		require.NoError(t, err)

		res := run(t, sql)
		require.Len(t, res.Rows, 1)
		require.EqualValues(t, 100, res.Rows[0][1])
	})

	t.Run("recursive CTE asset runs against fixtures", func(t *testing.T) {
		t.Parallel()
		// Wrapping a recursive query as SELECT * FROM (WITH RECURSIVE ...) is
		// rejected by some engines. We splice fixtures into the existing WITH and
		// rewrite the source table in place, so the recursive asset runs unchanged
		// as one statement.
		nodes := pipeline.UnitTestInput{
			Asset: "analytics.nodes",
			Rows: []map[string]interface{}{
				{"id": 1, "parent_id": nil, "name": "root"},
				{"id": 2, "parent_id": 1, "name": "child-a"},
				{"id": 3, "parent_id": 1, "name": "child-b"},
				{"id": 4, "parent_id": 2, "name": "grandchild"},
			},
		}
		schemas := map[string][]pipeline.Column{
			"analytics.nodes": {
				{Name: "id", Type: "BIGINT"},
				{Name: "parent_id", Type: "BIGINT"},
				{Name: "name", Type: "VARCHAR"},
			},
		}
		const assetSQL = "WITH RECURSIVE descendants AS (" +
			"SELECT id, parent_id, name FROM analytics.nodes WHERE parent_id IS NULL " +
			"UNION ALL " +
			"SELECT n.id, n.parent_id, n.name FROM analytics.nodes n JOIN descendants d ON n.parent_id = d.id" +
			") SELECT name FROM descendants ORDER BY id"

		sql, err := unittest.BuildWarehouseQuery(parser, "duckdb", assetSQL,
			pipeline.UnitTest{Inputs: []pipeline.UnitTestInput{nodes}}, schemas)
		require.NoError(t, err)
		require.Contains(t, strings.ToUpper(sql), "RECURSIVE", "the RECURSIVE keyword must survive the rewrite")

		res := run(t, sql)
		// The whole tree is reachable from the single root, so all four rows return.
		require.Len(t, res.Rows, 4)
	})

	t.Run("a materialization:none asset (CREATE VIEW AS SELECT) tests its inner SELECT", func(t *testing.T) {
		t.Parallel()
		// The asset body is full DDL. The builder reduces it to the inner SELECT,
		// so the test runs read-only against the fixture and never creates a view.
		sql, err := unittest.BuildWarehouseQuery(parser, "duckdb",
			"CREATE OR REPLACE VIEW analytics.revenue AS "+
				"SELECT status, SUM(amount) AS revenue FROM analytics.orders WHERE status = 'paid' GROUP BY status",
			pipeline.UnitTest{Inputs: []pipeline.UnitTestInput{orders}}, nil)
		require.NoError(t, err)

		res := run(t, sql)
		require.Len(t, res.Rows, 1)
		require.EqualValues(t, 100, res.Rows[0][1])
	})

	t.Run("an incremental SELECT reading its own target table tests the read logic", func(t *testing.T) {
		t.Parallel()
		// An incremental asset's SQL is a SELECT that often reads {{ this }} (the
		// target) for the high-water mark; after rendering, that target is just
		// another table the test mocks as an input. No materialization is run —
		// only the SELECT logic is exercised, read-only.
		incomingOrders := pipeline.UnitTestInput{
			Asset: "analytics.orders",
			Rows: []map[string]interface{}{
				{"id": 1, "amount": 10},
				{"id": 2, "amount": 20},
				{"id": 3, "amount": 30},
			},
		}
		priorState := pipeline.UnitTestInput{
			Asset: "analytics.revenue",
			Rows:  []map[string]interface{}{{"id": 1, "amount": 10}},
		}
		sql, err := unittest.BuildWarehouseQuery(parser, "duckdb",
			"SELECT id, amount FROM analytics.orders "+
				"WHERE id > (SELECT COALESCE(MAX(id), 0) FROM analytics.revenue) ORDER BY id",
			pipeline.UnitTest{Inputs: []pipeline.UnitTestInput{incomingOrders, priorState}}, nil)
		require.NoError(t, err)

		res := run(t, sql)
		// Only rows newer than the prior max id (1) are selected: ids 2 and 3.
		require.Len(t, res.Rows, 2)
		require.EqualValues(t, 2, res.Rows[0][0])
		require.EqualValues(t, 3, res.Rows[1][0])
	})

	t.Run("a write asset (DELETE) is rejected before any query is produced", func(t *testing.T) {
		t.Parallel()
		// The read-only guarantee: a write statement can never be turned into a
		// runnable query, so it never reaches the connection.
		_, err := unittest.BuildWarehouseQuery(parser, "duckdb",
			"DELETE FROM analytics.orders WHERE status = 'refunded'",
			pipeline.UnitTest{Inputs: []pipeline.UnitTestInput{orders}}, nil)
		require.Error(t, err)
	})

	t.Run("a CTE-level query returns an intermediate CTE's rows against fixtures", func(t *testing.T) {
		t.Parallel()
		// The model filters paid orders into a CTE, then aggregates. The CTE
		// query asserts that intermediate step rather than the final SUM.
		const assetSQL = "WITH paid AS (SELECT id, amount FROM analytics.orders WHERE status = 'paid') " +
			"SELECT SUM(amount) AS revenue FROM paid"
		base, err := unittest.BuildWarehouseQuery(parser, "duckdb", assetSQL,
			pipeline.UnitTest{Inputs: []pipeline.UnitTestInput{orders}}, nil)
		require.NoError(t, err)
		sql, err := parser.SelectFromCTE(base, "duckdb", "paid")
		require.NoError(t, err)

		res := run(t, sql)
		require.Len(t, res.Rows, 1) // only the paid order survives the CTE filter
		require.EqualValues(t, 1, res.Rows[0][0])
	})

	t.Run("execution_time freezes CURRENT_TIMESTAMP to a fixed value", func(t *testing.T) {
		t.Parallel()
		sql, err := unittest.BuildWarehouseQuery(parser, "duckdb",
			"SELECT CAST(CURRENT_TIMESTAMP AS VARCHAR) AS ts",
			pipeline.UnitTest{ExecutionTime: "2023-01-01 12:05:03"}, nil)
		require.NoError(t, err)

		res := run(t, sql)
		require.Len(t, res.Rows, 1)
		require.Contains(t, fmt.Sprint(res.Rows[0][0]), "2023-01-01 12:05:03")
	})
}
