//go:build !bruin_no_duckdb

package duck

import (
	"bytes"
	"context"
	"path/filepath"
	"strings"
	"testing"

	"github.com/bruin-data/bruin/pkg/executor"
	"github.com/bruin-data/bruin/pkg/jinja"
	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/bruin-data/bruin/pkg/query"
	"github.com/bruin-data/bruin/pkg/schemaevolution"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type fakeDevEnvModifier struct {
	rewrite func(sql string) string
}

func (f fakeDevEnvModifier) Modify(ctx context.Context, p *pipeline.Pipeline, a *pipeline.Asset, q *query.Query) (*query.Query, error) {
	return &query.Query{Query: f.rewrite(q.Query)}, nil
}

func (f fakeDevEnvModifier) RegisterAssetForSchemaCache(ctx context.Context, p *pipeline.Pipeline, a *pipeline.Asset, q *query.Query) error {
	return nil
}

type staticConnectionGetter struct {
	conn any
}

func (s staticConnectionGetter) GetConnection(name string) any {
	return s.conn
}

func newDuckDBEvolutionEngine(client DuckDBClient) *duckDBEvolutionEngine {
	return &duckDBEvolutionEngine{
		DuckDBDialect: newDuckDBDialect(false, false, false),
		session:       &clientFallbackSession{client: client},
	}
}

func newSchemaEvolutionTestOperator(t *testing.T, client *Client, fullRefresh bool) *BasicOperator {
	t.Helper()
	return &BasicOperator{
		connection: staticConnectionGetter{conn: client},
		extractor: &query.WholeFileExtractor{
			Renderer: jinja.NewRendererWithYesterday("test-pipeline", "test-run"),
		},
		materializer: pipeline.HookWrapperMaterializer{
			Mat: NewMaterializer(fullRefresh),
		},
	}
}

func newSchemaEvolutionTestClient(t *testing.T) *Client {
	t.Helper()
	client, err := NewClient(Config{Path: filepath.Join(t.TempDir(), "evolve.duckdb")})
	require.NoError(t, err)
	t.Cleanup(client.Close)
	return client
}

func columnNames(cols []schemaevolution.Column) []string {
	out := make([]string, len(cols))
	for i, c := range cols {
		out[i] = c.Name
	}
	return out
}

func columnTypes(cols []schemaevolution.Column) []string {
	out := make([]string, len(cols))
	for i, c := range cols {
		out[i] = c.Type
	}
	return out
}

func TestSchemaEvolution_DuckDB_CoreStrategies(t *testing.T) {
	t.Parallel()
	p := &pipeline.Pipeline{Name: "test-pipeline"}

	t.Run("append: adds new columns, widens types, preserves omitted columns, handles reordered columns", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()
		client := newSchemaEvolutionTestClient(t)
		op := newSchemaEvolutionTestOperator(t, client, false)

		require.NoError(t, client.RunQueryWithoutResult(ctx, &query.Query{
			Query: "CREATE SCHEMA demo; CREATE TABLE demo.events (id INTEGER, amount DECIMAL(10,2), legacy_note VARCHAR, created_at DATE);",
		}))
		require.NoError(t, client.RunQueryWithoutResult(ctx, &query.Query{
			Query: "INSERT INTO demo.events VALUES (1, 12.34, 'initial', DATE '2026-01-01');",
		}))

		var logBuf bytes.Buffer
		runCtx := context.WithValue(ctx, executor.KeyPrinter, &logBuf)

		asset := &pipeline.Asset{
			Name:            "demo.events",
			Type:            pipeline.AssetTypeDuckDBQuery,
			SchemaEvolution: &pipeline.SchemaEvolution{Enabled: boolPtr(true)},
			Materialization: pipeline.Materialization{
				Type:     pipeline.MaterializationTypeTable,
				Strategy: pipeline.MaterializationStrategyAppend,
			},
			ExecutableFile: pipeline.ExecutableFile{
				Content: `
WITH new_rows AS (
	SELECT
		TIMESTAMP '2026-01-02 10:30:00' AS created_at,
		CAST(9000000000 AS BIGINT) AS id,
		CAST(99.1234 AS DECIMAL(10,4)) AS amount,
		CAST(42.5 AS DOUBLE) AS "order"
)
SELECT * FROM new_rows;
`,
			},
		}

		require.NoError(t, op.RunTask(runCtx, p, asset))

		cols, exists, err := newDuckDBEvolutionEngine(client).ExistingColumns(ctx, "demo.events")
		require.NoError(t, err)
		require.True(t, exists)
		assert.Equal(t, []string{"id", "amount", "legacy_note", "created_at", "order"}, columnNames(cols))
		assert.Equal(t, []string{"BIGINT", "DECIMAL(12,4)", "VARCHAR", "TIMESTAMP", "DOUBLE"}, columnTypes(cols))

		rows, err := client.Select(ctx, &query.Query{
			Query: `SELECT id, CAST(amount AS VARCHAR), legacy_note, CAST(created_at AS VARCHAR), "order" FROM demo.events ORDER BY id`,
		})
		require.NoError(t, err)
		assert.Equal(t, [][]interface{}{
			{int64(1), "12.3400", "initial", "2026-01-01 00:00:00", nil},
			{int64(9000000000), "99.1234", nil, "2026-01-02 10:30:00", float64(42.5)},
		}, rows)

		assert.Contains(t, logBuf.String(), `Schema evolution on demo.events:`)
		assert.Contains(t, logBuf.String(), `widening column "id" (INTEGER -> BIGINT)`)
		assert.Contains(t, logBuf.String(), `adding column "order" (DOUBLE)`)

		// Narrower incoming type (INTEGER into BIGINT) is a no-op and succeeds.
		asset.ExecutableFile.Content = `SELECT 2 AS id, CAST(5.00 AS DECIMAL(6,2)) AS amount`
		require.NoError(t, op.RunTask(ctx, p, asset))
	})

	t.Run("delete+insert: evolves schema and preserves omitted columns on untouched rows", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()
		client := newSchemaEvolutionTestClient(t)
		op := newSchemaEvolutionTestOperator(t, client, false)

		require.NoError(t, client.RunQueryWithoutResult(ctx, &query.Query{
			Query: "CREATE SCHEMA demo; CREATE TABLE demo.metrics (batch_id INTEGER, score FLOAT, old_col VARCHAR);",
		}))
		require.NoError(t, client.RunQueryWithoutResult(ctx, &query.Query{
			Query: "INSERT INTO demo.metrics VALUES (1, 1.5, 'keep-me'), (2, 2.5, 'replace-me');",
		}))

		asset := &pipeline.Asset{
			Name:            "demo.metrics",
			Type:            pipeline.AssetTypeDuckDBQuery,
			SchemaEvolution: &pipeline.SchemaEvolution{Enabled: boolPtr(true)},
			Materialization: pipeline.Materialization{
				Type:           pipeline.MaterializationTypeTable,
				Strategy:       pipeline.MaterializationStrategyDeleteInsert,
				IncrementalKey: "batch_id",
			},
			ExecutableFile: pipeline.ExecutableFile{
				Content: "SELECT CAST(2 AS BIGINT) AS batch_id, CAST(2.75 AS DOUBLE) AS score, 'v2' AS tag",
			},
		}

		require.NoError(t, op.RunTask(ctx, p, asset))

		rows, err := client.Select(ctx, &query.Query{
			Query: "SELECT batch_id, score, old_col, tag FROM demo.metrics ORDER BY batch_id",
		})
		require.NoError(t, err)
		assert.Equal(t, [][]interface{}{
			{int64(1), float64(1.5), "keep-me", nil},
			{int64(2), float64(2.75), nil, "v2"},
		}, rows)
	})

	t.Run("truncate+insert: evolves schema and never drops omitted columns", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()
		client := newSchemaEvolutionTestClient(t)
		op := newSchemaEvolutionTestOperator(t, client, false)

		require.NoError(t, client.RunQueryWithoutResult(ctx, &query.Query{
			Query: "CREATE SCHEMA demo; CREATE TABLE demo.snapshot (id INTEGER, removed_col VARCHAR);",
		}))
		require.NoError(t, client.RunQueryWithoutResult(ctx, &query.Query{
			Query: "INSERT INTO demo.snapshot VALUES (1, 'old');",
		}))

		asset := &pipeline.Asset{
			Name:            "demo.snapshot",
			Type:            pipeline.AssetTypeDuckDBQuery,
			SchemaEvolution: &pipeline.SchemaEvolution{Enabled: boolPtr(true)},
			Materialization: pipeline.Materialization{
				Type:     pipeline.MaterializationTypeTable,
				Strategy: pipeline.MaterializationStrategyTruncateInsert,
			},
			ExecutableFile: pipeline.ExecutableFile{
				Content: "SELECT CAST(10 AS BIGINT) AS id, 'fresh' AS new_col",
			},
		}

		require.NoError(t, op.RunTask(ctx, p, asset))

		rows, err := client.Select(ctx, &query.Query{
			Query: "SELECT id, removed_col, new_col FROM demo.snapshot",
		})
		require.NoError(t, err)
		assert.Equal(t, [][]interface{}{{int64(10), nil, "fresh"}}, rows)
	})

	t.Run("time_interval: evolves schema with new column and widened type", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()
		client := newSchemaEvolutionTestClient(t)
		op := newSchemaEvolutionTestOperator(t, client, false)

		require.NoError(t, client.RunQueryWithoutResult(ctx, &query.Query{
			Query: "CREATE SCHEMA demo; CREATE TABLE demo.daily (dt DATE, total INTEGER);",
		}))
		require.NoError(t, client.RunQueryWithoutResult(ctx, &query.Query{
			Query: "INSERT INTO demo.daily VALUES (DATE '2020-01-01', 100);",
		}))

		asset := &pipeline.Asset{
			Name:            "demo.daily",
			Type:            pipeline.AssetTypeDuckDBQuery,
			SchemaEvolution: &pipeline.SchemaEvolution{Enabled: boolPtr(true)},
			Materialization: pipeline.Materialization{
				Type:            pipeline.MaterializationTypeTable,
				Strategy:        pipeline.MaterializationStrategyTimeInterval,
				IncrementalKey:  "dt",
				TimeGranularity: pipeline.MaterializationTimeGranularityDate,
			},
			ExecutableFile: pipeline.ExecutableFile{
				Content: "SELECT DATE '{{ start_date }}' AS dt, CAST(5000000000 AS BIGINT) AS total, 'usd' AS currency",
			},
		}

		require.NoError(t, op.RunTask(ctx, p, asset))

		cols, _, err := newDuckDBEvolutionEngine(client).ExistingColumns(ctx, "demo.daily")
		require.NoError(t, err)
		assert.Equal(t, []string{"dt", "total", "currency"}, columnNames(cols))
		assert.Equal(t, []string{"DATE", "BIGINT", "VARCHAR"}, columnTypes(cols))
	})

	t.Run("merge: evolves only declared asset columns, ignores undeclared query columns, and tolerates free-text doc types", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()
		client := newSchemaEvolutionTestClient(t)
		op := newSchemaEvolutionTestOperator(t, client, false)

		require.NoError(t, client.RunQueryWithoutResult(ctx, &query.Query{
			Query: "CREATE SCHEMA demo; CREATE TABLE demo.users (id INTEGER, name VARCHAR, legacy VARCHAR); INSERT INTO demo.users VALUES (1, 'Alice', 'keep');",
		}))

		asset := &pipeline.Asset{
			Name:            "demo.users",
			Type:            pipeline.AssetTypeDuckDBQuery,
			SchemaEvolution: &pipeline.SchemaEvolution{Enabled: boolPtr(true)},
			Materialization: pipeline.Materialization{
				Type:     pipeline.MaterializationTypeTable,
				Strategy: pipeline.MaterializationStrategyMerge,
			},
			Columns: []pipeline.Column{
				{Name: "id", Type: "number", PrimaryKey: true},
				{Name: "name", Type: "str", UpdateOnMerge: true},
				{Name: "email", Type: "str", UpdateOnMerge: true},
			},
			ExecutableFile: pipeline.ExecutableFile{
				Content: "SELECT CAST(1 AS BIGINT) AS id, 'Alice Updated' AS name, 'alice@example.com' AS email, 'ignored' AS extra_undeclared UNION ALL SELECT CAST(2 AS BIGINT), 'Bob', 'bob@example.com', 'ignored'",
			},
		}

		require.NoError(t, op.RunTask(ctx, p, asset))

		cols, _, err := newDuckDBEvolutionEngine(client).ExistingColumns(ctx, "demo.users")
		require.NoError(t, err)
		assert.Equal(t, []string{"id", "name", "legacy", "email"}, columnNames(cols))

		rows, err := client.Select(ctx, &query.Query{
			Query: "SELECT id, name, legacy, email FROM demo.users ORDER BY id",
		})
		require.NoError(t, err)
		assert.Equal(t, [][]interface{}{
			{int64(1), "Alice Updated", "keep", "alice@example.com"},
			{int64(2), "Bob", nil, "bob@example.com"},
		}, rows)
	})

	t.Run("scd2_by_column: evolves declared columns without adding undeclared query columns", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()
		client := newSchemaEvolutionTestClient(t)
		fullOp := newSchemaEvolutionTestOperator(t, client, true)
		incOp := newSchemaEvolutionTestOperator(t, client, false)

		require.NoError(t, client.RunQueryWithoutResult(ctx, &query.Query{
			Query: "CREATE SCHEMA demo; CREATE TABLE demo.src AS SELECT 1 AS id, 'a' AS label, 'a@example.com' AS email, 'skip' AS debug_col;",
		}))

		asset := &pipeline.Asset{
			Name:            "demo.dim_items",
			Type:            pipeline.AssetTypeDuckDBQuery,
			SchemaEvolution: &pipeline.SchemaEvolution{Enabled: boolPtr(true)},
			Materialization: pipeline.Materialization{
				Type:     pipeline.MaterializationTypeTable,
				Strategy: pipeline.MaterializationStrategySCD2ByColumn,
			},
			Columns: []pipeline.Column{
				{Name: "id", Type: "INTEGER", PrimaryKey: true},
				{Name: "label", Type: "VARCHAR"},
			},
			ExecutableFile: pipeline.ExecutableFile{
				Content: "SELECT id, label, debug_col FROM demo.src",
			},
		}
		require.NoError(t, fullOp.RunTask(ctx, p, asset))

		// Second run with no declared column change should NOT try to add debug_col.
		var logBuf bytes.Buffer
		require.NoError(t, incOp.RunTask(context.WithValue(ctx, executor.KeyPrinter, &logBuf), p, asset))
		assert.Empty(t, logBuf.String())

		// Add declared email column and widen id (also with trailing semicolon + comment on SCD2).
		asset.Columns = append(asset.Columns, pipeline.Column{Name: "email", Type: "VARCHAR"})
		asset.Columns[0].Type = "BIGINT"
		asset.ExecutableFile.Content = "SELECT CAST(id AS BIGINT) AS id, label, email, debug_col FROM demo.src; -- scd2 trailing comment"

		require.NoError(t, incOp.RunTask(ctx, p, asset))
		cols, _, err := newDuckDBEvolutionEngine(client).ExistingColumns(ctx, "demo.dim_items")
		require.NoError(t, err)
		assert.Equal(t, []string{"id", "label", "email", "_valid_from", "_valid_until", "_is_current"}, columnNames(cols))

		rows, err := client.Select(ctx, &query.Query{
			Query: "SELECT id, label, email, _is_current FROM demo.dim_items ORDER BY _is_current",
		})
		require.NoError(t, err)
		assert.Equal(t, [][]interface{}{
			{int64(1), "a", nil, false},
			{int64(1), "a", "a@example.com", true},
		}, rows)
	})

	t.Run("ddl: normalizes DuckDB type aliases, preserves volatile and literal defaults, and enforces PK/FK constraints", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()
		client := newSchemaEvolutionTestClient(t)
		op := newSchemaEvolutionTestOperator(t, client, true)

		require.NoError(t, client.RunQueryWithoutResult(ctx, &query.Query{
			Query: "CREATE SCHEMA demo; CREATE TABLE demo.ddl_tbl (pk INTEGER PRIMARY KEY, score DOUBLE, ts TIMESTAMP, meta STRUCT(a INTEGER), attrs MAP(VARCHAR, INTEGER), keep_me VARCHAR); INSERT INTO demo.ddl_tbl VALUES (1, 1.5, TIMESTAMP '2026-01-01', {'a': 1}, MAP {'k': 1}, 'preserved');",
		}))

		asset := &pipeline.Asset{
			Name:            "demo.ddl_tbl",
			Type:            pipeline.AssetTypeDuckDBQuery,
			SchemaEvolution: &pipeline.SchemaEvolution{Enabled: boolPtr(true)},
			Materialization: pipeline.Materialization{
				Type:     pipeline.MaterializationTypeTable,
				Strategy: pipeline.MaterializationStrategyDDL,
			},
			Columns: []pipeline.Column{
				{Name: "pk", Type: "INTEGER", PrimaryKey: true},
				{Name: "score", Type: "DOUBLE PRECISION"},
				{Name: "ts", Type: "TIMESTAMP WITHOUT TIME ZONE"},
				{Name: "meta", Type: "STRUCT(a INT)"},
				{Name: "attrs", Type: "MAP(VARCHAR, INT)"},
				{Name: "created_at", Type: "TIMESTAMP", Default: "now()"},
				{Name: "status", Type: "VARCHAR", Default: "'active'", Nullable: pipeline.DefaultTrueBool{Value: boolPtr(false)}},
			},
		}

		var logBuf bytes.Buffer
		runCtx := context.WithValue(ctx, executor.KeyPrinter, &logBuf)

		require.NoError(t, op.RunTask(runCtx, p, asset))

		require.NoError(t, client.RunQueryWithoutResult(ctx, &query.Query{
			Query: "INSERT INTO demo.ddl_tbl (pk) VALUES (2);",
		}))
		rows, err := client.Select(ctx, &query.Query{
			Query: "SELECT pk, keep_me, created_at IS NOT NULL, status FROM demo.ddl_tbl ORDER BY pk",
		})
		require.NoError(t, err)
		assert.Equal(t, [][]interface{}{
			{int64(1), "preserved", false, "active"},
			{int64(2), nil, true, "active"},
		}, rows)

		// Attempting to widen PRIMARY KEY column must return IncompatibleSchemaError.
		pkWiden := *asset
		pkWiden.Columns = append([]pipeline.Column(nil), asset.Columns...)
		pkWiden.Columns[0].Type = "BIGINT"
		err = op.RunTask(runCtx, p, &pkWiden)
		require.Error(t, err)
		var incompatErr *schemaevolution.IncompatibleSchemaError
		require.ErrorAs(t, err, &incompatErr)
		assert.Contains(t, err.Error(), "PRIMARY KEY or UNIQUE")

		// Adding new PK or FK columns on DDL is rejected.
		newPKFK := *asset
		newPKFK.Columns = append(
			append([]pipeline.Column(nil), asset.Columns...),
			pipeline.Column{Name: "pk2", Type: "INTEGER", PrimaryKey: true},
			pipeline.Column{Name: "fk2", Type: "INTEGER", ForeignKey: &pipeline.ColumnReference{Table: "demo.ddl_tbl", Column: "pk"}},
		)
		err = op.RunTask(runCtx, p, &newPKFK)
		require.Error(t, err)
		require.ErrorAs(t, err, &incompatErr)
		assert.Len(t, incompatErr.Changes, 2)
	})

	t.Run("merge accepts new logical primary_key and nullable:false columns with trailing comments", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()
		client := newSchemaEvolutionTestClient(t)
		op := newSchemaEvolutionTestOperator(t, client, false)

		require.NoError(t, client.RunQueryWithoutResult(ctx, &query.Query{
			Query: "CREATE SCHEMA demo; CREATE TABLE demo.m_tbl (id INT, val VARCHAR); INSERT INTO demo.m_tbl VALUES (1, 'old');",
		}))
		mergeAsset := &pipeline.Asset{
			Name:            "demo.m_tbl",
			Type:            pipeline.AssetTypeDuckDBQuery,
			SchemaEvolution: &pipeline.SchemaEvolution{Enabled: boolPtr(true)},
			Materialization: pipeline.Materialization{Type: pipeline.MaterializationTypeTable, Strategy: pipeline.MaterializationStrategyMerge},
			Columns: []pipeline.Column{
				{Name: "id", Type: "INTEGER", PrimaryKey: true},
				{Name: "region", Type: "VARCHAR", PrimaryKey: true},
				{Name: "val", Type: "VARCHAR", UpdateOnMerge: true, Nullable: pipeline.DefaultTrueBool{Value: boolPtr(false)}},
				{Name: "tier", Type: "VARCHAR", UpdateOnMerge: true, Nullable: pipeline.DefaultTrueBool{Value: boolPtr(false)}},
			},
			ExecutableFile: pipeline.ExecutableFile{
				Content: "SELECT 2 AS id, 'eu' AS region, 'new' AS val, 'pro' AS tier\n-- merge trailing comment",
			},
		}
		require.NoError(t, op.RunTask(ctx, p, mergeAsset))

		rows, err := client.Select(ctx, &query.Query{Query: "SELECT id, region, val, tier FROM demo.m_tbl ORDER BY id"})
		require.NoError(t, err)
		assert.Equal(t, [][]interface{}{
			{int64(1), nil, "old", nil},
			{int64(2), "eu", "new", "pro"},
		}, rows)
	})
}

func TestSchemaEvolution_DuckDB_LifecycleHooksAndDevEnv(t *testing.T) {
	t.Parallel()
	p := &pipeline.Pipeline{Name: "test-pipeline"}

	t.Run("atomic rollback: failed write rolls back ALTER TABLE and does not log applied plan", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()
		client := newSchemaEvolutionTestClient(t)
		op := newSchemaEvolutionTestOperator(t, client, false)

		require.NoError(t, client.RunQueryWithoutResult(ctx, &query.Query{
			Query: "CREATE SCHEMA demo; CREATE TABLE demo.atomic (id INTEGER PRIMARY KEY); INSERT INTO demo.atomic VALUES (1);",
		}))

		var logBuf bytes.Buffer
		runCtx := context.WithValue(ctx, executor.KeyPrinter, &logBuf)

		asset := &pipeline.Asset{
			Name:            "demo.atomic",
			Type:            pipeline.AssetTypeDuckDBQuery,
			SchemaEvolution: &pipeline.SchemaEvolution{Enabled: boolPtr(true)},
			Materialization: pipeline.Materialization{
				Type:     pipeline.MaterializationTypeTable,
				Strategy: pipeline.MaterializationStrategyAppend,
			},
			ExecutableFile: pipeline.ExecutableFile{
				Content: "SELECT 1 AS id, 'should_rollback' AS extra",
			},
		}

		require.Error(t, op.RunTask(runCtx, p, asset))
		assert.Empty(t, logBuf.String())

		cols, _, err := newDuckDBEvolutionEngine(client).ExistingColumns(ctx, "demo.atomic")
		require.NoError(t, err)
		assert.Equal(t, []string{"id"}, columnNames(cols))
	})

	t.Run("incompatible and lossy type changes fail cleanly without altering the table", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()
		client := newSchemaEvolutionTestClient(t)
		op := newSchemaEvolutionTestOperator(t, client, false)

		require.NoError(t, client.RunQueryWithoutResult(ctx, &query.Query{
			Query: "CREATE SCHEMA demo; CREATE TABLE demo.strict (id BIGINT, amount DECIMAL(18,3), code INTEGER); INSERT INTO demo.strict VALUES (9007199254740993, 123456789012345.123, 100);",
		}))

		asset := &pipeline.Asset{
			Name:            "demo.strict",
			Type:            pipeline.AssetTypeDuckDBQuery,
			SchemaEvolution: &pipeline.SchemaEvolution{Enabled: boolPtr(true)},
			Materialization: pipeline.Materialization{
				Type:     pipeline.MaterializationTypeTable,
				Strategy: pipeline.MaterializationStrategyAppend,
			},
			ExecutableFile: pipeline.ExecutableFile{
				Content: "SELECT CAST(1.5 AS DOUBLE) AS id, CAST(1.5 AS DOUBLE) AS amount, 'ABC' AS code, 'new' AS extra",
			},
		}

		err := op.RunTask(ctx, p, asset)
		require.Error(t, err)
		var incompatErr *schemaevolution.IncompatibleSchemaError
		require.ErrorAs(t, err, &incompatErr)
		assert.Len(t, incompatErr.Changes, 3)

		rows, err := client.Select(ctx, &query.Query{
			Query: "SELECT id, CAST(amount AS VARCHAR), code FROM demo.strict",
		})
		require.NoError(t, err)
		assert.Equal(t, [][]interface{}{{int64(9007199254740993), "123456789012345.123", int64(100)}}, rows)
	})

	t.Run("disabled schema_evolution preserves existing behavior (fails on new column)", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()
		client := newSchemaEvolutionTestClient(t)
		op := newSchemaEvolutionTestOperator(t, client, false)

		require.NoError(t, client.RunQueryWithoutResult(ctx, &query.Query{
			Query: "CREATE SCHEMA demo; CREATE TABLE demo.plain (id INTEGER);",
		}))

		asset := &pipeline.Asset{
			Name:            "demo.plain",
			Type:            pipeline.AssetTypeDuckDBQuery,
			SchemaEvolution: &pipeline.SchemaEvolution{Enabled: boolPtr(false)},
			Materialization: pipeline.Materialization{
				Type:     pipeline.MaterializationTypeTable,
				Strategy: pipeline.MaterializationStrategyAppend,
			},
			ExecutableFile: pipeline.ExecutableFile{
				Content: "SELECT 1 AS id, 'extra' AS note",
			},
		}

		require.Error(t, op.RunTask(ctx, p, asset))

		cols, _, err := newDuckDBEvolutionEngine(client).ExistingColumns(ctx, "demo.plain")
		require.NoError(t, err)
		assert.Equal(t, []string{"id"}, columnNames(cols))
	})

	t.Run("full_refresh rebuilds table normally without running schema evolution", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()
		client := newSchemaEvolutionTestClient(t)
		op := newSchemaEvolutionTestOperator(t, client, true)

		require.NoError(t, client.RunQueryWithoutResult(ctx, &query.Query{
			Query: "CREATE SCHEMA demo; CREATE TABLE demo.rebuild (id INTEGER, code INTEGER);",
		}))

		asset := &pipeline.Asset{
			Name:            "demo.rebuild",
			Type:            pipeline.AssetTypeDuckDBQuery,
			SchemaEvolution: &pipeline.SchemaEvolution{Enabled: boolPtr(true)},
			Materialization: pipeline.Materialization{
				Type:     pipeline.MaterializationTypeTable,
				Strategy: pipeline.MaterializationStrategyAppend,
			},
			ExecutableFile: pipeline.ExecutableFile{
				Content: "SELECT 1 AS id, 'rebuilt' AS code",
			},
		}

		require.NoError(t, op.RunTask(ctx, p, asset))

		cols, _, err := newDuckDBEvolutionEngine(client).ExistingColumns(ctx, "demo.rebuild")
		require.NoError(t, err)
		assert.Equal(t, []string{"INTEGER", "VARCHAR"}, columnTypes(cols))
	})

	t.Run("non-existent table is a no-op for schema evolution, missing catalog surfaces error", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()
		client := newSchemaEvolutionTestClient(t)
		op := newSchemaEvolutionTestOperator(t, client, false)

		require.NoError(t, client.RunQueryWithoutResult(ctx, &query.Query{Query: "CREATE SCHEMA demo;"}))

		asset := &pipeline.Asset{
			Name:            "demo.missing",
			Type:            pipeline.AssetTypeDuckDBQuery,
			SchemaEvolution: &pipeline.SchemaEvolution{Enabled: boolPtr(true)},
			Materialization: pipeline.Materialization{
				Type:     pipeline.MaterializationTypeTable,
				Strategy: pipeline.MaterializationStrategyAppend,
			},
			ExecutableFile: pipeline.ExecutableFile{Content: "SELECT 1 AS id"},
		}

		err := op.RunTask(ctx, p, asset)
		require.Error(t, err)
		assert.NotContains(t, err.Error(), "schema evolution")

		_, _, err = newDuckDBEvolutionEngine(client).ExistingColumns(ctx, "no_such_catalog.demo.missing")
		require.Error(t, err)
	})

	t.Run("devEnv: transactional delete+insert rewritten to BEGIN; and hooks both get renamed and execute cleanly", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()
		client := newSchemaEvolutionTestClient(t)

		require.NoError(t, client.RunQueryWithoutResult(ctx, &query.Query{
			Query: `
CREATE SCHEMA dev_demo;
CREATE TABLE dev_demo.src (batch_id BIGINT, label VARCHAR);
INSERT INTO dev_demo.src VALUES (2, 'dev_val');
CREATE TABLE dev_demo.items (batch_id INTEGER);
INSERT INTO dev_demo.items VALUES (1);
CREATE TABLE dev_demo.audit (msg VARCHAR);
`,
		}))

		op := &BasicOperator{
			connection: staticConnectionGetter{conn: client},
			extractor: &query.WholeFileExtractor{
				Renderer: jinja.NewRendererWithYesterday("test-pipeline", "test-run"),
			},
			materializer: pipeline.HookWrapperMaterializer{
				Mat: NewMaterializer(false),
			},
			devEnv: fakeDevEnvModifier{
				rewrite: func(sql string) string {
					out := strings.ReplaceAll(sql, "BEGIN TRANSACTION;", "BEGIN;")
					out = strings.ReplaceAll(out, "demo.src", "dev_demo.src")
					out = strings.ReplaceAll(out, "demo.audit", "dev_demo.audit")
					return out
				},
			},
		}

		asset := &pipeline.Asset{
			Name:            "dev_demo.items",
			Type:            pipeline.AssetTypeDuckDBQuery,
			SchemaEvolution: &pipeline.SchemaEvolution{Enabled: boolPtr(true)},
			Materialization: pipeline.Materialization{
				Type:           pipeline.MaterializationTypeTable,
				Strategy:       pipeline.MaterializationStrategyDeleteInsert,
				IncrementalKey: "batch_id",
			},
			Hooks: pipeline.Hooks{
				Pre:  []pipeline.Hook{{Query: "INSERT INTO demo.audit VALUES ('pre')"}},
				Post: []pipeline.Hook{{Query: "INSERT INTO demo.audit VALUES ('post')"}},
			},
			ExecutableFile: pipeline.ExecutableFile{
				Content: "SELECT batch_id, label FROM demo.src",
			},
		}

		require.NoError(t, op.RunTask(ctx, p, asset))

		rows, err := client.Select(ctx, &query.Query{Query: "SELECT batch_id, label FROM dev_demo.items ORDER BY batch_id"})
		require.NoError(t, err)
		assert.Equal(t, [][]interface{}{
			{int64(1), nil},
			{int64(2), "dev_val"},
		}, rows)

		auditRows, err := client.Select(ctx, &query.Query{Query: "SELECT msg FROM dev_demo.audit ORDER BY msg"})
		require.NoError(t, err)
		assert.Equal(t, [][]interface{}{{"post"}, {"pre"}}, auditRows)
	})

	t.Run("pre-hooks run before schema inspection and execute side effects strictly once", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()
		client := newSchemaEvolutionTestClient(t)
		op := newSchemaEvolutionTestOperator(t, client, false)

		require.NoError(t, client.RunQueryWithoutResult(ctx, &query.Query{
			Query: `
CREATE SCHEMA demo;
CREATE SEQUENCE demo.run_seq START 1;
CREATE TABLE demo.seq_log (n BIGINT);
CREATE TABLE demo.hooked (id INTEGER, ts TIMESTAMP);
INSERT INTO demo.hooked VALUES (1, TIMESTAMP '2024-01-01 00:00:00');
`,
		}))

		asset := &pipeline.Asset{
			Name:            "demo.hooked",
			Type:            pipeline.AssetTypeDuckDBQuery,
			SchemaEvolution: &pipeline.SchemaEvolution{Enabled: boolPtr(true)},
			Materialization: pipeline.Materialization{
				Type:     pipeline.MaterializationTypeTable,
				Strategy: pipeline.MaterializationStrategyAppend,
			},
			Hooks: pipeline.Hooks{
				Pre: []pipeline.Hook{
					{Query: "SET TimeZone = 'America/New_York'"},
					{Query: "BEGIN TRANSACTION; INSERT INTO demo.seq_log VALUES (nextval('demo.run_seq')); COMMIT;"},
					{Query: "CREATE TABLE demo.run_marker AS SELECT 1 AS ok"},
					{Query: "CREATE TEMP TABLE hook_src AS SELECT CAST(2 AS BIGINT) AS id, TIMESTAMPTZ '2024-01-02 00:00:00+00' AS ts, 10 AS a, 20 AS b, 'from_hook' AS src"},
				},
			},
			ExecutableFile: pipeline.ExecutableFile{
				Content: "SELECT id, ts, a, b, src FROM hook_src; -- trailing comment",
			},
		}

		require.NoError(t, op.RunTask(ctx, p, asset))

		rows, err := client.Select(ctx, &query.Query{
			Query: "SET TimeZone = 'UTC'; SELECT id, CAST(ts AS VARCHAR), src FROM demo.hooked ORDER BY id",
		})
		require.NoError(t, err)
		assert.Equal(t, [][]interface{}{
			{int64(1), "2024-01-01 00:00:00+00", nil},
			{int64(2), "2024-01-02 00:00:00+00", "from_hook"},
		}, rows)

		seqRows, err := client.Select(ctx, &query.Query{Query: "SELECT n FROM demo.seq_log"})
		require.NoError(t, err)
		assert.Equal(t, [][]interface{}{{int64(1)}}, seqRows)
	})
}
