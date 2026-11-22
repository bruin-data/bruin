//go:build !bruin_no_duckdb

package duck

import (
	"context"
	"testing"

	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/bruin-data/bruin/pkg/query"
	"github.com/bruin-data/bruin/pkg/schemaevolution"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSchemaEvolution_DuckDB_TypesNullsAndSyntax(t *testing.T) {
	t.Parallel()
	p := &pipeline.Pipeline{Name: "test-pipeline"}

	t.Run("untyped NULL and empty list on existing columns are no-ops; incompatible or new untyped columns are rejected", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()
		client := newSchemaEvolutionTestClient(t)
		op := newSchemaEvolutionTestOperator(t, client, false)

		require.NoError(t, client.RunQueryWithoutResult(ctx, &query.Query{
			Query: "CREATE SCHEMA demo; CREATE TABLE demo.nulls (id INTEGER, note VARCHAR, amount DECIMAL(10,2), dt DATE, active BOOLEAN, tags VARCHAR[], nums INTEGER[]);",
		}))

		asset := &pipeline.Asset{
			Name:            "demo.nulls",
			Type:            pipeline.AssetTypeDuckDBQuery,
			SchemaEvolution: &pipeline.SchemaEvolution{Enabled: boolPtr(true)},
			Materialization: pipeline.Materialization{
				Type:     pipeline.MaterializationTypeTable,
				Strategy: pipeline.MaterializationStrategyAppend,
			},
			ExecutableFile: pipeline.ExecutableFile{
				Content: "SELECT 1 AS id, NULL AS note, NULL AS amount, NULL AS dt, NULL AS active, [] AS tags, [] AS nums",
			},
		}
		require.NoError(t, op.RunTask(ctx, p, asset))

		cols, _, err := newDuckDBEvolutionEngine(client).ExistingColumns(ctx, "demo.nulls")
		require.NoError(t, err)
		assert.Equal(t, []string{"INTEGER", "VARCHAR", "DECIMAL(10,2)", "DATE", "BOOLEAN", "VARCHAR[]", "INTEGER[]"}, columnTypes(cols))

		// Untyped NULL and [] on brand-new columns must be rejected rather than added as INTEGER / INTEGER[].
		asset.ExecutableFile.Content = "SELECT 2 AS id, NULL AS new_untyped, [] AS new_empty_list"
		err = op.RunTask(ctx, p, asset)
		require.Error(t, err)
		var incompatErr *schemaevolution.IncompatibleSchemaError
		require.ErrorAs(t, err, &incompatErr)
		assert.Contains(t, err.Error(), `column "new_untyped"`)
		assert.Contains(t, err.Error(), `column "new_empty_list"`)

		// Untyped empty list into an existing scalar INTEGER column or deeper [[]] into 1D INTEGER[] must be rejected.
		asset.ExecutableFile.Content = "SELECT [] AS id, [[]] AS nums"
		err = op.RunTask(ctx, p, asset)
		require.Error(t, err)
		require.ErrorAs(t, err, &incompatErr)
		assert.Len(t, incompatErr.Changes, 2)

		// Untyped empty list into an existing VARCHAR column (and [[]] into VARCHAR[]) succeeds via DuckDB's implicit cast.
		asset.ExecutableFile.Content = "SELECT 3 AS id, [] AS note, [[]] AS tags"
		require.NoError(t, op.RunTask(ctx, p, asset))

		cols, _, err = newDuckDBEvolutionEngine(client).ExistingColumns(ctx, "demo.nulls")
		require.NoError(t, err)
		assert.Equal(t, []string{"id", "note", "amount", "dt", "active", "tags", "nums"}, columnNames(cols))
	})

	t.Run("dotted column names add, widen, and preserve untyped-NULL detection", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()
		client := newSchemaEvolutionTestClient(t)
		op := newSchemaEvolutionTestOperator(t, client, false)

		require.NoError(t, client.RunQueryWithoutResult(ctx, &query.Query{
			Query: `CREATE SCHEMA demo; CREATE TABLE demo.dotted (id INTEGER, "a.b" INTEGER); INSERT INTO demo.dotted VALUES (1, 10);`,
		}))

		asset := &pipeline.Asset{
			Name:            "demo.dotted",
			Type:            pipeline.AssetTypeDuckDBQuery,
			SchemaEvolution: &pipeline.SchemaEvolution{Enabled: boolPtr(true)},
			Materialization: pipeline.Materialization{
				Type:     pipeline.MaterializationTypeTable,
				Strategy: pipeline.MaterializationStrategyAppend,
			},
			ExecutableFile: pipeline.ExecutableFile{
				Content: `SELECT 2 AS id, CAST(9000000000 AS BIGINT) AS "a.b", 'ok' AS "x.y"`,
			},
		}
		require.NoError(t, op.RunTask(ctx, p, asset))

		cols, _, err := newDuckDBEvolutionEngine(client).ExistingColumns(ctx, "demo.dotted")
		require.NoError(t, err)
		assert.Equal(t, []string{"id", "a.b", "x.y"}, columnNames(cols))
		assert.Equal(t, []string{"INTEGER", "BIGINT", "VARCHAR"}, columnTypes(cols))

		// With an INTEGER dotted column in the query, the untyped-NULL probe must still succeed and reject a new NULL column.
		asset.ExecutableFile.Content = `SELECT 3 AS id, 20 AS "a.b", NULL AS bad_null`
		err = op.RunTask(ctx, p, asset)
		require.Error(t, err)
		var incompatErr *schemaevolution.IncompatibleSchemaError
		require.ErrorAs(t, err, &incompatErr)
		assert.Contains(t, err.Error(), `column "bad_null"`)
	})

	t.Run("ENUM column preserves case-sensitive labels and subsequent runs are idempotent", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()
		client := newSchemaEvolutionTestClient(t)
		op := newSchemaEvolutionTestOperator(t, client, false)

		require.NoError(t, client.RunQueryWithoutResult(ctx, &query.Query{
			Query: "CREATE SCHEMA demo; CREATE TABLE demo.feelings (id INTEGER);",
		}))

		asset := &pipeline.Asset{
			Name:            "demo.feelings",
			Type:            pipeline.AssetTypeDuckDBQuery,
			SchemaEvolution: &pipeline.SchemaEvolution{Enabled: boolPtr(true)},
			Materialization: pipeline.Materialization{
				Type:     pipeline.MaterializationTypeTable,
				Strategy: pipeline.MaterializationStrategyAppend,
			},
			ExecutableFile: pipeline.ExecutableFile{
				Content: "SELECT 1 AS id, CAST('Happy' AS ENUM('sad', 'Happy')) AS mood, CAST(['sad', 'Happy'] AS ENUM('sad', 'Happy')[]) AS history",
			},
		}

		require.NoError(t, op.RunTask(ctx, p, asset))
		asset.ExecutableFile.Content = "SELECT 2 AS id, CAST('sad' AS ENUM('sad', 'Happy')) AS mood, CAST(['Happy'] AS ENUM('sad', 'Happy')[]) AS history"
		require.NoError(t, op.RunTask(ctx, p, asset))

		rows, err := client.Select(ctx, &query.Query{
			Query: "SELECT id, CAST(mood AS VARCHAR) FROM demo.feelings ORDER BY id",
		})
		require.NoError(t, err)
		assert.Equal(t, [][]interface{}{
			{int64(1), "Happy"},
			{int64(2), "sad"},
		}, rows)
	})

	t.Run("implicit DuckDB casts and VARCHAR composite absorption keep working without error", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()
		client := newSchemaEvolutionTestClient(t)
		op := newSchemaEvolutionTestOperator(t, client, false)

		require.NoError(t, client.RunQueryWithoutResult(ctx, &query.Query{
			Query: "CREATE SCHEMA demo; CREATE TABLE demo.implicit_ok (code VARCHAR, dt DATE, payload VARCHAR);",
		}))

		asset := &pipeline.Asset{
			Name:            "demo.implicit_ok",
			Type:            pipeline.AssetTypeDuckDBQuery,
			SchemaEvolution: &pipeline.SchemaEvolution{Enabled: boolPtr(true)},
			Materialization: pipeline.Materialization{
				Type:     pipeline.MaterializationTypeTable,
				Strategy: pipeline.MaterializationStrategyAppend,
			},
			ExecutableFile: pipeline.ExecutableFile{
				Content: "SELECT 42 AS code, '2026-05-01' AS dt, [1, 2] AS payload, 'added' AS note",
			},
		}
		require.NoError(t, op.RunTask(ctx, p, asset))

		rows, err := client.Select(ctx, &query.Query{
			Query: "SELECT code, CAST(dt AS VARCHAR), payload, note FROM demo.implicit_ok",
		})
		require.NoError(t, err)
		assert.Equal(t, [][]interface{}{{"42", "2026-05-01", "[1, 2]", "added"}}, rows)
	})

	t.Run("TIMESTAMP[] to TIMESTAMPTZ[] uses deterministic UTC list_transform under non-UTC timezone", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()
		client := newSchemaEvolutionTestClient(t)
		op := newSchemaEvolutionTestOperator(t, client, false)

		require.NoError(t, client.RunQueryWithoutResult(ctx, &query.Query{
			Query: "CREATE SCHEMA demo; CREATE TABLE demo.ts_lists (id INT, stamps TIMESTAMP[]); INSERT INTO demo.ts_lists VALUES (1, [TIMESTAMP '2024-01-01 00:00:00']);",
		}))

		asset := &pipeline.Asset{
			Name:            "demo.ts_lists",
			Type:            pipeline.AssetTypeDuckDBQuery,
			SchemaEvolution: &pipeline.SchemaEvolution{Enabled: boolPtr(true)},
			Materialization: pipeline.Materialization{
				Type:     pipeline.MaterializationTypeTable,
				Strategy: pipeline.MaterializationStrategyAppend,
			},
			Hooks: pipeline.Hooks{
				Pre: []pipeline.Hook{{Query: "SET TimeZone = 'America/New_York'"}},
			},
			ExecutableFile: pipeline.ExecutableFile{
				Content: "SELECT 2 AS id, [TIMESTAMPTZ '2024-01-02 00:00:00+00'] AS stamps",
			},
		}

		require.NoError(t, op.RunTask(ctx, p, asset))

		rows, err := client.Select(ctx, &query.Query{
			Query: "SET TimeZone = 'UTC'; SELECT id, CAST(stamps[1] AS VARCHAR) FROM demo.ts_lists ORDER BY id",
		})
		require.NoError(t, err)
		assert.Equal(t, [][]interface{}{
			{int64(1), "2024-01-01 00:00:00+00"},
			{int64(2), "2024-01-02 00:00:00+00"},
		}, rows)
	})

	t.Run("trailing line comments and bare VALUES queries are handled safely", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()
		client := newSchemaEvolutionTestClient(t)
		op := newSchemaEvolutionTestOperator(t, client, false)

		require.NoError(t, client.RunQueryWithoutResult(ctx, &query.Query{
			Query: `
CREATE SCHEMA demo;
CREATE TABLE demo.comment_append (a INT);
INSERT INTO demo.comment_append VALUES (1);
CREATE TABLE demo.comment_trunc (a INT);
INSERT INTO demo.comment_trunc VALUES (1);
CREATE TABLE demo.vals (id INT, name VARCHAR);
`,
		}))

		appendAsset := &pipeline.Asset{
			Name:            "demo.comment_append",
			Type:            pipeline.AssetTypeDuckDBQuery,
			SchemaEvolution: &pipeline.SchemaEvolution{Enabled: boolPtr(true)},
			Materialization: pipeline.Materialization{
				Type:     pipeline.MaterializationTypeTable,
				Strategy: pipeline.MaterializationStrategyAppend,
			},
			ExecutableFile: pipeline.ExecutableFile{
				Content: "SELECT 2 AS a, 'café' AS b\n-- trailing comment without semicolon",
			},
		}
		require.NoError(t, op.RunTask(ctx, p, appendAsset))

		rows, err := client.Select(ctx, &query.Query{Query: "SELECT a, b FROM demo.comment_append ORDER BY a"})
		require.NoError(t, err)
		assert.Equal(t, [][]interface{}{{int64(1), nil}, {int64(2), "café"}}, rows)

		truncAsset := &pipeline.Asset{
			Name:            "demo.comment_trunc",
			Type:            pipeline.AssetTypeDuckDBQuery,
			SchemaEvolution: &pipeline.SchemaEvolution{Enabled: boolPtr(true)},
			Materialization: pipeline.Materialization{
				Type:     pipeline.MaterializationTypeTable,
				Strategy: pipeline.MaterializationStrategyTruncateInsert,
			},
			ExecutableFile: pipeline.ExecutableFile{
				Content: "SELECT 10 AS a, 'fresh' AS b\n-- another trailing comment",
			},
		}
		require.NoError(t, op.RunTask(ctx, p, truncAsset))

		rows, err = client.Select(ctx, &query.Query{Query: "SELECT a, b FROM demo.comment_trunc"})
		require.NoError(t, err)
		assert.Equal(t, [][]interface{}{{int64(10), "fresh"}}, rows)

		err = op.RunTask(ctx, p, &pipeline.Asset{
			Name:            "demo.vals",
			Type:            pipeline.AssetTypeDuckDBQuery,
			SchemaEvolution: &pipeline.SchemaEvolution{Enabled: boolPtr(true)},
			Materialization: pipeline.Materialization{Type: pipeline.MaterializationTypeTable, Strategy: pipeline.MaterializationStrategyAppend},
			ExecutableFile:  pipeline.ExecutableFile{Content: "VALUES (1, 'alice')"},
		})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "bare VALUES queries are not supported")
	})

	t.Run("fixed-size ARRAY columns preserve fixed length on element widening and reject mismatched array lengths", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()
		client := newSchemaEvolutionTestClient(t)
		op := newSchemaEvolutionTestOperator(t, client, false)

		require.NoError(t, client.RunQueryWithoutResult(ctx, &query.Query{
			Query: `
				SET TimeZone = 'America/New_York';
				CREATE SCHEMA demo;
				CREATE TABLE demo.vectors (
					id INT,
					vec INT[3],
					spans TIMESTAMP[2]
				);
				INSERT INTO demo.vectors VALUES (
					1,
					[1, 2, 3],
					[TIMESTAMP '2026-01-01 00:00:00', TIMESTAMP '2026-01-02 12:00:00']
				);
			`,
		}))

		asset := &pipeline.Asset{
			Name:            "demo.vectors",
			Type:            pipeline.AssetTypeDuckDBQuery,
			SchemaEvolution: &pipeline.SchemaEvolution{Enabled: boolPtr(true)},
			Materialization: pipeline.Materialization{
				Type:     pipeline.MaterializationTypeTable,
				Strategy: pipeline.MaterializationStrategyAppend,
			},
			ExecutableFile: pipeline.ExecutableFile{
				Content: `
					SELECT
						CAST(2 AS DECIMAL(5,0)) AS id,
						CAST([10, 20, 9000000000] AS BIGINT[3]) AS vec,
						CAST([TIMESTAMPTZ '2026-01-03 00:00:00+00', TIMESTAMPTZ '2026-01-04 00:00:00+00'] AS TIMESTAMPTZ[2]) AS spans
				`,
			},
		}
		require.NoError(t, op.RunTask(ctx, p, asset))

		cols, _, err := newDuckDBEvolutionEngine(client).ExistingColumns(ctx, "demo.vectors")
		require.NoError(t, err)
		assert.Equal(t, []string{"INTEGER", "BIGINT[3]", "TIMESTAMP WITH TIME ZONE[2]"}, columnTypes(cols))

		asset.ExecutableFile.Content = `SELECT 3 AS id, CAST([1, 2, 3, 4] AS BIGINT[4]) AS vec`
		err = op.RunTask(ctx, p, asset)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "fixed-size array")
	})

	t.Run("nested untyped NULL inside STRUCT or empty MAP is rejected on new columns and checked structurally on existing columns", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()
		client := newSchemaEvolutionTestClient(t)
		op := newSchemaEvolutionTestOperator(t, client, false)

		require.NoError(t, client.RunQueryWithoutResult(ctx, &query.Query{
			Query: "CREATE SCHEMA demo; CREATE TABLE demo.nested_nulls (id INT);",
		}))

		asset := &pipeline.Asset{
			Name:            "demo.nested_nulls",
			Type:            pipeline.AssetTypeDuckDBQuery,
			SchemaEvolution: &pipeline.SchemaEvolution{Enabled: boolPtr(true)},
			Materialization: pipeline.Materialization{
				Type:     pipeline.MaterializationTypeTable,
				Strategy: pipeline.MaterializationStrategyAppend,
			},
			ExecutableFile: pipeline.ExecutableFile{
				Content: `SELECT 1 AS id, {'x': NULL} AS meta`,
			},
		}
		err := op.RunTask(ctx, p, asset)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "nested untyped NULL")

		asset.ExecutableFile.Content = `SELECT 1 AS id, MAP {} AS kv`
		err = op.RunTask(ctx, p, asset)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "nested untyped NULL")

		// A struct whose field is literally named "NULL" with a concrete INTEGER value must NOT be falsely rejected.
		asset.ExecutableFile.Content = `SELECT 1 AS id, {'NULL': 42} AS named_null_field`
		require.NoError(t, op.RunTask(ctx, p, asset))

		cols, _, err := newDuckDBEvolutionEngine(client).ExistingColumns(ctx, "demo.nested_nulls")
		require.NoError(t, err)
		assert.Equal(t, []string{"id", "named_null_field"}, columnNames(cols))

		require.NoError(t, client.RunQueryWithoutResult(ctx, &query.Query{
			Query: "CREATE TABLE demo.existing_composites (id INT, meta STRUCT(x VARCHAR, dt DATE, tags INT[], cnt BIGINT), kv MAP(VARCHAR, BIGINT), u UNION(a VARCHAR, b BIGINT));",
		}))

		compAsset := &pipeline.Asset{
			Name:            "demo.existing_composites",
			Type:            pipeline.AssetTypeDuckDBQuery,
			SchemaEvolution: &pipeline.SchemaEvolution{Enabled: boolPtr(true)},
			Materialization: pipeline.Materialization{
				Type:     pipeline.MaterializationTypeTable,
				Strategy: pipeline.MaterializationStrategyAppend,
			},
			ExecutableFile: pipeline.ExecutableFile{
				Content: `SELECT 1 AS id, {'x': NULL, 'dt': DATE '2026-01-01', 'tags': [], 'cnt': 5} AS meta, MAP {'k': 1} AS kv, union_value(a := NULL) AS u`,
			},
		}
		require.NoError(t, op.RunTask(ctx, p, compAsset))

		// Concrete narrower STRUCT/MAP/UNION values without any NULLs also succeed cleanly.
		compAsset.ExecutableFile.Content = `SELECT 2 AS id, {'x': 'ok', 'dt': DATE '2026-01-02', 'tags': [1], 'cnt': 7} AS meta, MAP {'k': 2} AS kv, union_value(b := 9) AS u`
		require.NoError(t, op.RunTask(ctx, p, compAsset))

		// Deeper nested empty list [[]] inside STRUCT(tags INT[]) must be rejected as IncompatibleSchemaError.
		compAsset.ExecutableFile.Content = `SELECT 3 AS id, {'x': NULL, 'dt': DATE '2026-01-01', 'tags': [[]], 'cnt': 1} AS meta, MAP {} AS kv, union_value(a := NULL) AS u`
		err = op.RunTask(ctx, p, compAsset)
		var incompatErr *schemaevolution.IncompatibleSchemaError
		require.ErrorAs(t, err, &incompatErr)

		// Mismatched field name {'z': ...} fails and reports real "NULL" rather than fake INTEGER.
		compAsset.ExecutableFile.Content = `SELECT 4 AS id, {'z': NULL, 'dt': NULL, 'tags': [], 'cnt': 1} AS meta, MAP {} AS kv, union_value(a := NULL) AS u`
		err = op.RunTask(ctx, p, compAsset)
		require.Error(t, err)
		assert.Contains(t, err.Error(), `"NULL"`)
		assert.NotContains(t, err.Error(), "STRUCT(z INTEGER")

		// Incompatible sibling field {'dt': 123} into STRUCT(..., dt DATE, ...) fails cleanly.
		compAsset.ExecutableFile.Content = `SELECT 5 AS id, {'x': NULL, 'dt': 123, 'tags': [], 'cnt': 1} AS meta, MAP {} AS kv, union_value(a := NULL) AS u`
		require.Error(t, op.RunTask(ctx, p, compAsset))

		// Merge and SCD2 also reject adding declared new columns fed by nested untyped NULL / empty MAP.
		require.NoError(t, client.RunQueryWithoutResult(ctx, &query.Query{
			Query: `
				CREATE TABLE demo.merge_nested_null (id INT);
				CREATE TABLE demo.scd2_nested_null (
					id INT,
					_valid_from TIMESTAMPTZ,
					_valid_until TIMESTAMPTZ,
					_is_current BOOLEAN
				);
			`,
		}))

		mergeAsset := &pipeline.Asset{
			Name:            "demo.merge_nested_null",
			Type:            pipeline.AssetTypeDuckDBQuery,
			SchemaEvolution: &pipeline.SchemaEvolution{Enabled: boolPtr(true)},
			Materialization: pipeline.Materialization{
				Type:     pipeline.MaterializationTypeTable,
				Strategy: pipeline.MaterializationStrategyMerge,
			},
			Columns: []pipeline.Column{
				{Name: "id", PrimaryKey: true},
				{Name: "kv"},
			},
			ExecutableFile: pipeline.ExecutableFile{
				Content: `SELECT 1 AS id, {'x': NULL} AS kv`,
			},
		}
		err = op.RunTask(ctx, p, mergeAsset)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "nested untyped NULL")

		scd2Asset := &pipeline.Asset{
			Name:            "demo.scd2_nested_null",
			Type:            pipeline.AssetTypeDuckDBQuery,
			SchemaEvolution: &pipeline.SchemaEvolution{Enabled: boolPtr(true)},
			Materialization: pipeline.Materialization{
				Type:     pipeline.MaterializationTypeTable,
				Strategy: pipeline.MaterializationStrategySCD2ByColumn,
			},
			Columns: []pipeline.Column{
				{Name: "id", PrimaryKey: true},
				{Name: "kv"},
			},
			ExecutableFile: pipeline.ExecutableFile{
				Content: `SELECT 1 AS id, MAP {} AS kv`,
			},
		}
		err = op.RunTask(ctx, p, scd2Asset)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "nested untyped NULL")
	})
}
