//go:build !bruin_no_duckdb

package duck

import (
	"bytes"
	"context"
	"path/filepath"
	"testing"

	"github.com/bruin-data/bruin/pkg/executor"
	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/bruin-data/bruin/pkg/query"
	"github.com/bruin-data/bruin/pkg/schemaevolution"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSchemaEvolution_DuckDB_SCD2AndDataVault(t *testing.T) {
	t.Parallel()
	p := &pipeline.Pipeline{Name: "test-pipeline"}

	t.Run("datavault_satellite and hub: uses declared types and real DV resolvers for PK and mandatory NOT NULL columns", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()
		client := newSchemaEvolutionTestClient(t)
		fullOp := newSchemaEvolutionTestOperator(t, client, true)
		incOp := newSchemaEvolutionTestOperator(t, client, false)

		require.NoError(t, client.RunQueryWithoutResult(ctx, &query.Query{
			Query: `
CREATE SCHEMA stg;
CREATE TABLE stg.sat_src AS
SELECT
	'hk1' AS customer_hk,
	'hd1' AS hashdiff,
	CURRENT_TIMESTAMP AS load_dts,
	'crm' AS record_source,
	CAST(12.5 AS DOUBLE) AS amount,
	CAST(3.25 AS DOUBLE) AS bonus,
	'gold' AS segment;
`,
		}))

		asset := &pipeline.Asset{
			Name:            "demo.sat_customer",
			Type:            pipeline.AssetTypeDuckDBQuery,
			SchemaEvolution: &pipeline.SchemaEvolution{Enabled: boolPtr(true)},
			Materialization: pipeline.Materialization{
				Type:     pipeline.MaterializationTypeTable,
				Strategy: pipeline.MaterializationStrategyDataVaultSatellite,
			},
			Columns: []pipeline.Column{
				{Name: "customer_hk", Type: "VARCHAR", Meta: pipeline.EmptyStringMap{"datavault_role": "parent_hash_key"}},
				{Name: "hashdiff", Type: "VARCHAR", Meta: pipeline.EmptyStringMap{"datavault_role": "hashdiff"}},
				{Name: "load_dts", Type: "TIMESTAMP", Meta: pipeline.EmptyStringMap{"datavault_role": "load_datetime"}},
				{Name: "record_source", Type: "VARCHAR", Meta: pipeline.EmptyStringMap{"datavault_role": "record_source"}},
				{Name: "amount", Type: "DECIMAL(18,2)"},
			},
			ExecutableFile: pipeline.ExecutableFile{
				Content: "SELECT customer_hk, hashdiff, load_dts, record_source, amount, bonus, segment FROM stg.sat_src",
			},
		}

		require.NoError(t, fullOp.RunTask(ctx, p, asset))

		var logBuf bytes.Buffer
		require.NoError(t, incOp.RunTask(context.WithValue(ctx, executor.KeyPrinter, &logBuf), p, asset))
		assert.Empty(t, logBuf.String())

		asset.Columns = append(
			asset.Columns,
			pipeline.Column{Name: "bonus", Type: "DECIMAL(18,2)"},
			pipeline.Column{
				Name:       "segment",
				Type:       "VARCHAR",
				Default:    "now()",
				Collation:  "en_US",
				ForeignKey: &pipeline.ColumnReference{Table: "demo.sat_customer", Column: "customer_hk"},
			},
		)
		require.NoError(t, client.RunQueryWithoutResult(ctx, &query.Query{
			Query: "UPDATE stg.sat_src SET hashdiff = 'hd2', load_dts = CURRENT_TIMESTAMP + INTERVAL 1 SECOND",
		}))
		require.NoError(t, incOp.RunTask(ctx, p, asset))

		cols, _, err := newDuckDBEvolutionEngine(client).ExistingColumns(ctx, "demo.sat_customer")
		require.NoError(t, err)
		assert.Equal(t, []string{"customer_hk", "hashdiff", "load_dts", "record_source", "amount", "bonus", "segment"}, columnNames(cols))
		assert.Equal(t, []string{"VARCHAR", "VARCHAR", "TIMESTAMP", "VARCHAR", "DECIMAL(18,2)", "DECIMAL(18,2)", "VARCHAR"}, columnTypes(cols))

		notNullAsset := *asset
		notNullAsset.Columns = append(append([]pipeline.Column(nil), asset.Columns...), pipeline.Column{
			Name:     "required_attr",
			Type:     "VARCHAR",
			Nullable: pipeline.DefaultTrueBool{Value: boolPtr(false)},
		})
		err = incOp.RunTask(ctx, p, &notNullAsset)
		require.Error(t, err)
		var incompatErr *schemaevolution.IncompatibleSchemaError
		require.ErrorAs(t, err, &incompatErr)
		assert.Contains(t, err.Error(), "set nullable: true or rebuild the table")
		assert.NotContains(t, err.Error(), "DEFAULT")

		pkAsset := *asset
		pkAsset.Columns = append(append([]pipeline.Column(nil), asset.Columns...), pipeline.Column{
			Name:       "sub_key",
			Type:       "VARCHAR",
			PrimaryKey: true,
		})
		err = incOp.RunTask(ctx, p, &pkAsset)
		require.Error(t, err)
		require.ErrorAs(t, err, &incompatErr)
		assert.Contains(t, err.Error(), "PRIMARY KEY")

		require.NoError(t, client.RunQueryWithoutResult(ctx, &query.Query{
			Query: "CREATE TABLE stg.hub_src AS SELECT 'hk1' AS customer_hk, 'c1' AS customer_bk, 't1' AS tenant_bk, CURRENT_TIMESTAMP AS load_dts, 'crm' AS record_source;",
		}))
		hubAsset := &pipeline.Asset{
			Name:            "demo.hub_customer",
			Type:            pipeline.AssetTypeDuckDBQuery,
			SchemaEvolution: &pipeline.SchemaEvolution{Enabled: boolPtr(true)},
			Materialization: pipeline.Materialization{
				Type:     pipeline.MaterializationTypeTable,
				Strategy: pipeline.MaterializationStrategyDataVaultHub,
			},
			Columns: []pipeline.Column{
				{Name: "customer_hk", Type: "VARCHAR", Meta: pipeline.EmptyStringMap{"datavault_role": "hash_key"}},
				{Name: "customer_bk", Type: "VARCHAR", Meta: pipeline.EmptyStringMap{"datavault_role": "business_key"}},
				{Name: "load_dts", Type: "TIMESTAMP", Meta: pipeline.EmptyStringMap{"datavault_role": "load_datetime"}},
				{Name: "record_source", Type: "VARCHAR", Meta: pipeline.EmptyStringMap{"datavault_role": "record_source"}},
			},
			ExecutableFile: pipeline.ExecutableFile{
				Content: "SELECT customer_hk, customer_bk, tenant_bk, load_dts, record_source FROM stg.hub_src",
			},
		}
		require.NoError(t, fullOp.RunTask(ctx, p, hubAsset))

		hubAsset.Columns = append(hubAsset.Columns, pipeline.Column{Name: "tenant_bk", Type: "VARCHAR"})
		err = incOp.RunTask(ctx, p, hubAsset)
		require.Error(t, err)
		require.ErrorAs(t, err, &incompatErr)
		assert.Contains(t, err.Error(), "NOT NULL column to a Data Vault table")
	})

	t.Run("scd2: rejects VARCHAR into DATE, LIST/STRUCT/UUID into VARCHAR, and omitted existing columns", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()
		client := newSchemaEvolutionTestClient(t)
		fullOp := newSchemaEvolutionTestOperator(t, client, true)
		incOp := newSchemaEvolutionTestOperator(t, client, false)

		asset := &pipeline.Asset{
			Name:            "demo.scd2_guards",
			Type:            pipeline.AssetTypeDuckDBQuery,
			SchemaEvolution: &pipeline.SchemaEvolution{Enabled: boolPtr(true)},
			Materialization: pipeline.Materialization{
				Type:     pipeline.MaterializationTypeTable,
				Strategy: pipeline.MaterializationStrategySCD2ByColumn,
			},
			Columns: []pipeline.Column{
				{Name: "id", Type: "INTEGER", PrimaryKey: true},
				{Name: "d", Type: "DATE"},
				{Name: "payload", Type: "VARCHAR"},
				{Name: "keep_col", Type: "VARCHAR"},
			},
			ExecutableFile: pipeline.ExecutableFile{
				Content: "SELECT 1 AS id, DATE '2024-01-01' AS d, 'hello' AS payload, 'v1' AS keep_col",
			},
		}
		require.NoError(t, fullOp.RunTask(ctx, p, asset))

		for _, tc := range []struct {
			name string
			sql  string
		}{
			{"varchar into date", "SELECT 1 AS id, '2024-02-02' AS d, 'hello' AS payload, 'v1' AS keep_col"},
			{"integer into varchar", "SELECT 1 AS id, DATE '2024-01-01' AS d, 42 AS payload, 'v1' AS keep_col"},
			{"list into varchar", "SELECT 1 AS id, DATE '2024-01-01' AS d, [1, 2] AS payload, 'v1' AS keep_col"},
			{"struct into varchar", "SELECT 1 AS id, DATE '2024-01-01' AS d, {'a': 1} AS payload, 'v1' AS keep_col"},
			{"uuid into varchar", "SELECT 1 AS id, DATE '2024-01-01' AS d, uuid() AS payload, 'v1' AS keep_col"},
		} {
			testAsset := *asset
			testAsset.ExecutableFile.Content = tc.sql
			err := incOp.RunTask(ctx, p, &testAsset)
			require.Error(t, err, tc.name)
			var incompatErr *schemaevolution.IncompatibleSchemaError
			require.ErrorAs(t, err, &incompatErr, tc.name)
			assert.Contains(t, err.Error(), "UNION ALL", tc.name)
		}

		dropAsset := *asset
		dropAsset.Columns = asset.Columns[:3]
		dropAsset.ExecutableFile.Content = "SELECT 1 AS id, DATE '2024-01-01' AS d, 'hello' AS payload"
		err := incOp.RunTask(ctx, p, &dropAsset)
		require.Error(t, err)
		var incompatErr *schemaevolution.IncompatibleSchemaError
		require.ErrorAs(t, err, &incompatErr)
		assert.Contains(t, err.Error(), `column "keep_col"`)
		assert.Contains(t, err.Error(), "would drop it")

		cols, _, err := newDuckDBEvolutionEngine(client).ExistingColumns(ctx, "demo.scd2_guards")
		require.NoError(t, err)
		assert.Equal(t, []string{"id", "d", "payload", "keep_col", "_valid_from", "_valid_until", "_is_current"}, columnNames(cols))
		assert.Equal(t, []string{"INTEGER", "DATE", "VARCHAR", "VARCHAR", "TIMESTAMP WITH TIME ZONE", "TIMESTAMP WITH TIME ZONE", "BOOLEAN"}, columnTypes(cols))
	})

	t.Run("scd2_by_time: allows INTEGER into non-PK VARCHAR via UNION ALL, rejects UUID/LIST into non-PK VARCHAR and INTEGER into PK VARCHAR", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()
		client := newSchemaEvolutionTestClient(t)
		fullOp := newSchemaEvolutionTestOperator(t, client, true)
		incOp := newSchemaEvolutionTestOperator(t, client, false)

		asset := &pipeline.Asset{
			Name:            "demo.scd2_time_varchar",
			Type:            pipeline.AssetTypeDuckDBQuery,
			SchemaEvolution: &pipeline.SchemaEvolution{Enabled: boolPtr(true)},
			Materialization: pipeline.Materialization{
				Type:           pipeline.MaterializationTypeTable,
				Strategy:       pipeline.MaterializationStrategySCD2ByTime,
				IncrementalKey: "updated_at",
			},
			Columns: []pipeline.Column{
				{Name: "id", Type: "VARCHAR", PrimaryKey: true},
				{Name: "label", Type: "VARCHAR"},
				{Name: "updated_at", Type: "TIMESTAMP"},
			},
			ExecutableFile: pipeline.ExecutableFile{
				Content: "SELECT 'k1' AS id, 'initial' AS label, TIMESTAMP '2026-01-01 00:00:00' AS updated_at",
			},
		}
		require.NoError(t, fullOp.RunTask(ctx, p, asset))

		okAsset := *asset
		okAsset.ExecutableFile.Content = "SELECT 'k1' AS id, 42 AS label, TIMESTAMP '2026-01-02 00:00:00' AS updated_at"
		require.NoError(t, incOp.RunTask(ctx, p, &okAsset))

		cols, _, err := newDuckDBEvolutionEngine(client).ExistingColumns(ctx, "demo.scd2_time_varchar")
		require.NoError(t, err)
		assert.Equal(t, []string{"VARCHAR", "VARCHAR", "TIMESTAMP", "TIMESTAMP WITH TIME ZONE", "TIMESTAMP WITH TIME ZONE", "BOOLEAN"}, columnTypes(cols))

		rows, err := client.Select(ctx, &query.Query{
			Query: "SELECT id, label, _is_current FROM demo.scd2_time_varchar ORDER BY _is_current",
		})
		require.NoError(t, err)
		assert.Equal(t, [][]interface{}{
			{"k1", "initial", false},
			{"k1", "42", true},
		}, rows)

		for _, sql := range []string{
			"SELECT 'k1' AS id, [1, 2] AS label, TIMESTAMP '2026-01-03 00:00:00' AS updated_at",
			"SELECT 'k1' AS id, uuid() AS label, TIMESTAMP '2026-01-03 00:00:00' AS updated_at",
		} {
			badAsset := *asset
			badAsset.ExecutableFile.Content = sql
			err = incOp.RunTask(ctx, p, &badAsset)
			var incompatErr *schemaevolution.IncompatibleSchemaError
			require.ErrorAs(t, err, &incompatErr)
		}

		badPK := *asset
		badPK.ExecutableFile.Content = "SELECT 1 AS id, 'ok' AS label, TIMESTAMP '2026-01-03 00:00:00' AS updated_at"
		err = incOp.RunTask(ctx, p, &badPK)
		var incompatErr *schemaevolution.IncompatibleSchemaError
		require.ErrorAs(t, err, &incompatErr)
	})

	t.Run("SCD2 rejects UNION ALL narrowing on INTEGER[] <- INTEGER[3] and HUGEINT <- DECIMAL(9,0), and plans INTEGER <- DECIMAL(5,0) widening", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()
		client := newSchemaEvolutionTestClient(t)
		op := newSchemaEvolutionTestOperator(t, client, false)

		require.NoError(t, client.RunQueryWithoutResult(ctx, &query.Query{
			Query: `
				CREATE SCHEMA demo;
				CREATE TABLE demo.scd2_union_guard (
					id INT,
					tags INT[],
					big_val HUGEINT,
					count_val INT,
					_valid_from TIMESTAMPTZ,
					_valid_until TIMESTAMPTZ,
					_is_current BOOLEAN
				);
				INSERT INTO demo.scd2_union_guard VALUES (
					1,
					[10, 20],
					CAST('170141183460469231731687303715884105727' AS HUGEINT),
					5,
					TIMESTAMPTZ '2026-01-01 00:00:00+00',
					TIMESTAMPTZ '9999-12-31 00:00:00+00',
					TRUE
				);
			`,
		}))

		makeSCD2Asset := func(sql string) *pipeline.Asset {
			return &pipeline.Asset{
				Name:            "demo.scd2_union_guard",
				Type:            pipeline.AssetTypeDuckDBQuery,
				SchemaEvolution: &pipeline.SchemaEvolution{Enabled: boolPtr(true)},
				Materialization: pipeline.Materialization{
					Type:     pipeline.MaterializationTypeTable,
					Strategy: pipeline.MaterializationStrategySCD2ByColumn,
				},
				Columns: []pipeline.Column{
					{Name: "id", Type: "INT", PrimaryKey: true},
					{Name: "tags", Type: "INT[]"},
					{Name: "big_val", Type: "HUGEINT"},
					{Name: "count_val", Type: "INT"},
				},
				ExecutableFile: pipeline.ExecutableFile{Content: sql},
			}
		}

		err := op.RunTask(ctx, p, makeSCD2Asset(`SELECT 2 AS id, CAST([1, 2, 3] AS INT[3]) AS tags, CAST(10 AS HUGEINT) AS big_val, 7 AS count_val`))
		require.Error(t, err)
		assert.Contains(t, err.Error(), "UNION ALL")

		err = op.RunTask(ctx, p, makeSCD2Asset(`SELECT 2 AS id, CAST([1, 2, 9000000000] AS BIGINT[3]) AS tags, CAST(10 AS HUGEINT) AS big_val, 7 AS count_val`))
		require.Error(t, err)
		assert.Contains(t, err.Error(), "UNION ALL")

		err = op.RunTask(ctx, p, makeSCD2Asset(`SELECT 2 AS id, [1, 2] AS tags, CAST(42 AS DECIMAL(9,0)) AS big_val, 7 AS count_val`))
		require.Error(t, err)
		assert.Contains(t, err.Error(), "UNION ALL")

		require.NoError(t, op.RunTask(ctx, p, makeSCD2Asset(`SELECT 2 AS id, [1, 2, 3] AS tags, CAST(99 AS HUGEINT) AS big_val, CAST(42 AS DECIMAL(5,0)) AS count_val`)))

		cols, _, err := newDuckDBEvolutionEngine(client).ExistingColumns(ctx, "demo.scd2_union_guard")
		require.NoError(t, err)
		assert.Equal(t, []string{"INTEGER", "INTEGER[]", "HUGEINT", "DECIMAL(10,0)", "TIMESTAMP WITH TIME ZONE", "TIMESTAMP WITH TIME ZONE", "BOOLEAN"}, columnTypes(cols))
	})
}

func TestSchemaEvolution_DuckDB_CatalogConstraints(t *testing.T) {
	t.Parallel()
	p := &pipeline.Pipeline{Name: "test-pipeline"}

	t.Run("indexed column, expression index, referencing FK, and CHECK constraint all return IncompatibleSchemaError", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()
		client := newSchemaEvolutionTestClient(t)
		op := newSchemaEvolutionTestOperator(t, client, false)

		require.NoError(t, client.RunQueryWithoutResult(ctx, &query.Query{
			Query: `
CREATE SCHEMA demo;
CREATE TABLE demo.idx_tbl (id INT, v INT);
CREATE INDEX ix_id ON demo.idx_tbl(id);

CREATE TABLE demo.expr_idx_tbl (id INT, b INT);
CREATE INDEX ix_expr ON demo.expr_idx_tbl((b + 1));

CREATE TABLE demo.fk_parent (id INT PRIMARY KEY, v INT);
CREATE TABLE demo.fk_child (cid INT, pid INT REFERENCES demo.fk_parent(id));

CREATE TABLE demo.chk_tbl (id INT, v INT CHECK (v > 0));
`,
		}))

		for _, tc := range []struct {
			table string
			sql   string
			want  string
		}{
			{"demo.idx_tbl", "SELECT 1 AS id, CAST(10 AS BIGINT) AS v", "index depends on it"},
			{"demo.expr_idx_tbl", "SELECT 1 AS id, CAST(10 AS BIGINT) AS b", "index depends on it"},
			{"demo.fk_parent", "SELECT 1 AS id, CAST(10 AS BIGINT) AS v", "FOREIGN KEY"},
			{"demo.chk_tbl", "SELECT 1 AS id, CAST(10 AS BIGINT) AS v", "CHECK constraint"},
		} {
			err := op.RunTask(ctx, p, &pipeline.Asset{
				Name:            tc.table,
				Type:            pipeline.AssetTypeDuckDBQuery,
				SchemaEvolution: &pipeline.SchemaEvolution{Enabled: boolPtr(true)},
				Materialization: pipeline.Materialization{Type: pipeline.MaterializationTypeTable, Strategy: pipeline.MaterializationStrategyAppend},
				ExecutableFile:  pipeline.ExecutableFile{Content: tc.sql},
			})
			var incompatErr *schemaevolution.IncompatibleSchemaError
			require.ErrorAs(t, err, &incompatErr)
			assert.Contains(t, err.Error(), tc.want)
		}
	})

	t.Run("FK on child column and 2-part catalog.table name are detected as IncompatibleSchemaError", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()
		client := newSchemaEvolutionTestClient(t)
		op := newSchemaEvolutionTestOperator(t, client, false)

		require.NoError(t, client.RunQueryWithoutResult(ctx, &query.Query{
			Query: `
CREATE SCHEMA demo;
CREATE TABLE demo.p (id INT PRIMARY KEY);
CREATE TABLE demo.c (cid INT, pid INT REFERENCES demo.p(id));
`,
		}))

		err := op.RunTask(ctx, p, &pipeline.Asset{
			Name:            "demo.c",
			Type:            pipeline.AssetTypeDuckDBQuery,
			SchemaEvolution: &pipeline.SchemaEvolution{Enabled: boolPtr(true)},
			Materialization: pipeline.Materialization{Type: pipeline.MaterializationTypeTable, Strategy: pipeline.MaterializationStrategyAppend},
			ExecutableFile:  pipeline.ExecutableFile{Content: "SELECT 1 AS cid, CAST(1 AS BIGINT) AS pid"},
		})
		var incompatErr *schemaevolution.IncompatibleSchemaError
		require.ErrorAs(t, err, &incompatErr)
		assert.Contains(t, err.Error(), "FOREIGN KEY")

		extPath := filepath.Join(t.TempDir(), "ext.duckdb")
		extClient, err := NewClient(Config{Path: extPath})
		require.NoError(t, err)
		require.NoError(t, extClient.RunQueryWithoutResult(ctx, &query.Query{
			Query: "CREATE TABLE items (id INT, v INT); CREATE INDEX ix_ext ON items(id);",
		}))
		extClient.Close()

		_, _, err = executeWithDuckDBSchemaEvolution(ctx, duckDBEvolutionRequest{
			Conn:  client,
			Hooks: pipeline.Hooks{Pre: []pipeline.Hook{{Query: "ATTACH '" + extPath + "' AS extcat"}}},
			Asset: &pipeline.Asset{
				Name:            "extcat.items",
				Type:            pipeline.AssetTypeDuckDBQuery,
				SchemaEvolution: &pipeline.SchemaEvolution{Enabled: boolPtr(true)},
				Materialization: pipeline.Materialization{Type: pipeline.MaterializationTypeTable, Strategy: pipeline.MaterializationStrategyAppend},
			},
			InspectQuery: "SELECT 1 AS id, CAST(10 AS BIGINT) AS v",
			MainSQL:      "INSERT INTO extcat.items BY NAME SELECT 1 AS id, CAST(10 AS BIGINT) AS v",
		})
		require.ErrorAs(t, err, &incompatErr)
		assert.Contains(t, err.Error(), "index depends on it")
	})

	t.Run("quoted schema, table, and comma-containing column names in FK and CHECK constraints are detected accurately", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()
		client := newSchemaEvolutionTestClient(t)

		require.NoError(t, client.RunQueryWithoutResult(ctx, &query.Query{
			Query: `
				CREATE SCHEMA "my.schema";
				CREATE TABLE "my.schema"."parent table" (id INT PRIMARY KEY, score INT);
				CREATE TABLE "my.schema"."child.table" (
					parent_id INT REFERENCES "my.schema"."parent table"(id),
					"a,b" INT CHECK ("a,b" > 0)
				);
			`,
		}))

		parentAsset := &pipeline.Asset{
			Name:            `"my.schema"."parent table"`,
			Type:            pipeline.AssetTypeDuckDBQuery,
			SchemaEvolution: &pipeline.SchemaEvolution{Enabled: boolPtr(true)},
			Materialization: pipeline.Materialization{
				Type:     pipeline.MaterializationTypeTable,
				Strategy: pipeline.MaterializationStrategyAppend,
			},
		}
		_, _, err := executeWithDuckDBSchemaEvolution(ctx, duckDBEvolutionRequest{
			Conn:         client,
			Asset:        parentAsset,
			InspectQuery: "SELECT 1 AS id, CAST(10 AS BIGINT) AS score",
			MainSQL:      `INSERT INTO "my.schema"."parent table" BY NAME SELECT 1 AS id, CAST(10 AS BIGINT) AS score`,
		})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "FOREIGN KEY constraint references it")

		childAsset := &pipeline.Asset{
			Name:            `"my.schema"."child.table"`,
			Type:            pipeline.AssetTypeDuckDBQuery,
			SchemaEvolution: &pipeline.SchemaEvolution{Enabled: boolPtr(true)},
			Materialization: pipeline.Materialization{
				Type:     pipeline.MaterializationTypeTable,
				Strategy: pipeline.MaterializationStrategyAppend,
			},
		}
		_, _, err = executeWithDuckDBSchemaEvolution(ctx, duckDBEvolutionRequest{
			Conn:         client,
			Asset:        childAsset,
			InspectQuery: `SELECT 1 AS parent_id, CAST(5 AS BIGINT) AS "a,b"`,
			MainSQL:      `INSERT INTO "my.schema"."child.table" BY NAME SELECT 1 AS parent_id, CAST(5 AS BIGINT) AS "a,b"`,
		})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "CHECK constraint")
	})
}
