package schemaevolution_test

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/bruin-data/bruin/pkg/schemaevolution"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func boolPtr(v bool) *bool { return &v }

type fakeDialect struct {
	caseSensitive bool
}

func (d fakeDialect) ColumnKey(name string) string {
	trimmed := strings.TrimSpace(name)
	if d.caseSensitive {
		return trimmed
	}
	return strings.ToLower(trimmed)
}

func (fakeDialect) NormalizeType(t string) string {
	u := strings.ToUpper(strings.TrimSpace(t))
	if u == "INT" {
		return "INTEGER"
	}
	return u
}

func (d fakeDialect) ValidateNewColumn(col schemaevolution.Column) (schemaevolution.Column, string, bool) {
	if col.UntypedNull {
		return schemaevolution.Column{}, "cannot infer type from untyped NULL", false
	}
	n := d.NormalizeType(col.Type)
	if n == "" || n == "NULL" {
		return schemaevolution.Column{}, "cannot infer type from NULL", false
	}
	out := col
	out.Type = strings.TrimSpace(col.Type)
	return out, "", true
}

func (d fakeDialect) ResolveTypeChange(existing, incoming schemaevolution.Column) (schemaevolution.TypeResolution, string, string) {
	if incoming.UntypedNull {
		return schemaevolution.TypeChangeNone, "", ""
	}
	cur := d.NormalizeType(existing.Type)
	inc := d.NormalizeType(incoming.Type)
	if cur == inc || inc == "NULL" {
		return schemaevolution.TypeChangeNone, "", ""
	}
	if cur == "INTEGER" && inc == "BIGINT" {
		if existing.PrimaryKey || existing.Unique {
			return schemaevolution.TypeChangeIncompatible, "", "cannot widen key column"
		}
		return schemaevolution.TypeChangeWiden, "BIGINT", ""
	}
	if cur == "BIGINT" && inc == "INTEGER" {
		return schemaevolution.TypeChangeNone, "", ""
	}
	return schemaevolution.TypeChangeIncompatible, "", ""
}

type fakeEngine struct {
	fakeDialect
	existing      []schemaevolution.Column
	exists        bool
	existingErr   error
	incoming      []schemaevolution.Column
	incomingErr   error
	declaredFn    func(*pipeline.Asset) ([]schemaevolution.Column, error)
	queryCalls    int
	declaredCalls int
}

func (f *fakeEngine) ExistingColumns(ctx context.Context, tableName string) ([]schemaevolution.Column, bool, error) {
	return f.existing, f.exists, f.existingErr
}

func (f *fakeEngine) QueryColumns(ctx context.Context, queryStr string) ([]schemaevolution.Column, error) {
	f.queryCalls++
	return f.incoming, f.incomingErr
}

func (f *fakeEngine) DeclaredColumns(ctx context.Context, asset *pipeline.Asset) ([]schemaevolution.Column, error) {
	f.declaredCalls++
	if f.declaredFn != nil {
		return f.declaredFn(asset)
	}
	return schemaevolution.ColumnsFromAsset(asset), nil
}

func TestShouldEvolve(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		asset       *pipeline.Asset
		fullRefresh bool
		want        bool
	}{
		{name: "nil asset", asset: nil, want: false},
		{
			name: "schema_evolution not set",
			asset: &pipeline.Asset{
				Materialization: pipeline.Materialization{
					Type:     pipeline.MaterializationTypeTable,
					Strategy: pipeline.MaterializationStrategyAppend,
				},
			},
			want: false,
		},
		{
			name: "schema_evolution disabled",
			asset: &pipeline.Asset{
				SchemaEvolution: &pipeline.SchemaEvolution{Enabled: boolPtr(false)},
				Materialization: pipeline.Materialization{
					Type:     pipeline.MaterializationTypeTable,
					Strategy: pipeline.MaterializationStrategyAppend,
				},
			},
			want: false,
		},
		{
			name: "view materialization",
			asset: &pipeline.Asset{
				SchemaEvolution: &pipeline.SchemaEvolution{Enabled: boolPtr(true)},
				Materialization: pipeline.Materialization{Type: pipeline.MaterializationTypeView},
			},
			want: false,
		},
		{
			name: "create+replace strategy",
			asset: &pipeline.Asset{
				SchemaEvolution: &pipeline.SchemaEvolution{Enabled: boolPtr(true)},
				Materialization: pipeline.Materialization{
					Type:     pipeline.MaterializationTypeTable,
					Strategy: pipeline.MaterializationStrategyCreateReplace,
				},
			},
			want: false,
		},
		{
			name: "default empty strategy (create+replace)",
			asset: &pipeline.Asset{
				SchemaEvolution: &pipeline.SchemaEvolution{Enabled: boolPtr(true)},
				Materialization: pipeline.Materialization{Type: pipeline.MaterializationTypeTable},
			},
			want: false,
		},
		{
			name: "append strategy without full refresh",
			asset: &pipeline.Asset{
				SchemaEvolution: &pipeline.SchemaEvolution{Enabled: boolPtr(true)},
				Materialization: pipeline.Materialization{
					Type:     pipeline.MaterializationTypeTable,
					Strategy: pipeline.MaterializationStrategyAppend,
				},
			},
			want: true,
		},
		{
			name: "delete+insert strategy with full refresh is skipped",
			asset: &pipeline.Asset{
				SchemaEvolution: &pipeline.SchemaEvolution{Enabled: boolPtr(true)},
				Materialization: pipeline.Materialization{
					Type:     pipeline.MaterializationTypeTable,
					Strategy: pipeline.MaterializationStrategyDeleteInsert,
				},
			},
			fullRefresh: true,
			want:        false,
		},
		{
			name: "ddl strategy still evolves on full refresh",
			asset: &pipeline.Asset{
				SchemaEvolution: &pipeline.SchemaEvolution{Enabled: boolPtr(true)},
				Materialization: pipeline.Materialization{
					Type:     pipeline.MaterializationTypeTable,
					Strategy: pipeline.MaterializationStrategyDDL,
				},
			},
			fullRefresh: true,
			want:        true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, schemaevolution.ShouldEvolve(tt.asset, tt.fullRefresh))
		})
	}
}

func TestDiff(t *testing.T) {
	t.Parallel()

	t.Run("adds new columns with metadata, widens compatible types, keeps omitted columns, preserves casing", func(t *testing.T) {
		t.Parallel()
		existing := []schemaevolution.Column{
			{Name: "ID", Type: "INT"},
			{Name: "legacy_col", Type: "VARCHAR"},
			{Name: "count", Type: "BIGINT"},
		}
		incoming := []schemaevolution.Column{
			{Name: "id", Type: "BIGINT"},
			{Name: "count", Type: "INTEGER"},
			{Name: "email", Type: "VARCHAR", Default: "'none'", Collation: "en_US"},
		}

		plan, err := schemaevolution.Diff(fakeDialect{}, "demo.users", existing, incoming)
		require.NoError(t, err)
		require.False(t, plan.IsEmpty())
		require.Len(t, plan.Actions, 2)

		assert.Equal(t, schemaevolution.ActionAlterColumnType, plan.Actions[0].Type)
		assert.Equal(t, "ID", plan.Actions[0].ColumnName)
		assert.Equal(t, "INTEGER", plan.Actions[0].FromType)
		assert.Equal(t, "BIGINT", plan.Actions[0].ToType)

		assert.Equal(t, schemaevolution.ActionAddColumn, plan.Actions[1].Type)
		assert.Equal(t, "email", plan.Actions[1].ColumnName)
		assert.Equal(t, "'none'", plan.Actions[1].Column.Default)
		assert.Equal(t, "en_US", plan.Actions[1].Column.Collation)
	})

	t.Run("supports dialect-controlled case-sensitive column matching", func(t *testing.T) {
		t.Parallel()
		existing := []schemaevolution.Column{{Name: "Id", Type: "INTEGER"}}
		incoming := []schemaevolution.Column{{Name: "id", Type: "VARCHAR"}}

		plan, err := schemaevolution.Diff(fakeDialect{caseSensitive: true}, "demo.users", existing, incoming)
		require.NoError(t, err)
		require.Len(t, plan.Actions, 1)
		assert.Equal(t, schemaevolution.ActionAddColumn, plan.Actions[0].Type)
		assert.Equal(t, "id", plan.Actions[0].ColumnName)
	})

	t.Run("returns combined error on incompatible type changes, key constraints, and untyped null additions", func(t *testing.T) {
		t.Parallel()
		existing := []schemaevolution.Column{
			{Name: "id", Type: "INTEGER", PrimaryKey: true},
			{Name: "name", Type: "VARCHAR"},
		}
		incoming := []schemaevolution.Column{
			{Name: "id", Type: "BIGINT"},
			{Name: "name", Type: "INTEGER"},
			{Name: "unknown_col", Type: "INTEGER", UntypedNull: true},
		}

		plan, err := schemaevolution.Diff(fakeDialect{}, "demo.users", existing, incoming)
		require.Nil(t, plan)
		require.Error(t, err)

		var incompatErr *schemaevolution.IncompatibleSchemaError
		require.ErrorAs(t, err, &incompatErr)
		assert.Len(t, incompatErr.Changes, 3)
		assert.Contains(t, err.Error(), `column "id" (INTEGER -> BIGINT): cannot widen key column`)
		assert.Contains(t, err.Error(), `column "name" (VARCHAR -> INTEGER)`)
		assert.Contains(t, err.Error(), `column "unknown_col": cannot infer type from untyped NULL`)
	})

	t.Run("no changes produces empty plan", func(t *testing.T) {
		t.Parallel()
		existing := []schemaevolution.Column{{Name: "id", Type: "INTEGER"}}
		incoming := []schemaevolution.Column{{Name: "id", Type: "INT"}}

		plan, err := schemaevolution.Diff(fakeDialect{}, "demo.users", existing, incoming)
		require.NoError(t, err)
		assert.True(t, plan.IsEmpty())
	})
}

func TestBuildPlan(t *testing.T) {
	t.Parallel()

	t.Run("skips when table does not exist yet", func(t *testing.T) {
		t.Parallel()
		engine := &fakeEngine{exists: false}
		asset := &pipeline.Asset{
			Name:            "demo.users",
			SchemaEvolution: &pipeline.SchemaEvolution{Enabled: boolPtr(true)},
			Materialization: pipeline.Materialization{
				Type:     pipeline.MaterializationTypeTable,
				Strategy: pipeline.MaterializationStrategyAppend,
			},
		}

		plan, err := schemaevolution.BuildPlan(t.Context(), engine, asset, asset.Name, "SELECT 1 AS id", false)
		require.NoError(t, err)
		assert.True(t, plan.IsEmpty())
		assert.Equal(t, 0, engine.queryCalls)
	})

	t.Run("uses ResolveDeclaredColumns for DDL strategy without calling QueryColumns", func(t *testing.T) {
		t.Parallel()
		engine := &fakeEngine{
			exists:   true,
			existing: []schemaevolution.Column{{Name: "id", Type: "INTEGER"}},
		}
		asset := &pipeline.Asset{
			Name:            "demo.users",
			SchemaEvolution: &pipeline.SchemaEvolution{Enabled: boolPtr(true)},
			Materialization: pipeline.Materialization{
				Type:     pipeline.MaterializationTypeTable,
				Strategy: pipeline.MaterializationStrategyDDL,
			},
			Columns: []pipeline.Column{
				{Name: "id", Type: "BIGINT"},
				{Name: "email", Type: "VARCHAR", Default: "'unknown'"},
			},
		}

		plan, err := schemaevolution.BuildPlan(t.Context(), engine, asset, asset.Name, "", true)
		require.NoError(t, err)
		assert.Equal(t, 0, engine.queryCalls)
		assert.Equal(t, 1, engine.declaredCalls)
		assert.Len(t, plan.Actions, 2)
		assert.Equal(t, "'unknown'", plan.Actions[1].Column.Default)
	})

	t.Run("omits --full-refresh hint for DDL, SCD2, DataVault, and refresh_restricted assets", func(t *testing.T) {
		t.Parallel()

		for _, tc := range []struct {
			name              string
			strategy          pipeline.MaterializationStrategy
			refreshRestricted *bool
			cols              []pipeline.Column
		}{
			{
				name:     "ddl",
				strategy: pipeline.MaterializationStrategyDDL,
				cols:     []pipeline.Column{{Name: "id", Type: "VARCHAR"}},
			},
			{
				name:     "scd2_by_column",
				strategy: pipeline.MaterializationStrategySCD2ByColumn,
				cols:     []pipeline.Column{{Name: "id", Type: "VARCHAR", PrimaryKey: true}},
			},
			{
				name:     "datavault_hub",
				strategy: pipeline.MaterializationStrategyDataVaultHub,
				cols:     []pipeline.Column{{Name: "id", Type: "VARCHAR", PrimaryKey: true}},
			},
			{
				name:              "refresh_restricted",
				strategy:          pipeline.MaterializationStrategyAppend,
				refreshRestricted: boolPtr(true),
			},
		} {
			t.Run(tc.name, func(t *testing.T) {
				t.Parallel()
				eng := &fakeEngine{
					exists:   true,
					existing: []schemaevolution.Column{{Name: "id", Type: "INTEGER"}},
					incoming: []schemaevolution.Column{{Name: "id", Type: "VARCHAR"}},
				}
				asset := &pipeline.Asset{
					Name:              "demo.users",
					RefreshRestricted: tc.refreshRestricted,
					SchemaEvolution:   &pipeline.SchemaEvolution{Enabled: boolPtr(true)},
					Materialization: pipeline.Materialization{
						Type:     pipeline.MaterializationTypeTable,
						Strategy: tc.strategy,
					},
					Columns: tc.cols,
				}
				_, err := schemaevolution.BuildPlan(t.Context(), eng, asset, asset.Name, "SELECT 'x' AS id", false)
				require.Error(t, err)
				assert.NotContains(t, err.Error(), "--full-refresh")
			})
		}
	})

	t.Run("filters incoming query columns to asset.Columns for declared-column strategies", func(t *testing.T) {
		t.Parallel()
		engine := &fakeEngine{
			exists:   true,
			existing: []schemaevolution.Column{{Name: "id", Type: "INTEGER"}},
			incoming: []schemaevolution.Column{
				{Name: "id", Type: "BIGINT"},
				{Name: "email", Type: "VARCHAR"},
				{Name: "undeclared_debug", Type: "VARCHAR"},
			},
		}
		asset := &pipeline.Asset{
			Name:            "demo.users",
			SchemaEvolution: &pipeline.SchemaEvolution{Enabled: boolPtr(true)},
			Materialization: pipeline.Materialization{
				Type:     pipeline.MaterializationTypeTable,
				Strategy: pipeline.MaterializationStrategyMerge,
			},
			Columns: []pipeline.Column{
				{Name: "id", Type: "INTEGER", PrimaryKey: true},
				{Name: "email", Type: "VARCHAR"},
			},
		}

		plan, err := schemaevolution.BuildPlan(t.Context(), engine, asset, asset.Name, "SELECT ...", false)
		require.NoError(t, err)
		require.Len(t, plan.Actions, 2)
		assert.Equal(t, "id", plan.Actions[0].ColumnName)
		assert.Equal(t, "BIGINT", plan.Actions[0].ToType)
		assert.Equal(t, "email", plan.Actions[1].ColumnName)
	})

	t.Run("preserves query column UntypedNull when merging declared columns", func(t *testing.T) {
		t.Parallel()
		engine := &fakeEngine{
			exists:   true,
			existing: []schemaevolution.Column{{Name: "id", Type: "INTEGER"}},
			incoming: []schemaevolution.Column{
				{Name: "id", Type: "INTEGER"},
				{Name: "meta", Type: "INTEGER", UntypedNull: true},
			},
		}
		asset := &pipeline.Asset{
			Name:            "demo.users",
			SchemaEvolution: &pipeline.SchemaEvolution{Enabled: boolPtr(true)},
			Materialization: pipeline.Materialization{
				Type:     pipeline.MaterializationTypeTable,
				Strategy: pipeline.MaterializationStrategyMerge,
			},
			Columns: []pipeline.Column{
				{Name: "id", Type: "INTEGER", PrimaryKey: true},
				{Name: "meta", Type: "VARCHAR"},
			},
		}

		_, err := schemaevolution.BuildPlan(t.Context(), engine, asset, asset.Name, "SELECT ...", false)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "untyped NULL")
	})

	t.Run("propagates inspection errors", func(t *testing.T) {
		t.Parallel()
		engine := &fakeEngine{
			exists:      true,
			existing:    []schemaevolution.Column{{Name: "id", Type: "INTEGER"}},
			incomingErr: errors.New("syntax error"),
		}
		asset := &pipeline.Asset{
			Name:            "demo.users",
			SchemaEvolution: &pipeline.SchemaEvolution{Enabled: boolPtr(true)},
			Materialization: pipeline.Materialization{
				Type:     pipeline.MaterializationTypeTable,
				Strategy: pipeline.MaterializationStrategyAppend,
			},
		}

		_, err := schemaevolution.BuildPlan(t.Context(), engine, asset, asset.Name, "SELECT BAD", false)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "syntax error")
	})

	t.Run("Validate checks existing table compatibility and inspects query when table does not exist yet", func(t *testing.T) {
		t.Parallel()

		missingTableEngine := &fakeEngine{
			exists:   false,
			incoming: []schemaevolution.Column{{Name: "id", Type: "INTEGER"}},
		}
		asset := &pipeline.Asset{
			Name:            "analytics.events",
			SchemaEvolution: &pipeline.SchemaEvolution{Enabled: boolPtr(true)},
			Materialization: pipeline.Materialization{
				Type:     pipeline.MaterializationTypeTable,
				Strategy: pipeline.MaterializationStrategyAppend,
			},
		}
		require.NoError(t, schemaevolution.Validate(t.Context(), missingTableEngine, asset, "analytics.events", "SELECT 1 AS id"))
		assert.Equal(t, 1, missingTableEngine.queryCalls)
	})

	t.Run("merge fails when the query does not return a declared column", func(t *testing.T) {
		t.Parallel()

		engine := &fakeEngine{
			exists:   true,
			existing: []schemaevolution.Column{{Name: "id", Type: "INTEGER"}, {Name: "email", Type: "VARCHAR"}},
			incoming: []schemaevolution.Column{{Name: "id", Type: "INTEGER"}},
		}
		asset := &pipeline.Asset{
			Name:            "analytics.users",
			SchemaEvolution: &pipeline.SchemaEvolution{Enabled: boolPtr(true)},
			Materialization: pipeline.Materialization{
				Type:     pipeline.MaterializationTypeTable,
				Strategy: pipeline.MaterializationStrategyMerge,
			},
			Columns: []pipeline.Column{
				{Name: "id", Type: "INTEGER", PrimaryKey: true},
				{Name: "email", Type: "VARCHAR"},
			},
		}
		err := schemaevolution.Validate(t.Context(), engine, asset, "analytics.users", "SELECT 1 AS id")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "does not return declared column(s) email")
	})

	t.Run("DDL uses default ColumnsFromAsset normalization when engine does not implement DeclaredColumnResolver", func(t *testing.T) {
		t.Parallel()

		onlyTwoMethods := &minimalTwoMethodEngine{
			exists:   true,
			existing: []schemaevolution.Column{{Name: "id", Type: "INTEGER"}},
		}
		ddlAsset := &pipeline.Asset{
			Name:            "analytics.orders",
			SchemaEvolution: &pipeline.SchemaEvolution{Enabled: boolPtr(true)},
			Materialization: pipeline.Materialization{
				Type:     pipeline.MaterializationTypeTable,
				Strategy: pipeline.MaterializationStrategyDDL,
			},
			Columns: []pipeline.Column{
				{Name: "id", Type: "INT"},
				{Name: "note", Type: "VARCHAR"},
			},
		}
		plan, err := schemaevolution.BuildPlan(t.Context(), onlyTwoMethods, ddlAsset, "analytics.orders", "", false)
		require.NoError(t, err)
		require.Len(t, plan.Actions, 1)
		assert.Equal(t, "note", plan.Actions[0].ColumnName)
	})
}

type minimalTwoMethodEngine struct {
	fakeDialect
	existing []schemaevolution.Column
	exists   bool
}

func (m *minimalTwoMethodEngine) ExistingColumns(_ context.Context, _ string) ([]schemaevolution.Column, bool, error) {
	return m.existing, m.exists, nil
}

func (m *minimalTwoMethodEngine) QueryColumns(_ context.Context, _ string) ([]schemaevolution.Column, error) {
	return nil, nil
}

func TestLogPlan(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	plan := &schemaevolution.Plan{
		Actions: []schemaevolution.Action{
			{Type: schemaevolution.ActionAddColumn, ColumnName: "email", ToType: "VARCHAR"},
			{Type: schemaevolution.ActionAlterColumnType, ColumnName: "id", FromType: "INTEGER", ToType: "BIGINT"},
		},
	}
	schemaevolution.LogPlan(&buf, "demo.users", plan)
	assert.Equal(t, "Schema evolution on demo.users: adding column \"email\" (VARCHAR), widening column \"id\" (INTEGER -> BIGINT)\n", buf.String())
}
