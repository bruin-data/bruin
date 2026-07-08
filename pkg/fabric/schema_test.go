package fabric

import (
	"context"
	"errors"
	"testing"

	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/bruin-data/bruin/pkg/query"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type recordingSchemaRunner struct {
	queries []string
	err     error
}

func (r *recordingSchemaRunner) RunQueryWithoutResult(_ context.Context, q *query.Query) error {
	r.queries = append(r.queries, q.Query)
	return r.err
}

func TestSchemaCreator_CreateSchemaIfNotExist(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		assetName string
		wantQuery string
		wantRun   bool
	}{
		{
			name:      "two-part name preserves schema case",
			assetName: "bruin_Test.Products",
			wantQuery: "IF SCHEMA_ID(N'bruin_Test') IS NULL\n    EXEC(N'CREATE SCHEMA [bruin_Test]')",
			wantRun:   true,
		},
		{
			name:      "three-part name qualifies schema with catalog",
			assetName: "Warehouse.Sales.Orders",
			wantQuery: "IF NOT EXISTS (SELECT 1 FROM [Warehouse].sys.schemas WHERE name = N'Sales')\n    EXEC(N'USE [Warehouse]; CREATE SCHEMA [Sales]')",
			wantRun:   true,
		},
		{
			name:      "schema name escapes literals and brackets",
			assetName: "odd']schema.Orders",
			wantQuery: "IF SCHEMA_ID(N'odd'']schema') IS NULL\n    EXEC(N'CREATE SCHEMA [odd'']]schema]')",
			wantRun:   true,
		},
		{
			name:      "single-part name has no schema",
			assetName: "Orders",
			wantRun:   false,
		},
		{
			name:      "empty component is skipped",
			assetName: "Warehouse..Orders",
			wantRun:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			runner := &recordingSchemaRunner{}
			creator := NewSchemaCreator()

			err := creator.CreateSchemaIfNotExist(t.Context(), runner, &pipeline.Asset{Name: tt.assetName})
			require.NoError(t, err)

			if !tt.wantRun {
				assert.Empty(t, runner.queries)
				return
			}
			require.Len(t, runner.queries, 1)
			assert.Equal(t, tt.wantQuery, runner.queries[0])
		})
	}
}

func TestSchemaCreator_CreateSchemaIfNotExistCachesSchemas(t *testing.T) {
	t.Parallel()

	runner := &recordingSchemaRunner{}
	creator := NewSchemaCreator()
	asset := &pipeline.Asset{Name: "bruin_test.Products"}

	require.NoError(t, creator.CreateSchemaIfNotExist(t.Context(), runner, asset))
	require.NoError(t, creator.CreateSchemaIfNotExist(t.Context(), runner, asset))

	assert.Len(t, runner.queries, 1)
}

func TestSchemaCreator_CreateSchemaIfNotExistCachesQualifiedSchemasSeparately(t *testing.T) {
	t.Parallel()

	runner := &recordingSchemaRunner{}
	creator := NewSchemaCreator()

	require.NoError(t, creator.CreateSchemaIfNotExist(t.Context(), runner, &pipeline.Asset{Name: "DB1.Sales.Orders"}))
	require.NoError(t, creator.CreateSchemaIfNotExist(t.Context(), runner, &pipeline.Asset{Name: "DB2.Sales.Orders"}))

	require.Len(t, runner.queries, 2)
	assert.Equal(t, "IF NOT EXISTS (SELECT 1 FROM [DB1].sys.schemas WHERE name = N'Sales')\n    EXEC(N'USE [DB1]; CREATE SCHEMA [Sales]')", runner.queries[0])
	assert.Equal(t, "IF NOT EXISTS (SELECT 1 FROM [DB2].sys.schemas WHERE name = N'Sales')\n    EXEC(N'USE [DB2]; CREATE SCHEMA [Sales]')", runner.queries[1])
}

func TestSchemaCreator_CreateSchemaIfNotExistReturnsQueryError(t *testing.T) {
	t.Parallel()

	runner := &recordingSchemaRunner{err: errors.New("boom")}
	creator := NewSchemaCreator()

	err := creator.CreateSchemaIfNotExist(t.Context(), runner, &pipeline.Asset{Name: "bruin_test.Products"})

	require.Error(t, err)
	require.ErrorContains(t, err, "failed to create or ensure schema: bruin_test")
	require.ErrorContains(t, err, "boom")
}
