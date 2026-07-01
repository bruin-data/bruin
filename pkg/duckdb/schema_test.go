//go:build !bruin_no_duckdb

package duck

import (
	"context"
	"sync"
	"testing"

	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/bruin-data/bruin/pkg/query"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

type mockSchemaQR struct {
	mock.Mock
}

func (m *mockSchemaQR) RunQueryWithoutResult(ctx context.Context, q *query.Query) error {
	args := m.Called(ctx, q)
	return args.Error(0)
}

func TestDuckDBSchemaCreator_CreateSchemaIfNotExist(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		assetName string
		wantQuery string // "" => no query expected (early return)
	}{
		{name: "two-part lowercases the schema", assetName: "Analytics.Events", wantQuery: "CREATE SCHEMA IF NOT EXISTS analytics"},
		{name: "three-part qualifies schema with catalog", assetName: "Cat.Analytics.Events", wantQuery: "CREATE SCHEMA IF NOT EXISTS cat.analytics"},
		{name: "single component returns early", assetName: "events", wantQuery: ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			qr := new(mockSchemaQR)
			if tt.wantQuery != "" {
				qr.On("RunQueryWithoutResult", mock.Anything, &query.Query{Query: tt.wantQuery}).Return(nil)
			}
			creator := &DuckDBSchemaCreator{schemaNameCache: &sync.Map{}}
			require.NoError(t, creator.CreateSchemaIfNotExist(t.Context(), qr, &pipeline.Asset{Name: tt.assetName}))
			qr.AssertExpectations(t)
		})
	}
}

func TestDuckDBSchemaCreator_DoesNotDedupeAcrossCatalogs(t *testing.T) {
	t.Parallel()

	qr := new(mockSchemaQR)
	// Same schema name "public" in two different catalogs must both be created.
	qr.On("RunQueryWithoutResult", mock.Anything, &query.Query{Query: "CREATE SCHEMA IF NOT EXISTS cata.public"}).Return(nil)
	qr.On("RunQueryWithoutResult", mock.Anything, &query.Query{Query: "CREATE SCHEMA IF NOT EXISTS catb.public"}).Return(nil)

	creator := &DuckDBSchemaCreator{schemaNameCache: &sync.Map{}}
	require.NoError(t, creator.CreateSchemaIfNotExist(t.Context(), qr, &pipeline.Asset{Name: "cata.public.t1"}))
	require.NoError(t, creator.CreateSchemaIfNotExist(t.Context(), qr, &pipeline.Asset{Name: "catb.public.t2"}))

	qr.AssertExpectations(t)
	qr.AssertNumberOfCalls(t, "RunQueryWithoutResult", 2)
}
