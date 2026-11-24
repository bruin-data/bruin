package ansisql

import (
	"testing"

	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBuildTruncateInsertQuery(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		asset    *pipeline.Asset
		query    string
		expected string
	}{
		{
			name: "basic truncate+insert",
			asset: &pipeline.Asset{
				Name: "users",
			},
			query: "SELECT id, name FROM source_users",
			expected: `BEGIN TRANSACTION;
TRUNCATE TABLE users;
INSERT INTO users SELECT id, name FROM source_users;
COMMIT;`,
		},
		{
			name: "query with trailing semicolon",
			asset: &pipeline.Asset{
				Name: "products",
			},
			query: "SELECT * FROM source_products;",
			expected: `BEGIN TRANSACTION;
TRUNCATE TABLE products;
INSERT INTO products SELECT * FROM source_products;
COMMIT;`,
		},
		{
			name: "schema-qualified table name",
			asset: &pipeline.Asset{
				Name: "analytics.daily_metrics",
			},
			query: "SELECT date, revenue FROM metrics",
			expected: `BEGIN TRANSACTION;
TRUNCATE TABLE analytics.daily_metrics;
INSERT INTO analytics.daily_metrics SELECT date, revenue FROM metrics;
COMMIT;`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			result, err := BuildTruncateInsertQuery(tt.asset, tt.query)
			require.NoError(t, err)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestAddIncrementalPredicate(t *testing.T) {
	t.Parallel()

	base := []string{"target.id = source.id"}

	assert.Equal(t, base, AddIncrementalPredicate(base, ""))
	assert.Equal(t, base, AddIncrementalPredicate(base, "   "))
	assert.Equal(
		t,
		[]string{"target.id = source.id", "(target.event_date >= DATE '2026-07-01')"},
		AddIncrementalPredicate(base, "  target.event_date >= DATE '2026-07-01'  "),
	)
}

func TestBuildIncrementalDelete(t *testing.T) {
	t.Parallel()

	assert.Equal(t, "DELETE FROM my.asset WHERE id in (1)", BuildIncrementalDelete("my.asset", "id in (1)", ""))
	assert.Equal(t, "DELETE FROM my.asset WHERE id in (1)", BuildIncrementalDelete("my.asset", "id in (1)", "  "))
	assert.Equal(
		t,
		"DELETE FROM my.asset AS target WHERE id in (1) AND (target.region = 'eu')",
		BuildIncrementalDelete("my.asset", "id in (1)", " target.region = 'eu' "),
	)
}

func TestRejectDeleteInsertPredicate(t *testing.T) {
	t.Parallel()

	asset := &pipeline.Asset{}
	require.NoError(t, RejectDeleteInsertPredicate(asset, "Trino"))

	asset.Materialization.IncrementalPredicate = "  "
	require.NoError(t, RejectDeleteInsertPredicate(asset, "Trino"))

	asset.Materialization.IncrementalPredicate = "dt >= '2026-07-01'"
	require.EqualError(t, RejectDeleteInsertPredicate(asset, "Trino"), "incremental_predicate is not supported for Trino delete+insert materialization")
}
