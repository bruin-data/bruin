package ansisql

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bruin-data/bruin/pkg/pipeline"
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
