package trino

import (
	"strings"
	"testing"

	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBuildCreateReplaceQuery(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		asset    *pipeline.Asset
		query    string
		expected string
	}{
		{
			name: "basic create replace without partitioning",
			asset: &pipeline.Asset{
				Name: "test_table",
				Materialization: pipeline.Materialization{
					Type:     pipeline.MaterializationTypeTable,
					Strategy: pipeline.MaterializationStrategyCreateReplace,
				},
			},
			query: "SELECT * FROM source_table",
			expected: `
DROP TABLE IF EXISTS "test_table";
CREATE TABLE "test_table" WITH (format = 'PARQUET') AS
SELECT * FROM source_table;`,
		},
		{
			name: "create replace with partitioning",
			asset: &pipeline.Asset{
				Name: "partitioned_table",
				Materialization: pipeline.Materialization{
					Type:        pipeline.MaterializationTypeTable,
					Strategy:    pipeline.MaterializationStrategyCreateReplace,
					PartitionBy: "date_column",
				},
			},
			query: "SELECT * FROM source_table",
			expected: `
DROP TABLE IF EXISTS "partitioned_table";
CREATE TABLE "partitioned_table" WITH (format = 'PARQUET', partitioning = ARRAY['date_column']) AS
SELECT * FROM source_table;`,
		},
		{
			name: "query with trailing semicolon gets trimmed",
			asset: &pipeline.Asset{
				Name: "test_table",
				Materialization: pipeline.Materialization{
					Type:     pipeline.MaterializationTypeTable,
					Strategy: pipeline.MaterializationStrategyCreateReplace,
				},
			},
			query: "SELECT * FROM source_table;",
			expected: `
DROP TABLE IF EXISTS "test_table";
CREATE TABLE "test_table" WITH (format = 'PARQUET') AS
SELECT * FROM source_table;`,
		},
		{
			name: "complex query with partitioning",
			asset: &pipeline.Asset{
				Name: "complex_table",
				Materialization: pipeline.Materialization{
					Type:        pipeline.MaterializationTypeTable,
					Strategy:    pipeline.MaterializationStrategyCreateReplace,
					PartitionBy: "year",
				},
			},
			query: `SELECT 
    id,
    name,
    YEAR(created_at) as year
FROM users 
WHERE active = true`,
			expected: `
DROP TABLE IF EXISTS "complex_table";
CREATE TABLE "complex_table" WITH (format = 'PARQUET', partitioning = ARRAY['year']) AS
SELECT 
    id,
    name,
    YEAR(created_at) as year
FROM users 
WHERE active = true;`,
		},
		{
			name: "table name with schema",
			asset: &pipeline.Asset{
				Name: "schema.test_table",
				Materialization: pipeline.Materialization{
					Type:     pipeline.MaterializationTypeTable,
					Strategy: pipeline.MaterializationStrategyCreateReplace,
				},
			},
			query: "SELECT 1 as col",
			expected: `
DROP TABLE IF EXISTS "schema"."test_table";
CREATE TABLE "schema"."test_table" WITH (format = 'PARQUET') AS
SELECT 1 as col;`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			result, err := buildCreateReplaceQuery(tt.asset, tt.query)

			require.NoError(t, err)

			// Normalize whitespace for comparison
			normalizeWhitespace := func(s string) string {
				return strings.TrimSpace(strings.ReplaceAll(s, "\n", " "))
			}

			assert.Equal(t, normalizeWhitespace(tt.expected), normalizeWhitespace(result))
		})
	}
}

func TestBuildCreateReplaceQuery_EdgeCases(t *testing.T) {
	t.Parallel()
	t.Run("empty partition by should not add partitioning clause", func(t *testing.T) {
		t.Parallel()
		asset := &pipeline.Asset{
			Name: "test_table",
			Materialization: pipeline.Materialization{
				Type:        pipeline.MaterializationTypeTable,
				Strategy:    pipeline.MaterializationStrategyCreateReplace,
				PartitionBy: "", // empty partition by
			},
		}

		result, err := buildCreateReplaceQuery(asset, "SELECT * FROM source")

		require.NoError(t, err)
		assert.Contains(t, result, "WITH (format = 'PARQUET')")
		assert.NotContains(t, result, "partitioning")
	})

	t.Run("multiple semicolons at end should be handled", func(t *testing.T) {
		t.Parallel()
		asset := &pipeline.Asset{
			Name: "test_table",
			Materialization: pipeline.Materialization{
				Type:     pipeline.MaterializationTypeTable,
				Strategy: pipeline.MaterializationStrategyCreateReplace,
			},
		}

		result, err := buildCreateReplaceQuery(asset, "SELECT * FROM source;;;")

		require.NoError(t, err)
		// Should only trim one semicolon from the end
		assert.Contains(t, result, "SELECT * FROM source;;")
	})
}

func TestBuildAppendQuery(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		asset    *pipeline.Asset
		query    string
		expected string
	}{
		{
			name: "basic append query",
			asset: &pipeline.Asset{
				Name: "target_table",
				Materialization: pipeline.Materialization{
					Type:     pipeline.MaterializationTypeTable,
					Strategy: pipeline.MaterializationStrategyAppend,
				},
			},
			query:    "SELECT * FROM source_table",
			expected: "INSERT INTO \"target_table\" SELECT * FROM source_table",
		},
		{
			name: "append with SELECT statement",
			asset: &pipeline.Asset{
				Name: "logs",
				Materialization: pipeline.Materialization{
					Type:     pipeline.MaterializationTypeTable,
					Strategy: pipeline.MaterializationStrategyAppend,
				},
			},
			query:    "SELECT id, message, timestamp FROM temp_logs WHERE processed = false",
			expected: "INSERT INTO \"logs\" SELECT id, message, timestamp FROM temp_logs WHERE processed = false",
		},
		{
			name: "append with VALUES clause",
			asset: &pipeline.Asset{
				Name: "test_data",
				Materialization: pipeline.Materialization{
					Type:     pipeline.MaterializationTypeTable,
					Strategy: pipeline.MaterializationStrategyAppend,
				},
			},
			query:    "VALUES (1, 'test'), (2, 'data')",
			expected: "INSERT INTO \"test_data\" VALUES (1, 'test'), (2, 'data')",
		},
		{
			name: "append with schema-qualified table name",
			asset: &pipeline.Asset{
				Name: "schema.target_table",
				Materialization: pipeline.Materialization{
					Type:     pipeline.MaterializationTypeTable,
					Strategy: pipeline.MaterializationStrategyAppend,
				},
			},
			query:    "SELECT * FROM source",
			expected: "INSERT INTO \"schema\".\"target_table\" SELECT * FROM source",
		},
		{
			name: "append with complex query",
			asset: &pipeline.Asset{
				Name: "aggregated_data",
				Materialization: pipeline.Materialization{
					Type:     pipeline.MaterializationTypeTable,
					Strategy: pipeline.MaterializationStrategyAppend,
				},
			},
			query: `SELECT 
    user_id,
    COUNT(*) as event_count,
    SUM(amount) as total_amount
FROM events 
WHERE created_at >= '2023-01-01'
GROUP BY user_id`,
			expected: `INSERT INTO "aggregated_data" SELECT 
    user_id,
    COUNT(*) as event_count,
    SUM(amount) as total_amount
FROM events 
WHERE created_at >= '2023-01-01'
GROUP BY user_id`,
		},
		{
			name: "append preserves query with trailing semicolon",
			asset: &pipeline.Asset{
				Name: "target_table",
				Materialization: pipeline.Materialization{
					Type:     pipeline.MaterializationTypeTable,
					Strategy: pipeline.MaterializationStrategyAppend,
				},
			},
			query:    "SELECT * FROM source;",
			expected: "INSERT INTO \"target_table\" SELECT * FROM source;",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			result, err := buildAppendQuery(tt.asset, tt.query)

			require.NoError(t, err)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestBuildAppendQuery_EdgeCases(t *testing.T) {
	t.Parallel()
	t.Run("empty query should still generate INSERT statement", func(t *testing.T) {
		t.Parallel()
		asset := &pipeline.Asset{
			Name: "test_table",
			Materialization: pipeline.Materialization{
				Type:     pipeline.MaterializationTypeTable,
				Strategy: pipeline.MaterializationStrategyAppend,
			},
		}

		result, err := buildAppendQuery(asset, "")

		require.NoError(t, err)
		assert.Equal(t, "INSERT INTO \"test_table\" ", result)
	})

	t.Run("query with only whitespace", func(t *testing.T) {
		t.Parallel()
		asset := &pipeline.Asset{
			Name: "test_table",
			Materialization: pipeline.Materialization{
				Type:     pipeline.MaterializationTypeTable,
				Strategy: pipeline.MaterializationStrategyAppend,
			},
		}

		result, err := buildAppendQuery(asset, "   \n\t  ")

		require.NoError(t, err)
		assert.Equal(t, "INSERT INTO \"test_table\"    \n\t  ", result)
	})
}

func TestBuildIncrementalQuery(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		asset    *pipeline.Asset
		query    string
		expected string
	}{
		{
			name: "basic incremental query with id key",
			asset: &pipeline.Asset{
				Name: "users",
				Materialization: pipeline.Materialization{
					Type:           pipeline.MaterializationTypeTable,
					Strategy:       pipeline.MaterializationStrategyDeleteInsert,
					IncrementalKey: "id",
				},
			},
			query: "SELECT id, name, email FROM source_users WHERE updated_at > '2023-01-01'",
			expected: `
DELETE FROM "users" 
WHERE id IN (
    SELECT DISTINCT id 
    FROM (SELECT id, name, email FROM source_users WHERE updated_at > '2023-01-01') AS new_data
);

INSERT INTO "users"
SELECT * FROM (SELECT id, name, email FROM source_users WHERE updated_at > '2023-01-01') AS new_data;`,
		},
		{
			name: "incremental with date key",
			asset: &pipeline.Asset{
				Name: "daily_metrics",
				Materialization: pipeline.Materialization{
					Type:           pipeline.MaterializationTypeTable,
					Strategy:       pipeline.MaterializationStrategyDeleteInsert,
					IncrementalKey: "date",
				},
			},
			query: "SELECT date, revenue, users FROM metrics WHERE date = '2023-01-01'",
			expected: `
DELETE FROM "daily_metrics" 
WHERE date IN (
    SELECT DISTINCT date 
    FROM (SELECT date, revenue, users FROM metrics WHERE date = '2023-01-01') AS new_data
);

INSERT INTO "daily_metrics"
SELECT * FROM (SELECT date, revenue, users FROM metrics WHERE date = '2023-01-01') AS new_data;`,
		},
		{
			name: "incremental with schema-qualified table",
			asset: &pipeline.Asset{
				Name: "analytics.user_sessions",
				Materialization: pipeline.Materialization{
					Type:           pipeline.MaterializationTypeTable,
					Strategy:       pipeline.MaterializationStrategyDeleteInsert,
					IncrementalKey: "session_id",
				},
			},
			query: "SELECT session_id, user_id, duration FROM raw_sessions",
			expected: `
DELETE FROM "analytics"."user_sessions" 
WHERE session_id IN (
    SELECT DISTINCT session_id 
    FROM (SELECT session_id, user_id, duration FROM raw_sessions) AS new_data
);

INSERT INTO "analytics"."user_sessions"
SELECT * FROM (SELECT session_id, user_id, duration FROM raw_sessions) AS new_data;`,
		},
		{
			name: "query with trailing semicolon gets trimmed",
			asset: &pipeline.Asset{
				Name: "products",
				Materialization: pipeline.Materialization{
					Type:           pipeline.MaterializationTypeTable,
					Strategy:       pipeline.MaterializationStrategyDeleteInsert,
					IncrementalKey: "product_id",
				},
			},
			query: "SELECT product_id, name, price FROM source_products;",
			expected: `
DELETE FROM "products" 
WHERE product_id IN (
    SELECT DISTINCT product_id 
    FROM (SELECT product_id, name, price FROM source_products) AS new_data
);

INSERT INTO "products"
SELECT * FROM (SELECT product_id, name, price FROM source_products) AS new_data;`,
		},
		{
			name: "complex query with joins and aggregations",
			asset: &pipeline.Asset{
				Name: "user_stats",
				Materialization: pipeline.Materialization{
					Type:           pipeline.MaterializationTypeTable,
					Strategy:       pipeline.MaterializationStrategyDeleteInsert,
					IncrementalKey: "user_id",
				},
			},
			query: `SELECT 
    u.user_id,
    u.name,
    COUNT(o.order_id) as total_orders,
    SUM(o.amount) as total_spent
FROM users u
LEFT JOIN orders o ON u.user_id = o.user_id
WHERE u.updated_at > '2023-01-01'
GROUP BY u.user_id, u.name`,
			expected: `
DELETE FROM "user_stats" 
WHERE user_id IN (
    SELECT DISTINCT user_id 
    FROM (SELECT 
    u.user_id,
    u.name,
    COUNT(o.order_id) as total_orders,
    SUM(o.amount) as total_spent
FROM users u
LEFT JOIN orders o ON u.user_id = o.user_id
WHERE u.updated_at > '2023-01-01'
GROUP BY u.user_id, u.name) AS new_data
);

INSERT INTO "user_stats"
SELECT * FROM (SELECT 
    u.user_id,
    u.name,
    COUNT(o.order_id) as total_orders,
    SUM(o.amount) as total_spent
FROM users u
LEFT JOIN orders o ON u.user_id = o.user_id
WHERE u.updated_at > '2023-01-01'
GROUP BY u.user_id, u.name) AS new_data;`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			result, err := buildIncrementalQuery(tt.asset, tt.query)

			require.NoError(t, err)

			// Normalize whitespace for comparison
			normalizeWhitespace := func(s string) string {
				return strings.TrimSpace(strings.ReplaceAll(s, "\n", " "))
			}

			assert.Equal(t, normalizeWhitespace(tt.expected), normalizeWhitespace(result))
		})
	}
}

func TestBuildIncrementalQuery_ErrorCases(t *testing.T) {
	t.Parallel()
	t.Run("missing incremental key should return error", func(t *testing.T) {
		t.Parallel()
		asset := &pipeline.Asset{
			Name: "test_table",
			Materialization: pipeline.Materialization{
				Type:           pipeline.MaterializationTypeTable,
				Strategy:       pipeline.MaterializationStrategyDeleteInsert,
				IncrementalKey: "", // empty incremental key
			},
		}

		result, err := buildIncrementalQuery(asset, "SELECT * FROM source")

		require.Error(t, err)
		assert.Contains(t, err.Error(), "requires the `incremental_key` field to be set")
		assert.Empty(t, result)
	})

	t.Run("error message includes strategy name", func(t *testing.T) {
		t.Parallel()
		asset := &pipeline.Asset{
			Name: "test_table",
			Materialization: pipeline.Materialization{
				Type:           pipeline.MaterializationTypeTable,
				Strategy:       pipeline.MaterializationStrategyDeleteInsert,
				IncrementalKey: "",
			},
		}

		_, err := buildIncrementalQuery(asset, "SELECT * FROM source")

		require.Error(t, err)
		assert.Contains(t, err.Error(), string(pipeline.MaterializationStrategyDeleteInsert))
	})
}

func TestBuildTimeIntervalQuery(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		asset    *pipeline.Asset
		query    string
		expected string
	}{
		{
			name: "time interval with timestamp granularity",
			asset: &pipeline.Asset{
				Name: "events",
				Materialization: pipeline.Materialization{
					Type:            pipeline.MaterializationTypeTable,
					Strategy:        pipeline.MaterializationStrategyTimeInterval,
					IncrementalKey:  "created_at",
					TimeGranularity: pipeline.MaterializationTimeGranularityTimestamp,
				},
			},
			query: "SELECT id, user_id, event_type, created_at FROM raw_events WHERE created_at BETWEEN '{{start_timestamp}}' AND '{{end_timestamp}}'",
			expected: `
DELETE FROM "events" 
WHERE created_at BETWEEN TIMESTAMP '{{start_timestamp}}' AND TIMESTAMP '{{end_timestamp}}';

INSERT INTO "events"
SELECT id, user_id, event_type, created_at FROM raw_events WHERE created_at BETWEEN '{{start_timestamp}}' AND '{{end_timestamp}}';`,
		},
		{
			name: "time interval with date granularity",
			asset: &pipeline.Asset{
				Name: "daily_sales",
				Materialization: pipeline.Materialization{
					Type:            pipeline.MaterializationTypeTable,
					Strategy:        pipeline.MaterializationStrategyTimeInterval,
					IncrementalKey:  "sale_date",
					TimeGranularity: pipeline.MaterializationTimeGranularityDate,
				},
			},
			query: "SELECT product_id, sale_date, amount FROM sales WHERE sale_date BETWEEN '{{start_date}}' AND '{{end_date}}'",
			expected: `
DELETE FROM "daily_sales" 
WHERE sale_date BETWEEN DATE '{{start_date}}' AND DATE '{{end_date}}';

INSERT INTO "daily_sales"
SELECT product_id, sale_date, amount FROM sales WHERE sale_date BETWEEN '{{start_date}}' AND '{{end_date}}';`,
		},
		{
			name: "schema-qualified table with timestamp",
			asset: &pipeline.Asset{
				Name: "analytics.user_events",
				Materialization: pipeline.Materialization{
					Type:            pipeline.MaterializationTypeTable,
					Strategy:        pipeline.MaterializationStrategyTimeInterval,
					IncrementalKey:  "event_timestamp",
					TimeGranularity: pipeline.MaterializationTimeGranularityTimestamp,
				},
			},
			query: "SELECT user_id, event_type, event_timestamp FROM events",
			expected: `
DELETE FROM "analytics"."user_events" 
WHERE event_timestamp BETWEEN TIMESTAMP '{{start_timestamp}}' AND TIMESTAMP '{{end_timestamp}}';

INSERT INTO "analytics"."user_events"
SELECT user_id, event_type, event_timestamp FROM events;`,
		},
		{
			name: "query with trailing semicolon gets trimmed",
			asset: &pipeline.Asset{
				Name: "logs",
				Materialization: pipeline.Materialization{
					Type:            pipeline.MaterializationTypeTable,
					Strategy:        pipeline.MaterializationStrategyTimeInterval,
					IncrementalKey:  "log_date",
					TimeGranularity: pipeline.MaterializationTimeGranularityDate,
				},
			},
			query: "SELECT level, message, log_date FROM system_logs;",
			expected: `
DELETE FROM "logs" 
WHERE log_date BETWEEN DATE '{{start_date}}' AND DATE '{{end_date}}';

INSERT INTO "logs"
SELECT level, message, log_date FROM system_logs;`,
		},
		{
			name: "complex query with aggregations and timestamp",
			asset: &pipeline.Asset{
				Name: "hourly_metrics",
				Materialization: pipeline.Materialization{
					Type:            pipeline.MaterializationTypeTable,
					Strategy:        pipeline.MaterializationStrategyTimeInterval,
					IncrementalKey:  "hour_timestamp",
					TimeGranularity: pipeline.MaterializationTimeGranularityTimestamp,
				},
			},
			query: `SELECT 
    DATE_TRUNC('hour', created_at) as hour_timestamp,
    COUNT(*) as event_count,
    COUNT(DISTINCT user_id) as unique_users
FROM events 
WHERE created_at BETWEEN '{{start_timestamp}}' AND '{{end_timestamp}}'
GROUP BY DATE_TRUNC('hour', created_at)`,
			expected: `
DELETE FROM "hourly_metrics" 
WHERE hour_timestamp BETWEEN TIMESTAMP '{{start_timestamp}}' AND TIMESTAMP '{{end_timestamp}}';

INSERT INTO "hourly_metrics"
SELECT 
    DATE_TRUNC('hour', created_at) as hour_timestamp,
    COUNT(*) as event_count,
    COUNT(DISTINCT user_id) as unique_users
FROM events 
WHERE created_at BETWEEN '{{start_timestamp}}' AND '{{end_timestamp}}'
GROUP BY DATE_TRUNC('hour', created_at);`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			result, err := buildTimeIntervalQuery(tt.asset, tt.query)

			require.NoError(t, err)

			// Normalize whitespace for comparison
			normalizeWhitespace := func(s string) string {
				return strings.TrimSpace(strings.ReplaceAll(s, "\n", " "))
			}

			assert.Equal(t, normalizeWhitespace(tt.expected), normalizeWhitespace(result))
		})
	}
}

func TestBuildTimeIntervalQuery_ErrorCases(t *testing.T) {
	t.Parallel()
	t.Run("missing incremental key should return error", func(t *testing.T) {
		t.Parallel()
		asset := &pipeline.Asset{
			Name: "test_table",
			Materialization: pipeline.Materialization{
				Type:            pipeline.MaterializationTypeTable,
				Strategy:        pipeline.MaterializationStrategyTimeInterval,
				IncrementalKey:  "", // empty incremental key
				TimeGranularity: pipeline.MaterializationTimeGranularityDate,
			},
		}

		result, err := buildTimeIntervalQuery(asset, "SELECT * FROM source")

		require.Error(t, err)
		assert.Contains(t, err.Error(), "incremental_key is required for time_interval strategy")
		assert.Empty(t, result)
	})

	t.Run("missing time granularity should return error", func(t *testing.T) {
		t.Parallel()
		asset := &pipeline.Asset{
			Name: "test_table",
			Materialization: pipeline.Materialization{
				Type:            pipeline.MaterializationTypeTable,
				Strategy:        pipeline.MaterializationStrategyTimeInterval,
				IncrementalKey:  "created_at",
				TimeGranularity: "", // empty time granularity
			},
		}

		result, err := buildTimeIntervalQuery(asset, "SELECT * FROM source")

		require.Error(t, err)
		assert.Contains(t, err.Error(), "time_granularity is required for time_interval strategy")
		assert.Empty(t, result)
	})

	t.Run("invalid time granularity should return error", func(t *testing.T) {
		t.Parallel()
		asset := &pipeline.Asset{
			Name: "test_table",
			Materialization: pipeline.Materialization{
				Type:            pipeline.MaterializationTypeTable,
				Strategy:        pipeline.MaterializationStrategyTimeInterval,
				IncrementalKey:  "created_at",
				TimeGranularity: "hour", // invalid granularity
			},
		}

		result, err := buildTimeIntervalQuery(asset, "SELECT * FROM source")

		require.Error(t, err)
		assert.Contains(t, err.Error(), "time_granularity must be either 'date', or 'timestamp'")
		assert.Empty(t, result)
	})

	t.Run("both incremental key and time granularity missing", func(t *testing.T) {
		t.Parallel()
		asset := &pipeline.Asset{
			Name: "test_table",
			Materialization: pipeline.Materialization{
				Type:            pipeline.MaterializationTypeTable,
				Strategy:        pipeline.MaterializationStrategyTimeInterval,
				IncrementalKey:  "",
				TimeGranularity: "",
			},
		}

		result, err := buildTimeIntervalQuery(asset, "SELECT * FROM source")

		require.Error(t, err)
		// Should fail on the first validation (incremental_key)
		assert.Contains(t, err.Error(), "incremental_key is required")
		assert.Empty(t, result)
	})
}

func TestViewMaterializer(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		asset    *pipeline.Asset
		query    string
		expected string
	}{
		{
			name: "basic view creation",
			asset: &pipeline.Asset{
				Name: "user_stats",
				Materialization: pipeline.Materialization{
					Type:     pipeline.MaterializationTypeView,
					Strategy: pipeline.MaterializationStrategyNone,
				},
			},
			query:    "SELECT user_id, COUNT(*) as order_count FROM orders GROUP BY user_id",
			expected: "CREATE OR REPLACE VIEW \"user_stats\" AS\nSELECT user_id, COUNT(*) as order_count FROM orders GROUP BY user_id",
		},
		{
			name: "view with schema qualifier",
			asset: &pipeline.Asset{
				Name: "analytics.monthly_sales",
				Materialization: pipeline.Materialization{
					Type:     pipeline.MaterializationTypeView,
					Strategy: pipeline.MaterializationStrategyNone,
				},
			},
			query:    "SELECT DATE_TRUNC('month', sale_date) as month, SUM(amount) as total FROM sales GROUP BY DATE_TRUNC('month', sale_date)",
			expected: "CREATE OR REPLACE VIEW \"analytics\".\"monthly_sales\" AS\nSELECT DATE_TRUNC('month', sale_date) as month, SUM(amount) as total FROM sales GROUP BY DATE_TRUNC('month', sale_date)",
		},
		{
			name: "complex view with joins",
			asset: &pipeline.Asset{
				Name: "customer_summary",
				Materialization: pipeline.Materialization{
					Type:     pipeline.MaterializationTypeView,
					Strategy: pipeline.MaterializationStrategyNone,
				},
			},
			query: `SELECT 
    c.customer_id,
    c.name,
    COUNT(o.order_id) as total_orders,
    SUM(o.amount) as total_spent
FROM customers c
LEFT JOIN orders o ON c.customer_id = o.customer_id
GROUP BY c.customer_id, c.name`,
			expected: `CREATE OR REPLACE VIEW "customer_summary" AS
SELECT 
    c.customer_id,
    c.name,
    COUNT(o.order_id) as total_orders,
    SUM(o.amount) as total_spent
FROM customers c
LEFT JOIN orders o ON c.customer_id = o.customer_id
GROUP BY c.customer_id, c.name`,
		},
		{
			name: "query with trailing semicolon gets trimmed",
			asset: &pipeline.Asset{
				Name: "active_users",
				Materialization: pipeline.Materialization{
					Type:     pipeline.MaterializationTypeView,
					Strategy: pipeline.MaterializationStrategyNone,
				},
			},
			query:    "SELECT user_id, last_login FROM users WHERE last_login > CURRENT_DATE - INTERVAL '30' DAY;",
			expected: "CREATE OR REPLACE VIEW \"active_users\" AS\nSELECT user_id, last_login FROM users WHERE last_login > CURRENT_DATE - INTERVAL '30' DAY",
		},
		{
			name: "view with window functions",
			asset: &pipeline.Asset{
				Name: "ranked_products",
				Materialization: pipeline.Materialization{
					Type:     pipeline.MaterializationTypeView,
					Strategy: pipeline.MaterializationStrategyNone,
				},
			},
			query:    "SELECT product_id, name, price, ROW_NUMBER() OVER (ORDER BY price DESC) as price_rank FROM products",
			expected: "CREATE OR REPLACE VIEW \"ranked_products\" AS\nSELECT product_id, name, price, ROW_NUMBER() OVER (ORDER BY price DESC) as price_rank FROM products",
		},
		{
			name: "view with CTEs",
			asset: &pipeline.Asset{
				Name: "sales_with_growth",
				Materialization: pipeline.Materialization{
					Type:     pipeline.MaterializationTypeView,
					Strategy: pipeline.MaterializationStrategyNone,
				},
			},
			query: `WITH monthly_sales AS (
    SELECT 
        DATE_TRUNC('month', sale_date) as month,
        SUM(amount) as monthly_total
    FROM sales
    GROUP BY DATE_TRUNC('month', sale_date)
)
SELECT 
    month,
    monthly_total,
    LAG(monthly_total) OVER (ORDER BY month) as prev_month_total,
    (monthly_total - LAG(monthly_total) OVER (ORDER BY month)) / LAG(monthly_total) OVER (ORDER BY month) * 100 as growth_percent
FROM monthly_sales`,
			expected: `CREATE OR REPLACE VIEW "sales_with_growth" AS
WITH monthly_sales AS (
    SELECT 
        DATE_TRUNC('month', sale_date) as month,
        SUM(amount) as monthly_total
    FROM sales
    GROUP BY DATE_TRUNC('month', sale_date)
)
SELECT 
    month,
    monthly_total,
    LAG(monthly_total) OVER (ORDER BY month) as prev_month_total,
    (monthly_total - LAG(monthly_total) OVER (ORDER BY month)) / LAG(monthly_total) OVER (ORDER BY month) * 100 as growth_percent
FROM monthly_sales`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			result, err := viewMaterializer(tt.asset, tt.query)

			require.NoError(t, err)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestViewMaterializer_EdgeCases(t *testing.T) {
	t.Parallel()
	t.Run("empty query should still create view", func(t *testing.T) {
		t.Parallel()
		asset := &pipeline.Asset{
			Name: "empty_view",
			Materialization: pipeline.Materialization{
				Type:     pipeline.MaterializationTypeView,
				Strategy: pipeline.MaterializationStrategyNone,
			},
		}

		result, err := viewMaterializer(asset, "")

		require.NoError(t, err)
		assert.Equal(t, "CREATE OR REPLACE VIEW \"empty_view\" AS\n", result)
	})

	t.Run("multiple trailing semicolons", func(t *testing.T) {
		t.Parallel()
		asset := &pipeline.Asset{
			Name: "test_view",
			Materialization: pipeline.Materialization{
				Type:     pipeline.MaterializationTypeView,
				Strategy: pipeline.MaterializationStrategyNone,
			},
		}

		result, err := viewMaterializer(asset, "SELECT 1 as col;;;")

		require.NoError(t, err)
		// Should only trim one semicolon from the end
		assert.Equal(t, "CREATE OR REPLACE VIEW \"test_view\" AS\nSELECT 1 as col;;", result)
	})
}

func TestBuildMergeQuery(t *testing.T) {
	t.Parallel()
	t.Run("merge strategy should return not supported error", func(t *testing.T) {
		t.Parallel()
		asset := &pipeline.Asset{
			Name: "users",
			Materialization: pipeline.Materialization{
				Type:     pipeline.MaterializationTypeTable,
				Strategy: pipeline.MaterializationStrategyMerge,
			},
		}

		result, err := buildMergeQuery(asset, "SELECT id, name FROM source")

		require.Error(t, err)
		assert.Contains(t, err.Error(), "is not supported")
		assert.Contains(t, err.Error(), string(pipeline.MaterializationStrategyMerge))
		assert.Empty(t, result)
	})
}

func TestBuildDDLQuery(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		asset    *pipeline.Asset
		query    string
		expected string
	}{
		{
			name: "basic DDL with columns",
			asset: &pipeline.Asset{
				Name: "users",
				Materialization: pipeline.Materialization{
					Type:     pipeline.MaterializationTypeTable,
					Strategy: pipeline.MaterializationStrategyDDL,
				},
				Columns: []pipeline.Column{
					{Name: "id", Type: "BIGINT"},
					{Name: "name", Type: "VARCHAR"},
					{Name: "email", Type: "VARCHAR"},
				},
			},
			query: "", // DDL doesn't use the query parameter
			expected: `CREATE TABLE IF NOT EXISTS "users" (
    id BIGINT,
    name VARCHAR,
    email VARCHAR
) WITH (format = 'PARQUET')`,
		},
		{
			name: "DDL with column descriptions",
			asset: &pipeline.Asset{
				Name: "products",
				Materialization: pipeline.Materialization{
					Type:     pipeline.MaterializationTypeTable,
					Strategy: pipeline.MaterializationStrategyDDL,
				},
				Columns: []pipeline.Column{
					{Name: "product_id", Type: "BIGINT", Description: "Unique product identifier"},
					{Name: "name", Type: "VARCHAR", Description: "Product name"},
					{Name: "price", Type: "DECIMAL(10,2)", Description: "Product price in USD"},
				},
			},
			query: "",
			expected: `CREATE TABLE IF NOT EXISTS "products" (
    product_id BIGINT COMMENT 'Unique product identifier',
    name VARCHAR COMMENT 'Product name',
    price DECIMAL(10,2) COMMENT 'Product price in USD'
) WITH (format = 'PARQUET')`,
		},
		{
			name: "DDL with partitioning",
			asset: &pipeline.Asset{
				Name: "events",
				Materialization: pipeline.Materialization{
					Type:        pipeline.MaterializationTypeTable,
					Strategy:    pipeline.MaterializationStrategyDDL,
					PartitionBy: "date",
				},
				Columns: []pipeline.Column{
					{Name: "event_id", Type: "BIGINT"},
					{Name: "user_id", Type: "BIGINT"},
					{Name: "event_type", Type: "VARCHAR"},
					{Name: "date", Type: "DATE"},
				},
			},
			query: "",
			expected: `CREATE TABLE IF NOT EXISTS "events" (
    event_id BIGINT,
    user_id BIGINT,
    event_type VARCHAR,
    date DATE
) WITH (format = 'PARQUET', partitioning = ARRAY['date'])`,
		},
		{
			name: "schema-qualified table",
			asset: &pipeline.Asset{
				Name: "analytics.user_metrics",
				Materialization: pipeline.Materialization{
					Type:     pipeline.MaterializationTypeTable,
					Strategy: pipeline.MaterializationStrategyDDL,
				},
				Columns: []pipeline.Column{
					{Name: "user_id", Type: "BIGINT"},
					{Name: "total_orders", Type: "INTEGER"},
					{Name: "total_spent", Type: "DECIMAL(15,2)"},
				},
			},
			query: "",
			expected: `CREATE TABLE IF NOT EXISTS "analytics"."user_metrics" (
    user_id BIGINT,
    total_orders INTEGER,
    total_spent DECIMAL(15,2)
) WITH (format = 'PARQUET')`,
		},
		{
			name: "DDL with special characters in descriptions",
			asset: &pipeline.Asset{
				Name: "test_table",
				Materialization: pipeline.Materialization{
					Type:     pipeline.MaterializationTypeTable,
					Strategy: pipeline.MaterializationStrategyDDL,
				},
				Columns: []pipeline.Column{
					{Name: "id", Type: "BIGINT", Description: "ID with 'quotes' and \"double quotes\""},
					{Name: "name", Type: "VARCHAR", Description: "Name with special chars: @#$%"},
				},
			},
			query: "",
			expected: `CREATE TABLE IF NOT EXISTS "test_table" (
    id BIGINT COMMENT 'ID with ''quotes'' and "double quotes"',
    name VARCHAR COMMENT 'Name with special chars: @#$%'
) WITH (format = 'PARQUET')`,
		},
		{
			name: "DDL with complex data types",
			asset: &pipeline.Asset{
				Name: "complex_table",
				Materialization: pipeline.Materialization{
					Type:        pipeline.MaterializationTypeTable,
					Strategy:    pipeline.MaterializationStrategyDDL,
					PartitionBy: "created_date",
				},
				Columns: []pipeline.Column{
					{Name: "id", Type: "BIGINT"},
					{Name: "data", Type: "JSON", Description: "JSON data column"},
					{Name: "tags", Type: "ARRAY(VARCHAR)"},
					{Name: "metadata", Type: "MAP(VARCHAR, VARCHAR)"},
					{Name: "created_date", Type: "DATE"},
					{Name: "updated_at", Type: "TIMESTAMP"},
				},
			},
			query: "",
			expected: `CREATE TABLE IF NOT EXISTS "complex_table" (
    id BIGINT,
    data JSON COMMENT 'JSON data column',
    tags ARRAY(VARCHAR),
    metadata MAP(VARCHAR, VARCHAR),
    created_date DATE,
    updated_at TIMESTAMP
) WITH (format = 'PARQUET', partitioning = ARRAY['created_date'])`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			result, err := buildDDLQuery(tt.asset, tt.query)

			require.NoError(t, err)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestBuildDDLQuery_ErrorCases(t *testing.T) {
	t.Parallel()
	t.Run("missing columns should return error", func(t *testing.T) {
		t.Parallel()
		asset := &pipeline.Asset{
			Name: "test_table",
			Materialization: pipeline.Materialization{
				Type:     pipeline.MaterializationTypeTable,
				Strategy: pipeline.MaterializationStrategyDDL,
			},
			Columns: []pipeline.Column{}, // empty columns
		}

		result, err := buildDDLQuery(asset, "")

		require.Error(t, err)
		assert.Contains(t, err.Error(), "requires the `columns` field to be set")
		assert.Empty(t, result)
	})

	t.Run("error message includes strategy name", func(t *testing.T) {
		t.Parallel()
		asset := &pipeline.Asset{
			Name: "test_table",
			Materialization: pipeline.Materialization{
				Type:     pipeline.MaterializationTypeTable,
				Strategy: pipeline.MaterializationStrategyDDL,
			},
			Columns: []pipeline.Column{}, // empty columns to trigger error
		}

		_, err := buildDDLQuery(asset, "")

		require.Error(t, err)
		assert.Contains(t, err.Error(), string(pipeline.MaterializationStrategyDDL))
	})
}
