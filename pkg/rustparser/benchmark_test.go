package rustparser

import (
	"testing"
)

// Large BigQuery SQL query for benchmarking — realistic data warehouse query with
// multiple joins, CTEs, subqueries, and complex expressions.
const benchmarkQuery = `
WITH daily_revenue AS (
    SELECT
        DATE(o.created_at) AS order_date,
        p.category_id,
        c.category_name,
        SUM(oi.quantity * oi.unit_price) AS revenue,
        COUNT(DISTINCT o.order_id) AS num_orders,
        COUNT(DISTINCT o.customer_id) AS num_customers,
        AVG(oi.quantity * oi.unit_price) AS avg_order_value
    FROM ` + "`project.dataset.orders`" + ` o
    JOIN ` + "`project.dataset.order_items`" + ` oi ON o.order_id = oi.order_id
    JOIN ` + "`project.dataset.products`" + ` p ON oi.product_id = p.product_id
    JOIN ` + "`project.dataset.categories`" + ` c ON p.category_id = c.category_id
    WHERE o.created_at >= '2024-01-01'
      AND o.status NOT IN ('cancelled', 'refunded')
    GROUP BY 1, 2, 3
),
customer_segments AS (
    SELECT
        cs.customer_id,
        cs.segment_name,
        cs.lifetime_value,
        cs.first_purchase_date,
        cs.last_purchase_date
    FROM ` + "`project.dataset.customer_segments`" + ` cs
    WHERE cs.is_active = TRUE
),
product_performance AS (
    SELECT
        p.product_id,
        p.product_name,
        p.category_id,
        COUNT(*) AS times_ordered,
        SUM(oi.quantity) AS total_quantity,
        SUM(oi.quantity * oi.unit_price) AS total_revenue,
        AVG(r.rating) AS avg_rating,
        COUNT(r.review_id) AS num_reviews
    FROM ` + "`project.dataset.products`" + ` p
    JOIN ` + "`project.dataset.order_items`" + ` oi ON p.product_id = oi.product_id
    LEFT JOIN ` + "`project.dataset.reviews`" + ` r ON p.product_id = r.product_id
    GROUP BY 1, 2, 3
)
SELECT
    dr.order_date,
    dr.category_name,
    dr.revenue,
    dr.num_orders,
    dr.num_customers,
    dr.avg_order_value,
    pp.product_name AS top_product,
    pp.total_revenue AS product_revenue,
    pp.avg_rating,
    pp.num_reviews,
    COALESCE(seg_stats.segment_orders, 0) AS premium_segment_orders,
    LAG(dr.revenue) OVER (PARTITION BY dr.category_id ORDER BY dr.order_date) AS prev_day_revenue,
    SAFE_DIVIDE(dr.revenue - LAG(dr.revenue) OVER (PARTITION BY dr.category_id ORDER BY dr.order_date),
                LAG(dr.revenue) OVER (PARTITION BY dr.category_id ORDER BY dr.order_date)) AS revenue_growth
FROM daily_revenue dr
LEFT JOIN product_performance pp ON dr.category_id = pp.category_id
LEFT JOIN (
    SELECT
        DATE(o.created_at) AS order_date,
        COUNT(DISTINCT o.order_id) AS segment_orders
    FROM ` + "`project.dataset.orders`" + ` o
    JOIN customer_segments cs ON o.customer_id = cs.customer_id
    WHERE cs.segment_name = 'Premium'
    GROUP BY 1
) seg_stats ON dr.order_date = seg_stats.order_date
ORDER BY dr.order_date DESC, dr.revenue DESC
`

func BenchmarkRustParser_UsedTables(b *testing.B) {
	parser := NewRustSQLParser()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := parser.UsedTables(benchmarkQuery, "bigquery")
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkRustParser_ColumnLineage(b *testing.B) {
	parser := NewRustSQLParserWithConfig(100000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := parser.ColumnLineage(benchmarkQuery, "bigquery", Schema{})
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkRustParser_AddLimit(b *testing.B) {
	parser := NewRustSQLParser()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := parser.AddLimit(benchmarkQuery, 100, "bigquery")
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkRustParser_IsSingleSelect(b *testing.B) {
	parser := NewRustSQLParser()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := parser.IsSingleSelectQuery(benchmarkQuery, "bigquery")
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkRustParser_RenameTables(b *testing.B) {
	parser := NewRustSQLParser()
	mapping := map[string]string{
		"orders":     "orders_v2",
		"products":   "products_v2",
		"categories": "categories_v2",
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := parser.RenameTables(benchmarkQuery, "bigquery", mapping)
		if err != nil {
			b.Fatal(err)
		}
	}
}
